package cluster

// Scale-out admission: a join-only node must start with an EMPTY Raft
// configuration (no phantom bootstrap), be admitted by the leader via
// the OpJoinCluster handler as a non-voter, replicate the existing
// cluster state, and be promoted to voter when it asks again caught up.
// The handler itself must refuse on non-leaders, naming the leader, so
// the joiner can find it.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func TestJoinOnlyNodeAdmittedByLeaderHandler(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()

	// Bootstrap a single-node cluster and give it some state.
	leaderAddr := freeTCPAddr(t)
	leader, err := metastore.New(metastore.Config{
		NodeID:        "node-a",
		DataDir:       filepath.Join(baseDir, "a"),
		BindAddr:      leaderAddr,
		AdvertiseAddr: leaderAddr,
	})
	if err != nil {
		t.Fatalf("metastore.New(leader) error = %v", err)
	}
	t.Cleanup(func() { _ = leader.Close() })
	waitStoreLeader(t, leader)
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "pre-existing", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	// A join-only node: no bootstrap, empty configuration.
	joinerAddr := freeTCPAddr(t)
	joiner, err := metastore.New(metastore.Config{
		NodeID:        "node-b",
		DataDir:       filepath.Join(baseDir, "b"),
		BindAddr:      joinerAddr,
		AdvertiseAddr: joinerAddr,
		Peers:         []metastore.Peer{{ID: "node-a", Addr: leaderAddr}},
		JoinOnly:      true,
	})
	if err != nil {
		t.Fatalf("metastore.New(joiner) error = %v", err)
	}
	t.Cleanup(func() { _ = joiner.Close() })
	if has, err := joiner.HasRaftConfiguration(); err != nil || has {
		t.Fatalf("join-only node HasRaftConfiguration() = %v, %v; want false, nil (no phantom bootstrap)", has, err)
	}
	if joiner.LeaderID() != "" {
		t.Fatalf("join-only node sees leader %q before admission", joiner.LeaderID())
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	payload, err := nodewire.EncodeJoinClusterRequest(nodewire.JoinClusterRequest{ID: "node-b", ClusterAddr: joinerAddr})
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}

	// A node with no Raft configuration at all (itself unadmitted) must
	// say so (412): it is no evidence that a cluster exists, unlike a
	// configured non-leader's 421. Either way the joiner tries the next
	// peer.
	followerServer := NewRPCServer(nil, joiner, log)
	if res := followerServer.handleJoinCluster(payload); res.Status != http.StatusPreconditionFailed {
		t.Fatalf("unconfigured-node join status = %d, want %d", res.Status, http.StatusPreconditionFailed)
	}

	// The leader admits the joiner as a non-voter; the joiner must
	// converge on the leader and replicate pre-existing state.
	leaderServer := NewRPCServer(nil, leader, log)
	res := leaderServer.handleJoinCluster(payload)
	if res.Status != http.StatusOK {
		t.Fatalf("leader join status = %d, want 200", res.Status)
	}
	if status, _ := joinOutcome(t, res); status != metastore.JoinStaged {
		t.Fatalf("join outcome = %q, want %q", status, metastore.JoinStaged)
	}
	waitFor(t, 10*time.Second, "joiner to see the leader", func() bool {
		return joiner.LeaderID() == "node-a"
	})
	waitFor(t, 10*time.Second, "joiner to replicate the topic", func() bool {
		_, err := joiner.GetTopic(ctx, "pre-existing")
		return err == nil
	})

	// The joiner registers and, caught up, asks again: the leader
	// promotes it once it has led long enough to judge heartbeats (12s
	// from its election), deferring with a reason until then. Re-joining
	// (lost reply, joiner restart) stays safe throughout.
	for _, id := range []string{"node-a", "node-b"} {
		addr := map[string]string{"node-a": leaderAddr, "node-b": joinerAddr}[id]
		if err := leader.RegisterMember(ctx, metastore.Member{ID: id, Addr: addr, ClusterAddr: addr, Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
			t.Fatalf("RegisterMember(%s) error = %v", id, err)
		}
	}
	waitFor(t, 10*time.Second, "the joiner's replica to catch up", joiner.AppliedCaughtUp)
	waitFor(t, 30*time.Second, "the leader to promote the joiner", func() bool {
		res := leaderServer.handleJoinCluster(payload)
		if res.Status != http.StatusOK {
			t.Fatalf("promotion request status = %d %s, want 200", res.Status, res.Body)
		}
		status, reason := joinOutcome(t, res)
		if status == metastore.JoinDeferred && reason == "" {
			t.Fatalf("deferred without a reason: %s", res.Body)
		}
		return status == metastore.JoinPromoted
	})
	waitFor(t, 10*time.Second, "the joiner to see itself as a voter", func() bool {
		voter, err := joiner.LocalVoter()
		return err == nil && voter
	})
	if res := leaderServer.handleJoinCluster(payload); res.Status != http.StatusOK {
		t.Fatalf("repeat join status = %d, want 200", res.Status)
	} else if status, _ := joinOutcome(t, res); status != metastore.JoinVoter {
		t.Fatalf("repeat join from a voter = %q, want %q", status, metastore.JoinVoter)
	}

	// Writes now require the joiner in quorum (2 voters): prove the
	// promoted node participates by committing new state through the
	// leader and reading it back on the joiner.
	if voters, err := leader.Voters(); err != nil || len(voters) != 2 {
		t.Fatalf("voters after promotion = %v, %v; want node-a and node-b", voters, err)
	}
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "post-join", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic(post-join) error = %v", err)
	}
	waitFor(t, 10*time.Second, "joiner to replicate post-join topic", func() bool {
		_, err := joiner.GetTopic(ctx, "post-join")
		return err == nil
	})
}

func waitStoreLeader(t *testing.T, s *metastore.Store) {
	t.Helper()
	waitFor(t, 10*time.Second, "store to become leader", s.IsLeader)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// A decommissioned ID must not rejoin with its old state: the pod the
// StatefulSet keeps running after RemoveServer, or a restart with the
// old volume, would otherwise undo the decommission through the very
// loop that exists to recover leaderless nodes. A node declaring an
// EMPTY data directory is a deliberate re-add and is readmitted.
func TestJoinRefusesRemovedIDUnlessFresh(t *testing.T) {
	ctx := context.Background()
	leaderAddr := freeTCPAddr(t)
	leader, err := metastore.New(metastore.Config{
		NodeID:        "node-a",
		DataDir:       filepath.Join(t.TempDir(), "a"),
		BindAddr:      leaderAddr,
		AdvertiseAddr: leaderAddr,
	})
	if err != nil {
		t.Fatalf("metastore.New(leader) error = %v", err)
	}
	t.Cleanup(func() { _ = leader.Close() })
	waitStoreLeader(t, leader)

	if err := leader.RegisterMember(ctx, metastore.Member{ID: "node-z", Addr: "10.0.0.9:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	if err := leader.RemoveMember(ctx, "node-z", 1); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := NewRPCServer(nil, leader, log)

	// The departed pod's heartbeat is refused, not resurrected.
	hb, err := nodewire.EncodeMemberRequest(nodewire.MemberRequest{ID: "node-z", Addr: "10.0.0.9:7942", Status: "alive", LastHeartbeat: 2})
	if err != nil {
		t.Fatalf("encode member request: %v", err)
	}
	if res := server.handleRegisterMember(hb); res.Status != http.StatusGone {
		t.Fatalf("heartbeat from removed member status = %d, want %d", res.Status, http.StatusGone)
	}
	if _, err := leader.GetMember("node-z"); err == nil {
		t.Fatal("removed member resurrected by heartbeat")
	}

	// The old incarnation asks to rejoin: refused.
	stale, err := nodewire.EncodeJoinClusterRequest(nodewire.JoinClusterRequest{ID: "node-z", ClusterAddr: "10.0.0.9:7943"})
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}
	if res := server.handleJoinCluster(stale); res.Status != http.StatusConflict {
		t.Fatalf("stale rejoin status = %d, want %d", res.Status, http.StatusConflict)
	}
	if removed, _ := leader.MemberRemoved("node-z"); !removed {
		t.Fatal("tombstone cleared by a refused join")
	}

	// A fresh node under the same ID: readmitted, its tombstone cleared,
	// and staged as a non-voter. Its address is unreachable here, which
	// costs a non-voter nothing.
	fresh, err := nodewire.EncodeJoinClusterRequest(nodewire.JoinClusterRequest{ID: "node-z", ClusterAddr: "10.0.0.9:7943", Fresh: true})
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}
	res := server.handleJoinCluster(fresh)
	if res.Status != http.StatusOK {
		t.Fatalf("fresh rejoin status = %d %s, want 200", res.Status, res.Body)
	}
	if status, _ := joinOutcome(t, res); status != metastore.JoinStaged {
		t.Fatalf("fresh rejoin outcome = %q, want %q", status, metastore.JoinStaged)
	}
	if removed, _ := leader.MemberRemoved("node-z"); removed {
		t.Fatal("tombstone not cleared by a fresh rejoin")
	}
}

// joinOutcome decodes the status and reason of a 200 join answer.
func joinOutcome(t *testing.T, res nodewire.Response) (status, reason string) {
	t.Helper()
	var body struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(res.Body, &body); err != nil {
		t.Fatalf("join answer body %q: %v", res.Body, err)
	}
	return body.Status, body.Reason
}

// leaderOf returns the store among stores that currently leads, or nil.
func leaderOf(stores map[string]*metastore.Store) *metastore.Store {
	for _, s := range stores {
		if s.IsLeader() {
			return s
		}
	}
	return nil
}

// commitTopicWithin creates a topic through whichever store leads,
// retrying for up to d, and returns the last error if none committed.
func commitTopicWithin(stores map[string]*metastore.Store, name string, d time.Duration) error {
	deadline := time.Now().Add(d)
	last := context.DeadlineExceeded
	for time.Now().Before(deadline) {
		if l := leaderOf(stores); l != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := l.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 1})
			cancel()
			if err == nil {
				return nil
			}
			last = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last
}

// Three voters, one of them down, admit a joiner whose Raft address
// nobody answers. Admitting it as a voter made the new configuration
// need three of four voters while only two could answer: the leader lost
// its lease, nobody could win an election, and no write or configuration
// change could commit again. A joiner is now staged as a non-voter, so
// the two remaining voters keep their quorum.
func TestUnreachableJoinerWithOneVoterDownKeepsQuorum(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	stores := newTestStoreCluster(t, "narad-0", "narad-1", "narad-2")
	leaderID, leader := waitForClusterLeader(t, stores)
	if err := commitTopicWithin(stores, "before", 10*time.Second); err != nil {
		t.Fatalf("write with three voters: %v", err)
	}
	for id, s := range stores {
		if id != leaderID {
			_ = s.Close()
			delete(stores, id)
			break
		}
	}
	if err := commitTopicWithin(stores, "one-down", 10*time.Second); err != nil {
		t.Fatalf("write with one voter down: %v", err)
	}

	payload, err := nodewire.EncodeJoinClusterRequest(nodewire.JoinClusterRequest{ID: "narad-3", ClusterAddr: freeTCPAddr(t), Fresh: true})
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}
	res := NewRPCServer(nil, leader, log).handleJoinCluster(payload)

	if err := commitTopicWithin(stores, "after-join", 10*time.Second); err != nil {
		t.Fatalf("no write committed within 10s of admitting an unreachable joiner with one voter down (join answered %d %s): %v",
			res.Status, strings.TrimSpace(string(res.Body)), err)
	}
	if res.Status != http.StatusOK {
		t.Fatalf("join status = %d %s, want 200", res.Status, res.Body)
	}
	if status, _ := joinOutcome(t, res); status != metastore.JoinStaged {
		t.Fatalf("join outcome = %q, want %q", status, metastore.JoinStaged)
	}
	current := leaderOf(stores)
	if current == nil {
		t.Fatal("no leader after the join")
	}
	voters, err := current.Voters()
	if err != nil || len(voters) != 3 || slices.Contains(voters, "narad-3") {
		t.Fatalf("voters = %v, %v; want the three original voters and no narad-3", voters, err)
	}
	if nonvoters, err := current.Nonvoters(); err != nil || !slices.Equal(nonvoters, []string{"narad-3"}) {
		t.Fatalf("non-voters = %v, %v; want [narad-3]", nonvoters, err)
	}

	// Asking again (what a caught-up joiner does) does not promote a
	// joiner nobody reaches: the answer is deferred, quorum unchanged.
	res = NewRPCServer(nil, current, log).handleJoinCluster(payload)
	if status, reason := joinOutcome(t, res); res.Status != http.StatusOK || status != metastore.JoinDeferred || reason == "" {
		t.Fatalf("repeat join = %d %s, want 200 deferred with a reason", res.Status, res.Body)
	}
	if err := commitTopicWithin(stores, "after-repeat", 10*time.Second); err != nil {
		t.Fatalf("write after a repeat join: %v", err)
	}
}

// A join is answered by staging the joiner: it replicates the cluster's
// state but is not a voter until it asks again, caught up.
func TestJoinerIsStagedAsANonvoter(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	leaderAddr := freeTCPAddr(t)
	leader, err := metastore.New(metastore.Config{NodeID: "node-a", DataDir: filepath.Join(t.TempDir(), "a"), BindAddr: leaderAddr, AdvertiseAddr: leaderAddr})
	if err != nil {
		t.Fatalf("metastore.New(leader) error = %v", err)
	}
	t.Cleanup(func() { _ = leader.Close() })
	waitStoreLeader(t, leader)
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "pre-existing", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	joinerAddr := freeTCPAddr(t)
	joiner, err := metastore.New(metastore.Config{NodeID: "node-b", DataDir: filepath.Join(t.TempDir(), "b"), BindAddr: joinerAddr, AdvertiseAddr: joinerAddr, JoinOnly: true})
	if err != nil {
		t.Fatalf("metastore.New(joiner) error = %v", err)
	}
	t.Cleanup(func() { _ = joiner.Close() })

	payload, err := nodewire.EncodeJoinClusterRequest(nodewire.JoinClusterRequest{ID: "node-b", ClusterAddr: joinerAddr, Fresh: true})
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}
	res := NewRPCServer(nil, leader, log).handleJoinCluster(payload)
	if res.Status != http.StatusOK {
		t.Fatalf("join status = %d %s, want 200", res.Status, res.Body)
	}
	waitFor(t, 10*time.Second, "the joiner to replicate the topic", func() bool {
		_, err := joiner.GetTopic(ctx, "pre-existing")
		return err == nil
	})
	if voters, err := leader.Voters(); err != nil || !slices.Equal(voters, []string{"node-a"}) {
		t.Fatalf("voters after the join = %v, %v; want only node-a (the joiner staged)", voters, err)
	}
	if status, _ := joinOutcome(t, res); status != metastore.JoinStaged {
		t.Fatalf("join outcome = %q, want %q", status, metastore.JoinStaged)
	}
	if nonvoters, err := leader.Nonvoters(); err != nil || !slices.Equal(nonvoters, []string{"node-b"}) {
		t.Fatalf("non-voters = %v, %v; want [node-b]", nonvoters, err)
	}
	if voter, err := joiner.LocalVoter(); err != nil || voter {
		t.Fatalf("joiner LocalVoter() = %v, %v; want false (staged)", voter, err)
	}
}

// A follower's answer to a join names the leader and the leader's
// node-RPC address from its replica, so a joiner whose pinned peers do
// not include the leader can go straight to it.
func TestNotLeaderJoinAnswerNamesTheLeader(t *testing.T) {
	ctx := context.Background()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	stores := newTestStoreCluster(t, "narad-0", "narad-1", "narad-2")
	leaderID, leader := waitForClusterLeader(t, stores)
	var follower *metastore.Store
	for id, s := range stores {
		if id != leaderID {
			follower = s
			break
		}
	}
	waitFor(t, 10*time.Second, "the follower to see the leader", func() bool { return follower.LeaderID() == leaderID })
	payload, err := nodewire.EncodeJoinClusterRequest(nodewire.JoinClusterRequest{ID: "narad-9", ClusterAddr: "10.0.0.9:7943"})
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}
	hint := func() map[string]string {
		t.Helper()
		res := NewRPCServer(nil, follower, log).handleJoinCluster(payload)
		if res.Status != http.StatusMisdirectedRequest {
			t.Fatalf("follower join status = %d %s, want 421", res.Status, res.Body)
		}
		var body map[string]string
		if err := json.Unmarshal(res.Body, &body); err != nil {
			t.Fatalf("421 body %q: %v", res.Body, err)
		}
		return body
	}

	// No member records yet: the leader is named, its address unknown.
	if body := hint(); body["leader_id"] != leaderID || body["leader_addr"] != "" || body["error"] == "" {
		t.Fatalf("421 body without member records = %v, want error, leader_id %s and no leader_addr", body, leaderID)
	}

	memberAddr := "narad-leader.narad-headless.default.svc.cluster.local:7942"
	if err := leader.RegisterMember(ctx, metastore.Member{ID: leaderID, Addr: memberAddr, ClusterAddr: leader.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	waitForMember(t, follower, leaderID)
	if body := hint(); body["leader_id"] != leaderID || body["leader_addr"] != memberAddr {
		t.Fatalf("421 body = %v, want leader_id %s and leader_addr %s", body, leaderID, memberAddr)
	}
}
