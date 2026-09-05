package cluster

// Scale-out admission: a join-only node must start with an EMPTY Raft
// configuration (no phantom bootstrap), be admitted by the leader via
// the OpJoinCluster handler, and then replicate the existing cluster
// state. The handler itself must refuse on non-leaders so the joiner
// walks its peer list to find the leader.

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
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

	// The leader admits; the joiner must converge on the leader and
	// replicate pre-existing state.
	leaderServer := NewRPCServer(nil, leader, log)
	if res := leaderServer.handleJoinCluster(payload); res.Status != http.StatusOK {
		t.Fatalf("leader join status = %d, want 200", res.Status)
	}
	waitFor(t, 10*time.Second, "joiner to see the leader", func() bool {
		return joiner.LeaderID() == "node-a"
	})
	waitFor(t, 10*time.Second, "joiner to replicate the topic", func() bool {
		_, err := joiner.GetTopic(ctx, "pre-existing")
		return err == nil
	})

	// Re-joining (lost reply, joiner restart) must stay idempotent.
	if res := leaderServer.handleJoinCluster(payload); res.Status != http.StatusOK {
		t.Fatalf("repeat join status = %d, want 200", res.Status)
	}

	// Writes now require the joiner in quorum (2 voters): prove the
	// admitted node participates by committing new state through the
	// leader and reading it back on the joiner.
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

	// A fresh node under the same ID: readmitted and its tombstone
	// cleared before AddVoter. The address is unreachable here, so the
	// configuration change may time out (503) once the new voter is
	// needed for quorum; the tombstone is what this test pins.
	fresh, err := nodewire.EncodeJoinClusterRequest(nodewire.JoinClusterRequest{ID: "node-z", ClusterAddr: "10.0.0.9:7943", Fresh: true})
	if err != nil {
		t.Fatalf("encode join request: %v", err)
	}
	res := server.handleJoinCluster(fresh)
	if res.Status != http.StatusOK && res.Status != http.StatusServiceUnavailable {
		t.Fatalf("fresh rejoin status = %d, want 200 (or 503 from an unreachable AddVoter)", res.Status)
	}
	if removed, _ := leader.MemberRemoved("node-z"); removed {
		t.Fatal("tombstone not cleared by a fresh rejoin")
	}
}
