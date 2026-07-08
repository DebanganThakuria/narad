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

	// A non-leader must refuse so the joiner tries the next peer.
	followerServer := NewRPCServer(nil, joiner, log)
	if res := followerServer.handleJoinCluster(payload); res.Status != http.StatusMisdirectedRequest {
		t.Fatalf("non-leader join status = %d, want %d", res.Status, http.StatusMisdirectedRequest)
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
