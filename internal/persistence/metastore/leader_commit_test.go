package metastore

// The ownership latch must not set on a heartbeat alone. Heartbeats carry
// no LeaderCommitIndex, so a restarted follower that has heard one reads
// applied >= commit_index (0 >= 0) with its FSM still at the snapshot.
// These tests pin the two halves of the fix: the transport wrapper
// records the leader's commit index off real AppendEntries only, and a
// follower whose applied index is behind that number is neither caught
// up nor ready, however fresh its leader contact.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// fakeTransport is the minimal raft.Transport the wrapper test needs: a
// consumer channel the test feeds directly.
type fakeTransport struct {
	ch     chan raft.RPC
	closed bool
}

func (f *fakeTransport) Consumer() <-chan raft.RPC                               { return f.ch }
func (f *fakeTransport) LocalAddr() raft.ServerAddress                           { return "fake" }
func (f *fakeTransport) EncodePeer(_ raft.ServerID, a raft.ServerAddress) []byte { return []byte(a) }
func (f *fakeTransport) DecodePeer(b []byte) raft.ServerAddress                  { return raft.ServerAddress(b) }
func (f *fakeTransport) SetHeartbeatHandler(func(raft.RPC))                      {}
func (f *fakeTransport) Close() error                                            { f.closed = true; return nil }
func (f *fakeTransport) AppendEntriesPipeline(raft.ServerID, raft.ServerAddress) (raft.AppendPipeline, error) {
	return nil, errors.New("unsupported")
}
func (f *fakeTransport) AppendEntries(raft.ServerID, raft.ServerAddress, *raft.AppendEntriesRequest, *raft.AppendEntriesResponse) error {
	return errors.New("unsupported")
}
func (f *fakeTransport) RequestVote(raft.ServerID, raft.ServerAddress, *raft.RequestVoteRequest, *raft.RequestVoteResponse) error {
	return errors.New("unsupported")
}
func (f *fakeTransport) InstallSnapshot(raft.ServerID, raft.ServerAddress, *raft.InstallSnapshotRequest, *raft.InstallSnapshotResponse, io.Reader) error {
	return errors.New("unsupported")
}
func (f *fakeTransport) TimeoutNow(raft.ServerID, raft.ServerAddress, *raft.TimeoutNowRequest, *raft.TimeoutNowResponse) error {
	return errors.New("unsupported")
}

func TestCommitObservingTransportRecordsLeaderCommitFromRealAppendEntriesOnly(t *testing.T) {
	inner := &fakeTransport{ch: make(chan raft.RPC)}
	tr := newCommitObservingTransport(inner)
	t.Cleanup(func() { _ = tr.Close() })

	feed := func(cmd any) raft.RPC {
		t.Helper()
		in := raft.RPC{Command: cmd}
		select {
		case inner.ch <- in:
		case <-time.After(5 * time.Second):
			t.Fatal("relay did not accept the RPC")
		}
		select {
		case out := <-tr.Consumer():
			return out
		case <-time.After(5 * time.Second):
			t.Fatal("relay did not deliver the RPC")
		}
		return raft.RPC{}
	}

	// A heartbeat: no LeaderCommitIndex. Nothing learned.
	feed(&raft.AppendEntriesRequest{Term: 3})
	if got := tr.LeaderCommitIndex(); got != 0 {
		t.Fatalf("LeaderCommitIndex after heartbeat = %d, want 0", got)
	}

	// A real AppendEntries carries the leader's commit index.
	out := feed(&raft.AppendEntriesRequest{Term: 3, LeaderCommitIndex: 42})
	if ae, ok := out.Command.(*raft.AppendEntriesRequest); !ok || ae.LeaderCommitIndex != 42 {
		t.Fatalf("relayed command = %#v, want the AppendEntries unchanged", out.Command)
	}
	if got := tr.LeaderCommitIndex(); got != 42 {
		t.Fatalf("LeaderCommitIndex = %d, want 42", got)
	}

	// Monotone: an older (smaller) value from a stale leader never
	// lowers it, and other RPC kinds are ignored.
	feed(&raft.AppendEntriesRequest{Term: 2, LeaderCommitIndex: 7})
	feed(&raft.RequestVoteRequest{Term: 4})
	feed(&raft.InstallSnapshotRequest{Term: 4, LastLogIndex: 900})
	if got := tr.LeaderCommitIndex(); got != 42 {
		t.Fatalf("LeaderCommitIndex after stale/other RPCs = %d, want 42", got)
	}
	feed(&raft.AppendEntriesRequest{Term: 4, LeaderCommitIndex: 100})
	if got := tr.LeaderCommitIndex(); got != 100 {
		t.Fatalf("LeaderCommitIndex = %d, want 100", got)
	}

	if err := tr.Close(); err != nil || !inner.closed {
		t.Fatalf("Close: err=%v innerClosed=%v", err, inner.closed)
	}
	// Idempotent close must not panic.
	_ = tr.Close()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// threeNodeCluster boots a bootstrapped three-voter cluster on free
// ports and returns the stores in ID order.
func threeNodeCluster(t *testing.T) []*Store {
	t.Helper()
	baseDir := t.TempDir()
	ids := []string{"lc-1", "lc-2", "lc-3"}
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	stores := make([]*Store, 0, 3)
	for i := range ids {
		var peers []Peer
		for j := range ids {
			if i != j {
				peers = append(peers, Peer{ID: ids[j], Addr: addrs[j]})
			}
		}
		s, err := New(Config{
			NodeID:        ids[i],
			DataDir:       filepath.Join(baseDir, ids[i]),
			BindAddr:      addrs[i],
			AdvertiseAddr: addrs[i],
			Peers:         peers,
		})
		if err != nil {
			t.Fatalf("New(%s): %v", ids[i], err)
		}
		stores = append(stores, s)
	}
	t.Cleanup(func() {
		for _, s := range stores {
			_ = s.Close()
		}
	})
	return stores
}

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func appliedIndex(t *testing.T, s *Store) uint64 {
	t.Helper()
	var applied uint64
	if _, err := fmt.Sscan(s.r.Stats()["applied_index"], &applied); err != nil {
		t.Fatalf("applied_index: %v", err)
	}
	return applied
}

func TestFollowerLatchRefusesWhileLeaderCommitAheadOfApplied(t *testing.T) {
	ctx := context.Background()
	stores := threeNodeCluster(t)

	var leader *Store
	waitUntil(t, 15*time.Second, "a leader", func() bool {
		for _, s := range stores {
			if s.IsLeader() {
				leader = s
				return true
			}
		}
		return false
	})
	if err := leader.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := leader.AssignPartition(ctx, "orders", 0, "lc-owner"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	var follower *Store
	for _, s := range stores {
		if s != leader {
			follower = s
			break
		}
	}

	// Simulate what the wire does to a restarted follower: the leader
	// reports a commit index far beyond what this FSM has applied. Fresh
	// contact and a local applied >= commit_index must not be enough.
	follower.leaderCommit.leaderCommit.Store(1 << 40)
	waitUntil(t, 10*time.Second, "follower to see the leader", func() bool {
		return follower.LeaderID() == leader.LeaderID() && !follower.r.LastContact().IsZero()
	})
	if follower.AppliedCaughtUp() {
		t.Fatal("AppliedCaughtUp() = true with the leader's commit index ahead of applied")
	}
	if _, err := follower.ListAssignments("orders"); !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("ListAssignments err = %v, want ErrUnavailable while behind the leader's commit index", err)
	}
	if _, err := follower.GetAssignment("orders", 0); !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("GetAssignment err = %v, want ErrUnavailable while behind the leader's commit index", err)
	}
	if err := follower.ClusterReady(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("ClusterReady err = %v, want ErrNotReady while behind the leader's commit index", err)
	}

	// Once the leader's reported commit index is something this FSM has
	// applied (the leader has applied the assignment, so its applied
	// index covers it), the follower is caught up and latches.
	follower.leaderCommit.leaderCommit.Store(appliedIndex(t, leader))
	waitUntil(t, 10*time.Second, "follower to catch up", follower.AppliedCaughtUp)
	if err := follower.ClusterReady(); err != nil {
		t.Fatalf("ClusterReady after catch-up: %v", err)
	}
	// The gate is open now; the assignment is visible once the FSM has
	// applied the entry. The latch's fsm_pending guard makes that
	// immediate in practice, but the two stats are read one at a time.
	var rows []Assignment
	waitUntil(t, 10*time.Second, "assignment to be readable after catch-up", func() bool {
		var err error
		rows, err = follower.ListAssignments("orders")
		return err == nil && len(rows) == 1
	})
	if _, err := follower.GetAssignment("orders", 0); err != nil {
		t.Fatalf("GetAssignment after catch-up: %v", err)
	}

	// Readiness is live, not a latch: losing the leader (quorum gone)
	// flips the follower back to not ready while the ownership latch,
	// which is about the view's provenance, stays set.
	for _, s := range stores {
		if s != follower {
			_ = s.Close()
		}
	}
	waitUntil(t, 20*time.Second, "follower to lose its leader", func() bool {
		return errors.Is(follower.ClusterReady(), ErrNotReady)
	})
	if _, err := follower.ListAssignments("orders"); err != nil {
		t.Fatalf("ownership latch cleared on leader loss: %v", err)
	}
}

func TestFollowerCatchesUpFromLiveReplication(t *testing.T) {
	// Untouched, a follower learns the leader's commit index from the
	// leader's periodic AppendEntries (CommitTimeout) and latches on its
	// own within seconds.
	stores := threeNodeCluster(t)
	waitUntil(t, 15*time.Second, "a leader", func() bool {
		for _, s := range stores {
			if s.IsLeader() {
				return true
			}
		}
		return false
	})
	for _, s := range stores {
		if s.IsLeader() {
			continue
		}
		waitUntil(t, 10*time.Second, "follower "+s.r.String()+" to catch up", s.AppliedCaughtUp)
		if got := s.leaderCommitIndex(); got == 0 {
			t.Fatalf("follower caught up without a leader commit index")
		}
		if err := s.ClusterReady(); err != nil {
			t.Fatalf("ClusterReady: %v", err)
		}
	}
}

func TestClusterReadyWithoutLeader(t *testing.T) {
	// A join-only store (empty configuration) never sees a leader and
	// must not read as ready.
	s, err := New(Config{
		NodeID:        "lonely",
		DataDir:       t.TempDir(),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
		JoinOnly:      true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.ClusterReady(); !errors.Is(err, ErrNotReady) {
		t.Fatalf("ClusterReady on a leaderless node = %v, want ErrNotReady", err)
	}
	if s.AppliedCaughtUp() {
		t.Fatal("AppliedCaughtUp() = true on a leaderless node")
	}
}

func TestLeaderRequiresCommittedNoopBeforeCaughtUp(t *testing.T) {
	// A single-node store is caught up (and ready) once elected and its
	// term's no-op has committed and applied; both are visible through
	// commit_index > 0.
	s, err := New(Config{
		NodeID:        "solo",
		DataDir:       t.TempDir(),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	waitUntil(t, 10*time.Second, "leader caught up", s.AppliedCaughtUp)
	if err := s.ClusterReady(); err != nil {
		t.Fatalf("ClusterReady: %v", err)
	}
	if !s.ownershipReady.Load() {
		t.Fatal("ownership latch not set after a self-leader barrier")
	}
}
