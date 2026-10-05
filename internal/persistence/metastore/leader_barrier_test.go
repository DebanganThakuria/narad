package metastore

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// freeLoopbackAddrs reserves n loopback TCP addresses for a test
// cluster: Raft peers must know each other's addresses up front, so
// ":0" cannot be used.
func freeLoopbackAddrs(t *testing.T, n int) []string {
	t.Helper()
	listeners := make([]net.Listener, 0, n)
	addrs := make([]string, 0, n)
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve address: %v", err)
		}
		listeners = append(listeners, l)
		addrs = append(addrs, l.Addr().String())
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return addrs
}

// startTestCluster starts an n-voter in-process Raft cluster and
// returns its stores, with every store closed at test end.
func startTestCluster(t *testing.T, n int) (stores []*Store, ids, addrs []string) {
	t.Helper()
	addrs = freeLoopbackAddrs(t, n)
	baseDir := t.TempDir()
	for i := range n {
		ids = append(ids, fmt.Sprintf("node-%d", i+1))
	}
	for i := range n {
		var peers []Peer
		for j := range n {
			if j != i {
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
		t.Cleanup(func() { _ = s.Close() })
		stores = append(stores, s)
	}
	return stores, ids, addrs
}

// waitLeaderIndex waits until some store leads and has committed a
// write in its term, and returns its index.
func waitLeaderIndex(t *testing.T, stores []*Store) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for i, s := range stores {
			if !s.IsLeader() {
				continue
			}
			if err := s.CreateTopic(context.Background(), topic.Topic{Name: "__leader_probe__", Partitions: 1}); err == nil {
				_ = s.DeleteTopic(context.Background(), "__leader_probe__")
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a leader")
	return -1
}

// waitIsLeader waits until s leads.
func waitIsLeader(t *testing.T, s *Store) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if s.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leadership to arrive")
}

// A just-elected leader's FSM may lag its log, so the first topic
// read-modify-write of a term must barrier first. Doing it on every
// mutation would add a Raft round trip to each; once per term is
// enough, because a leader's FSM only falls behind its own log across
// an election. A follower's proposal fails anyway, so it never
// barriers, and concurrent first callers share one barrier.
func TestLeaderBarrierRunsOncePerTerm(t *testing.T) {
	stores, ids, addrs := startTestCluster(t, 3)
	ctx := context.Background()

	li := waitLeaderIndex(t, stores)
	leader := stores[li]
	fi := (li + 1) % len(stores)
	follower := stores[fi]

	start := leader.leaderBarriers.Load()
	for range 3 {
		if err := leader.LeaderBarrier(ctx); err != nil {
			t.Fatalf("LeaderBarrier on the leader: %v", err)
		}
	}
	if got := leader.leaderBarriers.Load() - start; got != 1 {
		t.Fatalf("barriers in one term = %d, want 1", got)
	}

	if err := follower.LeaderBarrier(ctx); err != nil {
		t.Fatalf("LeaderBarrier on a follower: %v", err)
	}
	if got := follower.leaderBarriers.Load(); got != 0 {
		t.Fatalf("a follower ran %d barriers, want 0", got)
	}

	// Hand leadership away and back: a new term, so the next mutation
	// must barrier again, and only once for every concurrent caller.
	if err := leader.r.LeadershipTransferToServer(raft.ServerID(ids[fi]), raft.ServerAddress(addrs[fi])).Error(); err != nil {
		t.Fatalf("transfer to %s: %v", ids[fi], err)
	}
	waitIsLeader(t, follower)
	if err := follower.r.LeadershipTransferToServer(raft.ServerID(ids[li]), raft.ServerAddress(addrs[li])).Error(); err != nil {
		t.Fatalf("transfer back to %s: %v", ids[li], err)
	}
	waitIsLeader(t, leader)

	start = leader.leaderBarriers.Load()
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() { errs[i] = leader.LeaderBarrier(ctx) })
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("LeaderBarrier in the new term: %v", err)
		}
	}
	if got := leader.leaderBarriers.Load() - start; got != 1 {
		t.Fatalf("barriers by 8 concurrent callers in a new term = %d, want 1", got)
	}
}
