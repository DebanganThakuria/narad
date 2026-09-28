package metastore

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func wp10bResetJoinSeen(t *testing.T) {
	t.Helper()
	reset := func() {
		joinSeen.mu.Lock()
		joinSeen.since = time.Time{}
		joinSeen.mu.Unlock()
	}
	reset()
	t.Cleanup(reset)
}

func wp10bFreeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// wp10bThreeVoters starts a three-voter Raft cluster and returns its
// leader (ownership view ready) and the voter IDs. The stores open
// concurrently: opened one after another under load, the first two could
// elect a leader and replicate to the third before it bootstrapped,
// which then refused to bootstrap a non-empty log.
func wp10bThreeVoters(t *testing.T) (*Store, []string) {
	t.Helper()
	ids := []string{"narad-1", "narad-2", "narad-3"}
	addrs := []string{wp10bFreeAddr(t), wp10bFreeAddr(t), wp10bFreeAddr(t)}
	base := t.TempDir()
	stores := make([]*Store, len(ids))
	errs := make([]error, len(ids))
	var wg sync.WaitGroup
	for i := range ids {
		var peers []Peer
		for j := range ids {
			if j != i {
				peers = append(peers, Peer{ID: ids[j], Addr: addrs[j]})
			}
		}
		wg.Go(func() {
			stores[i], errs[i] = New(Config{
				NodeID:        ids[i],
				DataDir:       filepath.Join(base, ids[i]),
				BindAddr:      addrs[i],
				AdvertiseAddr: addrs[i],
				Peers:         peers,
			})
		})
	}
	wg.Wait()
	for i, s := range stores {
		if s != nil {
			t.Cleanup(func() { _ = s.Close() })
		}
		if errs[i] != nil {
			t.Fatalf("New(%s): %v", ids[i], errs[i])
		}
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, s := range stores {
			if !s.IsLeader() {
				continue
			}
			if _, err := s.ListAssignments("__probe__"); err == nil {
				return s, ids
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a leader with a ready ownership view")
	return nil, nil
}

func wp10bRegister(t *testing.T, s *Store, id string) {
	t.Helper()
	m := Member{ID: id, Addr: id + ":7943", Status: MemberAlive, LastHeartbeat: time.Now().Unix()}
	if err := s.RegisterMember(context.Background(), m); err != nil {
		t.Fatalf("RegisterMember(%s): %v", id, err)
	}
}

func wp10bOwnerCounts(t *testing.T, s *Store, topicName string) map[string]int {
	t.Helper()
	assignments, err := s.ListAssignments(topicName)
	if err != nil {
		t.Fatalf("ListAssignments(%s): %v", topicName, err)
	}
	counts := map[string]int{}
	for _, a := range assignments {
		counts[a.OwnerID]++
	}
	return counts
}

// Fresh cluster: the leader has registered, the other voters have not yet.
// A create placed then put every partition on the leader, and rebalance
// moved most of them away while clients were already producing and
// consuming. The create must wait for the other voters and spread.
func TestWP10BAssignNewPartitionsWaitsForJoiningVoters(t *testing.T) {
	wp10bResetJoinSeen(t)
	ctx := context.Background()
	s, ids := wp10bThreeVoters(t)
	wp10bRegister(t, s, s.LeaderID())
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 6, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.AssignNewPartitions(ctx, "orders", 0, 6) }()
	time.Sleep(300 * time.Millisecond)
	for _, id := range ids {
		if id != s.LeaderID() {
			wp10bRegister(t, s, id)
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("AssignNewPartitions: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AssignNewPartitions did not return")
	}

	counts := wp10bOwnerCounts(t, s, "orders")
	for _, id := range ids {
		if counts[id] != 2 {
			t.Fatalf("owners = %v, want 2 partitions on each of %v; the create placed on the members registered so far", counts, ids)
		}
	}
}

// A voter that never registers (a node that never started) must not stall
// creates for good: the first one waits at most joinWait and places on
// the members it has, and later ones do not wait again.
func TestWP10BAssignNewPartitionsJoinWaitIsBounded(t *testing.T) {
	wp10bResetJoinSeen(t)
	ctx := context.Background()
	s, ids := wp10bThreeVoters(t)
	var registered []string
	for _, id := range ids {
		if len(registered) < 2 {
			wp10bRegister(t, s, id)
			registered = append(registered, id)
		}
	}

	for i, name := range []string{"first", "second"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 4, RetentionMs: 3_600_000}); err != nil {
			t.Fatalf("CreateTopic(%s): %v", name, err)
		}
		start := time.Now()
		if err := s.AssignNewPartitions(ctx, name, 0, 4); err != nil {
			t.Fatalf("AssignNewPartitions(%s): %v", name, err)
		}
		took := time.Since(start)
		if took > joinWait+2*time.Second {
			t.Fatalf("%s create waited %v, want at most about joinWait (%v)", name, took, joinWait)
		}
		if i == 1 && took >= joinWait {
			t.Fatalf("second create waited %v after the first had already waited out joinWait", took)
		}
		counts := wp10bOwnerCounts(t, s, name)
		if counts[registered[0]] != 2 || counts[registered[1]] != 2 {
			t.Fatalf("%s owners = %v, want 2 on each registered member %v", name, counts, registered)
		}
	}
}

// The cluster counts as forming only while some voters have registered
// and others have not; a voter with a dead record has registered.
func TestWP10BVotersJoiningDecision(t *testing.T) {
	wp10bResetJoinSeen(t)
	s, ids := wp10bThreeVoters(t)
	members := func(registered ...string) []Member {
		out := make([]Member, 0, len(registered))
		for _, id := range registered {
			out = append(out, Member{ID: id, Status: MemberAlive})
		}
		return out
	}
	cases := []struct {
		name       string
		registered []string
		want       bool
	}{
		{"none registered", nil, false},
		{"only non-voters registered", []string{"other-1", "other-2"}, false},
		{"some voters registered", ids[:1], true},
		{"all voters registered", ids, false},
	}
	for _, tc := range cases {
		if got := s.votersJoining(members(tc.registered...)); got != tc.want {
			t.Fatalf("%s: votersJoining = %v, want %v", tc.name, got, tc.want)
		}
	}
	dead := members(ids...)
	dead[2].Status = MemberDead
	if s.votersJoining(dead) {
		t.Fatal("a voter with a dead record is waited for; it registered before and may never return")
	}
}
