package controller_test

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func newTestStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{
		NodeID:   "narad-0",
		DataDir:  t.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	waitLeader(t, s)
	return s
}

// waitLeader blocks until the store becomes the Raft leader.
func waitLeader(t *testing.T, s *metastore.Store) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.IsLeader() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for raft leader")
}

func registerMember(t *testing.T, s *metastore.Store, id string) {
	t.Helper()
	err := s.RegisterMember(context.Background(), metastore.Member{
		ID:            id,
		Addr:          id + ":7943",
		Status:        metastore.MemberAlive,
		LastHeartbeat: time.Now().Unix(),
	})
	if err != nil {
		t.Fatalf("RegisterMember %s: %v", id, err)
	}
}

// newController creates a controller with fast intervals for testing.
func newController(s *metastore.Store) *controller.Controller {
	return controller.New(s, controller.Config{
		ReconcileInterval: 100 * time.Millisecond,
		DeadTimeout:       500 * time.Millisecond,
	})
}

func TestReconcileAssignsNewTopic(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerMember(t, s, "narad-0")
	registerMember(t, s, "narad-1")
	registerMember(t, s, "narad-2")

	s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 6})

	c := newController(s)
	go c.Run(ctx)

	// Wait for the reconcile loop to assign all 6 partitions.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		assignments, _ := s.ListAssignments("orders")
		if len(assignments) == 6 {
			// Verify load is roughly balanced across 3 members.
			counts := map[string]int{}
			for _, a := range assignments {
				counts[a.OwnerID]++
			}
			if len(counts) != 3 {
				t.Fatalf("expected 3 distinct owners, got %v", counts)
			}
			for id, n := range counts {
				if n != 2 {
					t.Fatalf("member %s owns %d partitions, expected 2", id, n)
				}
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for partition assignments")
}

func TestReconcileDoesNotMoveExistingAssignments(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	registerMember(t, s, "narad-0")
	s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 4})

	// Pre-assign all partitions to narad-0.
	for p := 0; p < 4; p++ {
		s.AssignPartition(ctx, "orders", p, "narad-0")
	}

	// Register a second (less-loaded) member.
	registerMember(t, s, "narad-1")

	c := newController(s)
	go c.Run(ctx)
	time.Sleep(300 * time.Millisecond) // let reconcile run at least twice

	// All partitions must still be on narad-0 — reconcile never moves existing assignments.
	assignments, _ := s.ListAssignments("orders")
	for _, a := range assignments {
		if a.OwnerID != "narad-0" {
			t.Fatalf("partition %d moved to %s, expected narad-0", a.Partition, a.OwnerID)
		}
	}
}

func TestHeartbeatMonitorMarksDead(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Register a member with a stale heartbeat (1000 seconds ago).
	s.RegisterMember(ctx, metastore.Member{
		ID:            "narad-1",
		Addr:          "narad-1:7943",
		Status:        metastore.MemberAlive,
		LastHeartbeat: time.Now().Unix() - 1000,
	})

	c := newController(s)
	go c.Run(ctx)

	// The controller should mark narad-1 dead within one reconcile cycle.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, err := s.GetMember("narad-1")
		if err == nil && m.Status == metastore.MemberDead {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out waiting for member to be marked dead")
}

func TestHeartbeaterUpdatesTimestamp(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hb := controller.NewHeartbeater(s, metastore.Member{
		ID:     "narad-0",
		Addr:   "narad-0:7942",
		Status: metastore.MemberAlive,
	}, 100*time.Millisecond)
	go hb.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := s.GetMember("narad-0")
		if m.LastHeartbeat > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("heartbeat never updated")
}
