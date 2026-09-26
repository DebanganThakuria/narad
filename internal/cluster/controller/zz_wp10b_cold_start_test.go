package controller_test

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func wp10bOwnerCounts(t *testing.T, s *metastore.Store, topicName string) (int, map[string]int) {
	t.Helper()
	assignments, err := s.ListAssignments(topicName)
	if err != nil {
		return 0, nil
	}
	counts := map[string]int{}
	for _, a := range assignments {
		counts[a.OwnerID]++
	}
	return len(assignments), counts
}

func wp10bWaitAssigned(t *testing.T, s *metastore.Store, topicName string, partitions int, within time.Duration) map[string]int {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if n, counts := wp10bOwnerCounts(t, s, topicName); n == partitions {
			return counts
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, counts := wp10bOwnerCounts(t, s, topicName)
	t.Fatalf("%d/%d partitions of %s assigned after %v (owners %v); the controller waited for its reconcile tick", n, partitions, topicName, within, counts)
	return nil
}

// Cold start: a topic created before any member registered has nothing
// to be assigned to. The leader used to leave it unowned until its next
// ReconcileInterval tick (10 s in serve); it must place it as soon as
// the members arrive, spread across all of them.
func TestWP10BAssignsSoonAfterMembersRegister(t *testing.T) {
	s := newTestStore(t) // Raft voter narad-0
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c := controller.New(s, controller.Config{ReconcileInterval: time.Hour, DeadTimeout: time.Hour})
	go c.Run(ctx)

	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 6}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// Let the leader's first pass run and find no member to assign to.
	time.Sleep(300 * time.Millisecond)
	if n, _ := wp10bOwnerCounts(t, s, "orders"); n != 0 {
		t.Fatalf("%d partitions assigned with no member registered", n)
	}

	// The voter last, so the pass cannot run before all three are in.
	registerMember(t, s, "narad-1")
	registerMember(t, s, "narad-2")
	registerMember(t, s, "narad-0")

	counts := wp10bWaitAssigned(t, s, "orders", 6, 3*time.Second)
	for _, id := range []string{"narad-0", "narad-1", "narad-2"} {
		if counts[id] != 2 {
			t.Fatalf("owners = %v, want 2 partitions on each member", counts)
		}
	}
}

// The first member to arrive must not receive every partition: the pass
// waits MemberSettleDelay for more members while a voter is missing.
func TestWP10BFirstArrivalDoesNotTakeEveryPartition(t *testing.T) {
	s := newTestStore(t) // Raft voter narad-0, never registered here
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	c := controller.New(s, controller.Config{
		ReconcileInterval: time.Hour,
		DeadTimeout:       time.Hour,
		MemberSettleDelay: 1500 * time.Millisecond,
	})
	go c.Run(ctx)

	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 6}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	registerMember(t, s, "narad-1")
	time.Sleep(300 * time.Millisecond)
	if n, counts := wp10bOwnerCounts(t, s, "orders"); n != 0 {
		t.Fatalf("partitions assigned %v before the settle delay passed", counts)
	}
	registerMember(t, s, "narad-2")

	counts := wp10bWaitAssigned(t, s, "orders", 6, 5*time.Second)
	if counts["narad-1"] != 3 || counts["narad-2"] != 3 {
		t.Fatalf("owners = %v, want 3 partitions on each of the two arrivals", counts)
	}
}
