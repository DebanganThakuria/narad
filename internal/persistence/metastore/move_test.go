package metastore_test

// The partition-move state machine is what keeps rebalance split-brain
// free: OwnerID names exactly one node at every point in the Raft log,
// and the ownership flip is a guarded compare-and-swap that fails
// (rather than silently overwriting) when its preconditions no longer
// hold. These tests pin those guarantees.

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestSetTargetPreservesOwner(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreateAndAssign(t, s, "orders", 0, "narad-0")

	if err := s.SetAssignmentTarget(ctx, "orders", 0, "narad-1"); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}
	a, err := s.GetAssignment("orders", 0)
	if err != nil {
		t.Fatalf("GetAssignment: %v", err)
	}
	if a.OwnerID != "narad-0" || a.TargetID != "narad-1" {
		t.Fatalf("assignment = owner %q target %q, want narad-0 / narad-1", a.OwnerID, a.TargetID)
	}
}

func TestCompleteMoveFlipsOwnershipOnMatch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreateAndAssign(t, s, "orders", 0, "narad-0")
	if err := s.SetAssignmentTarget(ctx, "orders", 0, "narad-1"); err != nil {
		t.Fatalf("set target: %v", err)
	}

	if err := s.CompleteMove(ctx, "orders", 0, "narad-0", "narad-1"); err != nil {
		t.Fatalf("CompleteMove: %v", err)
	}
	a, _ := s.GetAssignment("orders", 0)
	if a.OwnerID != "narad-1" || a.TargetID != "" {
		t.Fatalf("after flip = owner %q target %q, want narad-1 / empty", a.OwnerID, a.TargetID)
	}
}

// The CAS guard: if the owner changed since the destination started
// copying (e.g. a re-plan reassigned it), the flip must FAIL — never
// silently overwrite. This is the split-brain guard.
func TestCompleteMoveRejectsStaleOwner(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreateAndAssign(t, s, "orders", 0, "narad-0")
	s.SetAssignmentTarget(ctx, "orders", 0, "narad-1")

	// A destination that thinks the owner is still narad-2 must be rejected.
	if err := s.CompleteMove(ctx, "orders", 0, "narad-2", "narad-1"); err == nil {
		t.Fatal("CompleteMove with wrong expected owner must fail (CAS guard)")
	}
	a, _ := s.GetAssignment("orders", 0)
	if a.OwnerID != "narad-0" {
		t.Fatalf("owner changed to %q on a rejected flip", a.OwnerID)
	}
}

// If the move was retargeted (target changed) before the flip, an
// in-flight destination's flip for the OLD target must fail.
func TestCompleteMoveRejectsStaleTarget(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreateAndAssign(t, s, "orders", 0, "narad-0")
	s.SetAssignmentTarget(ctx, "orders", 0, "narad-1")
	// Re-plan retargets the move to narad-2.
	s.SetAssignmentTarget(ctx, "orders", 0, "narad-2")

	if err := s.CompleteMove(ctx, "orders", 0, "narad-0", "narad-1"); err == nil {
		t.Fatal("CompleteMove for a superseded target must fail")
	}
	a, _ := s.GetAssignment("orders", 0)
	if a.OwnerID != "narad-0" || a.TargetID != "narad-2" {
		t.Fatalf("state corrupted by rejected flip: owner %q target %q", a.OwnerID, a.TargetID)
	}
}

func TestAbortMoveClearsMatchingTargetOnly(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mustCreateAndAssign(t, s, "orders", 0, "narad-0")
	s.SetAssignmentTarget(ctx, "orders", 0, "narad-1")

	// Aborting a superseded target is a no-op — a re-plan to narad-2 wins.
	s.SetAssignmentTarget(ctx, "orders", 0, "narad-2")
	if err := s.AbortMove(ctx, "orders", 0, "narad-1"); err != nil {
		t.Fatalf("AbortMove (stale) should no-op, got %v", err)
	}
	if a, _ := s.GetAssignment("orders", 0); a.TargetID != "narad-2" {
		t.Fatalf("stale abort clobbered the current target: %q", a.TargetID)
	}

	// Aborting the current target clears it, owner untouched.
	if err := s.AbortMove(ctx, "orders", 0, "narad-2"); err != nil {
		t.Fatalf("AbortMove: %v", err)
	}
	a, _ := s.GetAssignment("orders", 0)
	if a.OwnerID != "narad-0" || a.TargetID != "" {
		t.Fatalf("after abort = owner %q target %q, want narad-0 / empty", a.OwnerID, a.TargetID)
	}
}

func mustCreateAndAssign(t *testing.T, s interface {
	CreateTopic(context.Context, topic.Topic) error
	AssignPartition(context.Context, string, int, string) error
}, topicName string, partition int, owner string) {
	t.Helper()
	ctx := context.Background()
	if err := s.CreateTopic(ctx, topic.Topic{Name: topicName, Partitions: partition + 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := s.AssignPartition(ctx, topicName, partition, owner); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
}
