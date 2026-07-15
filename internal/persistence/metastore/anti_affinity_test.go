package metastore_test

// The replica pattern: a fan-out child's partition p must not land on
// the node that owns the parent's partition p, or the "second copy"
// shares a disk with the original and survives nothing. Pure-function
// coverage of the shared owner-picking decision, then end-to-end
// coverage through a real Raft store with real AttachChild links.

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func aliveMembers(ids ...string) []metastore.Member {
	out := make([]metastore.Member, len(ids))
	for i, id := range ids {
		out[i] = metastore.Member{ID: id, Status: metastore.MemberAlive}
	}
	return out
}

func TestAntiAffineOwnerAvoidsParentOwner(t *testing.T) {
	active := aliveMembers("narad-0", "narad-1", "narad-2")
	for p := 0; p < 9; p++ {
		canonical, _ := metastore.RoundRobinOwner(active, p)
		owner, ok := metastore.AntiAffineOwner(active, p, canonical)
		if !ok {
			t.Fatalf("partition %d: no owner", p)
		}
		if owner == canonical {
			t.Fatalf("partition %d: anti-affine pick %q equals the avoided owner", p, owner)
		}
	}
}

// The avoided owner isn't always the canonical pick — a parent assigned
// under an older member set can own any position. The walk must skip it
// wherever it sits.
func TestAntiAffineOwnerAvoidsNonCanonicalOwner(t *testing.T) {
	active := aliveMembers("narad-0", "narad-1", "narad-2")
	owner, ok := metastore.AntiAffineOwner(active, 1, "narad-2")
	if !ok || owner == "narad-2" {
		t.Fatalf("owner = %q, ok = %v; want a member other than narad-2", owner, ok)
	}
}

// One live member: a colocated copy beats an unassigned partition.
func TestAntiAffineOwnerSingleMemberFallsBack(t *testing.T) {
	active := aliveMembers("narad-0")
	owner, ok := metastore.AntiAffineOwner(active, 3, "narad-0")
	if !ok || owner != "narad-0" {
		t.Fatalf("owner = %q, ok = %v; want fallback to narad-0", owner, ok)
	}
}

func TestAntiAffineOwnerEmptyAndNegative(t *testing.T) {
	if _, ok := metastore.AntiAffineOwner(nil, 0, "x"); ok {
		t.Fatal("empty member list must not yield an owner")
	}
	if _, ok := metastore.AntiAffineOwner(aliveMembers("a"), -1, "x"); ok {
		t.Fatal("negative partition must not yield an owner")
	}
}

func TestChildAwareOwnerDecisions(t *testing.T) {
	active := aliveMembers("narad-0", "narad-1", "narad-2")
	parentOwners := map[int]string{0: "narad-0", 1: "narad-1"} // partition 2 unassigned

	tests := []struct {
		name             string
		partition        int
		parentOwners     map[int]string
		parentPartitions int
		wantDeferred     bool
		wantNot          string // owner must differ from this; "" = no constraint
	}{
		{"standalone topic", 1, nil, 0, false, ""},
		{"child avoids parent owner", 0, parentOwners, 3, false, "narad-0"},
		{"child avoids parent owner p1", 1, parentOwners, 3, false, "narad-1"},
		{"parent counterpart unassigned defers", 2, parentOwners, 3, true, ""},
		{"beyond parent range unconstrained", 5, parentOwners, 3, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, ok, deferred := metastore.ChildAwareOwner(active, tt.partition, tt.parentOwners, tt.parentPartitions)
			if deferred != tt.wantDeferred {
				t.Fatalf("deferred = %v, want %v", deferred, tt.wantDeferred)
			}
			if tt.wantDeferred {
				return
			}
			if !ok {
				t.Fatal("no owner picked")
			}
			if tt.wantNot != "" && owner == tt.wantNot {
				t.Fatalf("owner = %q, must avoid %q", owner, tt.wantNot)
			}
		})
	}
}

// registerAliveMembers registers fake pods so assignment has owners to
// round-robin over.
func registerAliveMembers(t *testing.T, s *metastore.Store, ids ...string) {
	t.Helper()
	for _, id := range ids {
		m := metastore.Member{ID: id, Addr: id + ":7942", Status: metastore.MemberAlive}
		if err := s.RegisterMember(context.Background(), m); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
}

// End-to-end through a real store and a real attach: a child created
// then attached BEFORE assignment gets every partition on a different
// node than the parent's same-index partition.
func TestAssignNewPartitionsAntiAffineForAttachedChild(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	registerAliveMembers(t, s, "narad-0", "narad-1", "narad-2")

	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 6, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic(orders): %v", err)
	}
	if err := s.AssignNewPartitions(ctx, "orders", 0, 6); err != nil {
		t.Fatalf("assign parent: %v", err)
	}
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders-replica", Partitions: 6, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic(orders-replica): %v", err)
	}
	if err := s.AttachChild(ctx, "orders", "orders-replica", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}
	if err := s.AssignNewPartitions(ctx, "orders-replica", 0, 6); err != nil {
		t.Fatalf("assign child: %v", err)
	}

	parent := assignmentsByPartition(t, s, "orders")
	child := assignmentsByPartition(t, s, "orders-replica")
	if len(parent) != 6 || len(child) != 6 {
		t.Fatalf("assigned %d parent / %d child partitions, want 6/6", len(parent), len(child))
	}
	for p := 0; p < 6; p++ {
		if parent[p] == child[p] {
			t.Fatalf("partition %d: parent and child both owned by %q — copies share a disk", p, parent[p])
		}
	}
}

// A child whose parent is not yet assigned must wait, not guess.
func TestAssignNewPartitionsDefersChildUntilParentAssigned(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	registerAliveMembers(t, s, "narad-0", "narad-1")

	for _, name := range []string{"orders", "orders-replica"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3, RetentionMs: 3_600_000}); err != nil {
			t.Fatalf("CreateTopic(%s): %v", name, err)
		}
	}
	if err := s.AttachChild(ctx, "orders", "orders-replica", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}

	// Parent exists but has no assignments yet.
	if err := s.AssignNewPartitions(ctx, "orders-replica", 0, 3); err != nil {
		t.Fatalf("assign child: %v", err)
	}
	if got := assignmentsByPartition(t, s, "orders-replica"); len(got) != 0 {
		t.Fatalf("child assigned %v before its parent was placed", got)
	}

	// Once the parent is placed, the child follows, anti-affine.
	if err := s.AssignNewPartitions(ctx, "orders", 0, 3); err != nil {
		t.Fatalf("assign parent: %v", err)
	}
	if err := s.AssignNewPartitions(ctx, "orders-replica", 0, 3); err != nil {
		t.Fatalf("assign child after parent: %v", err)
	}
	parent := assignmentsByPartition(t, s, "orders")
	child := assignmentsByPartition(t, s, "orders-replica")
	if len(child) != 3 {
		t.Fatalf("child assignments = %v, want all 3 partitions placed", child)
	}
	for p := 0; p < 3; p++ {
		if parent[p] == child[p] {
			t.Fatalf("partition %d: colocated on %q", p, parent[p])
		}
	}
}

// Sticky above all: re-running assignment must never move anything,
// even where an existing child assignment happens to be colocated
// (e.g. a child attached the two-step way before this feature).
func TestAssignNewPartitionsNeverMovesExisting(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	registerAliveMembers(t, s, "narad-0", "narad-1", "narad-2")

	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic(orders): %v", err)
	}
	if err := s.AssignNewPartitions(ctx, "orders", 0, 3); err != nil {
		t.Fatalf("assign parent: %v", err)
	}
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders-replica", Partitions: 3, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic(orders-replica): %v", err)
	}
	// Simulate a pre-feature colocated child: same owners as the parent,
	// assigned before the attach.
	parent := assignmentsByPartition(t, s, "orders")
	for p := 0; p < 3; p++ {
		if err := s.AssignPartition(ctx, "orders-replica", p, parent[p]); err != nil {
			t.Fatalf("seed colocated assignment: %v", err)
		}
	}
	if err := s.AttachChild(ctx, "orders", "orders-replica", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}

	if err := s.AssignNewPartitions(ctx, "orders-replica", 0, 3); err != nil {
		t.Fatalf("re-run assign: %v", err)
	}
	child := assignmentsByPartition(t, s, "orders-replica")
	for p := 0; p < 3; p++ {
		if child[p] != parent[p] {
			t.Fatalf("partition %d moved from %q to %q — assignments must be sticky", p, parent[p], child[p])
		}
	}
}

func assignmentsByPartition(t *testing.T, s *metastore.Store, topicName string) map[int]string {
	t.Helper()
	assignments, err := s.ListAssignments(topicName)
	if err != nil {
		t.Fatalf("ListAssignments(%s): %v", topicName, err)
	}
	out := make(map[int]string, len(assignments))
	for _, a := range assignments {
		out[a.Partition] = a.OwnerID
	}
	return out
}
