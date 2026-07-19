package controller

// The rebalance wiring turns the pure planner into desired state: on the
// leader it barriers, reads placement, and writes Assignment.TargetID for
// the partitions that should move — bounded by MaxInFlightMoves, skipping
// partitions already in-flight, honoring anti-affinity, and leaving
// dead-owner partitions put. These tests pin that translation.

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func targetsSet(store *fakeControllerStore) int {
	n := 0
	for _, parts := range store.targets {
		for _, tgt := range parts {
			if tgt != "" {
				n++
			}
		}
	}
	return n
}

// A new empty node joins three balanced nodes; the leader sets targets to
// move partitions onto it, bounded by the in-flight cap.
func TestRebalanceTargetsNewNode(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c", "d")
	store.topics = []topic.Topic{{Name: "orders", Partitions: 12}}
	store.assignments["orders"] = map[int]string{}
	for p := 0; p < 12; p++ {
		store.assignments["orders"][p] = []string{"a", "b", "c"}[p%3] // 4 each, d empty
	}
	c := &Controller{store: store, cfg: Config{MaxInFlightMoves: 8}.withDefaults()}

	c.reconcileRebalance(context.Background())

	// 12 over 4 = 3 each ⇒ 3 partitions should move to d, all under the cap.
	if got := targetsSet(store); got != 3 {
		t.Fatalf("targets set = %d, want 3 (one off each of a,b,c)", got)
	}
	for _, entry := range store.targetLog {
		if entry[len(entry)-1] != 'd' {
			t.Fatalf("target %q should point at the new node d", entry)
		}
	}
}

// The in-flight cap bounds how many targets a single pass sets; already
// in-flight moves count against the budget.
func TestRebalanceRespectsInFlightCap(t *testing.T) {
	store := newFakeControllerStore("a", "b")
	store.topics = []topic.Topic{{Name: "orders", Partitions: 10}}
	store.assignments["orders"] = map[int]string{}
	for p := 0; p < 10; p++ {
		store.assignments["orders"][p] = "a" // all on a; b empty
	}
	c := &Controller{store: store, cfg: Config{MaxInFlightMoves: 3}.withDefaults()}

	c.reconcileRebalance(context.Background())
	// 10 over 2 = 5 each ⇒ 5 want to move, but the cap allows only 3 per pass.
	if got := targetsSet(store); got != 3 {
		t.Fatalf("targets set = %d, want 3 (capped)", got)
	}

	// A second pass with 3 already in-flight is at the cap: no new targets.
	before := len(store.targetLog)
	c.reconcileRebalance(context.Background())
	if len(store.targetLog) != before {
		t.Fatalf("pass at the cap set %d more targets, want 0", len(store.targetLog)-before)
	}
}

// A partition already in-flight is never re-targeted, and the planner counts
// it at its destination so the cluster still balances.
func TestRebalanceDoesNotRetargetInFlight(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c")
	store.topics = []topic.Topic{{Name: "orders", Partitions: 9}}
	store.assignments["orders"] = map[int]string{}
	for p := 0; p < 9; p++ {
		store.assignments["orders"][p] = "a"
	}
	// Partition 0 is already moving a→c.
	store.targets["orders"] = map[int]string{0: "c"}
	c := &Controller{store: store, cfg: Config{MaxInFlightMoves: 8}.withDefaults()}

	c.reconcileRebalance(context.Background())

	if got := store.targets["orders"][0]; got != "c" {
		t.Fatalf("in-flight partition 0 retargeted to %q, want c untouched", got)
	}
	// 9 over 3 = 3 each. c already gets 1 (in-flight). Fresh targets should
	// bring b to 3 and c to 3 without touching partition 0: 5 more moves,
	// capped at 8 so all 5 land.
	if fresh := len(store.targetLog); fresh != 5 {
		t.Fatalf("fresh targets = %d, want 5 (3 to b, 2 to c; partition 0 already in-flight)", fresh)
	}
}

// Anti-affinity: a fan-out child partition must not be targeted onto the node
// that owns its parent's same-index partition when a balanced alternative
// exists.
func TestRebalanceHonorsAntiAffinity(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c")
	// Parent balanced across a,b,c. Child all piled on a; must spread to b,c
	// but partition p must avoid the parent's owner of p.
	store.topics = []topic.Topic{
		{Name: "orders", Partitions: 3, Role: topic.RoleParent, Children: []string{"replica"}},
		{Name: "replica", Partitions: 3, Parent: "orders", Role: topic.RoleChild},
	}
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	store.assignments["replica"] = map[int]string{0: "a", 1: "a", 2: "a"}
	c := &Controller{store: store, cfg: Config{MaxInFlightMoves: 8}.withDefaults()}

	c.reconcileRebalance(context.Background())

	// Each child partition p ends up (owner or target) on a node != parent[p].
	parent := store.assignments["orders"]
	for p := 0; p < 3; p++ {
		holder := store.assignments["replica"][p]
		if tgt := store.targets["replica"][p]; tgt != "" {
			holder = tgt
		}
		if holder == parent[p] {
			t.Fatalf("child partition %d colocated with parent on %q", p, holder)
		}
	}
}

// A partition whose owner is dead is stuck (its data lives only there) and
// must not be moved, nor counted as a receiver candidate.
func TestRebalanceLeavesDeadOwnerPartitionsPut(t *testing.T) {
	store := newFakeControllerStore("a", "b")
	store.members = append(store.members, deadMember("c"))
	store.topics = []topic.Topic{{Name: "orders", Partitions: 4}}
	store.assignments["orders"] = map[int]string{0: "a", 1: "a", 2: "c", 3: "c"} // c is dead
	c := &Controller{store: store, cfg: Config{MaxInFlightMoves: 8}.withDefaults()}

	c.reconcileRebalance(context.Background())

	// c's partitions (2,3) must not be targeted anywhere.
	for _, p := range []int{2, 3} {
		if tgt := store.targets["orders"][p]; tgt != "" {
			t.Fatalf("dead-owner partition %d targeted to %q, want left put", p, tgt)
		}
	}
}

// A barrier failure aborts the pass without touching desired state.
func TestRebalanceSkipsOnBarrierFailure(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c")
	store.barrierErr = errors.New("not leader yet")
	store.topics = []topic.Topic{{Name: "orders", Partitions: 6}}
	store.assignments["orders"] = map[int]string{0: "a", 1: "a", 2: "a", 3: "a", 4: "a", 5: "a"}
	c := &Controller{store: store, cfg: Config{MaxInFlightMoves: 8}.withDefaults()}

	c.reconcileRebalance(context.Background())
	if got := targetsSet(store); got != 0 {
		t.Fatalf("set %d targets despite a barrier failure, want 0", got)
	}
}

func deadMember(id string) metastore.Member {
	return metastore.Member{ID: id, Status: metastore.MemberDead}
}
