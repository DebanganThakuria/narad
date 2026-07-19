package controller

// Decommission's completion half: once a draining node owns nothing, the
// controller removes it from the Raft voter set — but only when the two
// guards allow (keep at least MinVoters; never remove the current leader
// without transferring leadership first). And never while it still owns
// partitions (moves in flight).

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func decomStore(t *testing.T) *fakeControllerStore {
	t.Helper()
	store := newFakeControllerStore("a", "b", "c", "d")
	store.members[3].Draining = true // d is draining
	store.leaderID = "a"
	store.topics = []topic.Topic{{Name: "orders", Partitions: 3}}
	return store
}

func TestDecommissionRemovesDrainedNode(t *testing.T) {
	store := decomStore(t)
	// d owns nothing (fully drained); a,b,c own the partitions.
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 1 || store.removed[0] != "d" {
		t.Fatalf("removed = %v, want [d]", store.removed)
	}
}

func TestDecommissionWaitsWhileNodeStillOwns(t *testing.T) {
	store := decomStore(t)
	// d still owns partition 2 — a move is still in flight.
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "d"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 0 {
		t.Fatalf("removed a node that still owns a partition: %v", store.removed)
	}
}

func TestDecommissionRespectsMinVoters(t *testing.T) {
	// Only 3 voters; removing one would drop to 2, below the floor.
	store := newFakeControllerStore("a", "b", "c")
	store.members[2].Draining = true // c draining, owns nothing
	store.leaderID = "a"
	store.topics = []topic.Topic{{Name: "orders", Partitions: 2}}
	store.assignments["orders"] = map[int]string{0: "a", 1: "b"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()} // MinVoters 3

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 0 {
		t.Fatalf("removed a node that would drop voters below MinVoters: %v", store.removed)
	}
}

func TestDecommissionTransfersLeadershipOffDepartingLeader(t *testing.T) {
	store := decomStore(t)
	store.leaderID = "d" // the draining, drained node is the leader
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if store.transferred == 0 {
		t.Fatal("did not transfer leadership off the departing leader")
	}
	if len(store.removed) != 0 {
		t.Fatalf("removed the leader from its own config: %v", store.removed)
	}
}

func TestDecommissionNoopWithoutDrainingNodes(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c", "d")
	store.leaderID = "a"
	store.topics = []topic.Topic{{Name: "orders", Partitions: 4}}
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c", 3: "d"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileDecommission(context.Background())
	if len(store.removed) != 0 || store.transferred != 0 {
		t.Fatalf("acted with no draining nodes: removed %v, transfers %d", store.removed, store.transferred)
	}
}
