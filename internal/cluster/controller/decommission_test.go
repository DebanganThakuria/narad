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
	// The second half: the member record goes with the voter, so the
	// departed pod is not listed forever (nor resurrected by heartbeat).
	if len(store.forgotten) != 1 || store.forgotten[0] != "d" {
		t.Fatalf("forgotten = %v, want [d]", store.forgotten)
	}
	for _, m := range store.members {
		if m.ID == "d" {
			t.Fatal("member d still listed after removal")
		}
	}
}

func TestDecommissionForgetsDrainedNodeAlreadyOutOfVoterSet(t *testing.T) {
	// A leadership change between RemoveServer and RemoveMember leaves a
	// draining member that is no longer a voter; the new leader must
	// still forget it rather than skip it as "already removed".
	store := decomStore(t)
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	store.voters = []string{"a", "b", "c"} // d already out of the configuration
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 0 {
		t.Fatalf("RemoveServer called for a node already out of the configuration: %v", store.removed)
	}
	if len(store.forgotten) != 1 || store.forgotten[0] != "d" {
		t.Fatalf("forgotten = %v, want [d]", store.forgotten)
	}
}

func TestDecommissionWaitsWhileNodeStillOwns(t *testing.T) {
	store := decomStore(t)
	// d still owns partition 2 — a move is still in flight.
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "d"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 0 || len(store.forgotten) != 0 {
		t.Fatalf("acted on a node that still owns a partition: removed %v, forgotten %v", store.removed, store.forgotten)
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

	if len(store.removed) != 0 || len(store.forgotten) != 0 {
		t.Fatalf("acted on a node that would drop voters below MinVoters: removed %v, forgotten %v", store.removed, store.forgotten)
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
	if len(store.removed) != 0 || len(store.forgotten) != 0 {
		t.Fatalf("removed the leader from its own config: removed %v, forgotten %v", store.removed, store.forgotten)
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
