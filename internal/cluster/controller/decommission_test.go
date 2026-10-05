package controller

// Decommission's completion half: once a draining node owns nothing, the
// controller removes it from the Raft voter set — but only when the two
// guards allow (keep at least MinVoters; never remove the current leader
// without transferring leadership first). And never while it still owns
// partitions (moves in flight).

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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

// A node staged as a Raft non-voter (it joined but was never promoted)
// and then decommissioned leaves the Raft configuration too, not just
// the member list. A non-voter carries no quorum weight and cannot lead,
// so the MinVoters floor and the leader hand-off do not apply to it.
func TestDecommissionRemovesADrainedNonvoterFromRaft(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c", "e")
	store.members[3].Draining = true // e is draining and owns nothing
	store.leaderID = "a"
	store.voters = []string{"a", "b", "c"} // at the MinVoters floor
	store.nonvoters = []string{"e"}
	store.topics = []topic.Topic{{Name: "orders", Partitions: 3}}
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()} // MinVoters 3

	c.reconcileDecommission(context.Background())

	if !slices.Equal(store.removed, []string{"e"}) {
		t.Fatalf("RemoveServer calls = %v, want [e] (the drained non-voter)", store.removed)
	}
	if !slices.Equal(store.forgotten, []string{"e"}) {
		t.Fatalf("RemoveMember calls = %v, want [e]", store.forgotten)
	}
	if store.transferred != 0 {
		t.Fatalf("TransferLeadership called %d times for a non-voter", store.transferred)
	}
	if !slices.Equal(store.voters, []string{"a", "b", "c"}) {
		t.Fatalf("voters = %v, want a, b and c untouched", store.voters)
	}
}

// fakeNodeStatus answers NodeStatus from a per-address script: each call
// pops the next answer, the last one repeating.
type fakeNodeStatus struct {
	mu      sync.Mutex
	answers map[string][]NodeStatusResult
	asked   []string
}

func (f *fakeNodeStatus) status(_ context.Context, addr string) (NodeStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, addr)
	q := f.answers[addr]
	if len(q) == 0 {
		return NodeStatus{}, errors.New("no answer scripted")
	}
	r := q[0]
	if len(q) > 1 {
		f.answers[addr] = q[1:]
	}
	return r.Status, r.Err
}

func withAddrs(store *fakeControllerStore) {
	for i := range store.members {
		store.members[i].Addr = store.members[i].ID + ":7942"
	}
}

// Five voters, two of them dead, and an alive drained voter: removing it
// would leave four voters with only two alive, a configuration that can
// never elect a leader again. Counting dead voters as healthy removed it.
func TestDecommissionKeepsAVoterWhoseRemovalLeavesNoHealthyMajority(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c", "d", "e")
	store.members[3].Status = metastore.MemberDead
	store.members[4].Status = metastore.MemberDead
	store.members[2].Draining = true // c: alive, drained
	store.leaderID = "a"
	store.topics = []topic.Topic{{Name: "orders", Partitions: 2}}
	store.assignments["orders"] = map[int]string{0: "a", 1: "b"}
	reg := prometheus.NewRegistry()
	c := &Controller{store: store, cfg: Config{}.withDefaults(), m: newMetrics(reg)}

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 0 || len(store.forgotten) != 0 {
		t.Fatalf("removed %v (forgotten %v); removing c leaves 2 of 4 voters alive", store.removed, store.forgotten)
	}
	if got := testutil.ToFloat64(c.m.decomBlocked.WithLabelValues("c", BlockedNoHealthyMajority)); got != 1 {
		t.Fatalf("narad_decommission_blocked{c,no_healthy_majority} = %v, want 1", got)
	}
}

func TestVoterRemovalRule(t *testing.T) {
	alive := func(ids ...string) []metastore.Member {
		var ms []metastore.Member
		for _, id := range ids {
			ms = append(ms, metastore.Member{ID: id, Status: metastore.MemberAlive})
		}
		return ms
	}
	dead := func(ms []metastore.Member, ids ...string) []metastore.Member {
		for i := range ms {
			if slices.Contains(ids, ms[i].ID) {
				ms[i].Status = metastore.MemberDead
			}
		}
		return ms
	}
	cases := []struct {
		name    string
		voters  []string
		members []metastore.Member
		id      string
		min     int
		want    string // "" allowed, else the Blocker code
	}{
		{"not a voter", []string{"a", "b", "c"}, alive("a", "b", "c", "x"), "x", 3, ""},
		{"four healthy voters", []string{"a", "b", "c", "d"}, alive("a", "b", "c", "d"), "d", 3, ""},
		{"floor", []string{"a", "b", "c"}, alive("a", "b", "c"), "c", 3, BlockedBelowMinVoters},
		{"default floor", []string{"a", "b", "c"}, alive("a", "b", "c"), "c", 0, BlockedBelowMinVoters},
		{"lower floor", []string{"a", "b", "c"}, alive("a", "b", "c"), "c", 2, ""},
		{"half the rest dead", []string{"a", "b", "c", "d", "e"}, dead(alive("a", "b", "c", "d", "e"), "d", "e"), "c", 3, BlockedNoHealthyMajority},
		{"dead voter goes", []string{"a", "b", "c", "d", "e"}, dead(alive("a", "b", "c", "d", "e"), "d", "e"), "e", 3, ""},
		{"voters without a record", []string{"a", "b", "g1", "g2", "d"}, alive("a", "b", "d"), "d", 3, BlockedNoHealthyMajority},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckVoterRemoval(tc.voters, tc.members, tc.id, tc.min)
			var b Blocker
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("CheckVoterRemoval = %v, want allowed", err)
			case tc.want != "" && (!errors.As(err, &b) || b.Code != tc.want):
				t.Fatalf("CheckVoterRemoval = %v, want %s", err, tc.want)
			}
		})
	}
}

// With a dead voter around, removing a dead draining voter first keeps
// the alive voters a majority, and then the alive draining voter can go
// too in the same pass. In ID order, the alive one would have been
// refused (or, before the health check, removed first).
func TestDecommissionRemovesDeadDrainingVotersFirst(t *testing.T) {
	store := newFakeControllerStore("a", "b", "c", "x", "y")
	store.members[2].Status = metastore.MemberDead // c: dead, not draining
	store.members[3].Draining = true               // x: alive, draining
	store.members[4].Draining = true               // y: dead, draining
	store.members[4].Status = metastore.MemberDead
	store.leaderID = "a"
	store.topics = []topic.Topic{{Name: "orders", Partitions: 2}}
	store.assignments["orders"] = map[int]string{0: "a", 1: "b"}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if !slices.Equal(store.removed, []string{"y", "x"}) {
		t.Fatalf("RemoveServer calls = %v, want [y x] (the dead draining voter first)", store.removed)
	}
}

// A draining node that is still the target of a move must not be
// removed: a flip landing after the removal would leave the partition on
// a node nothing can route to.
func TestDecommissionWaitsWhileTheNodeIsAMoveTarget(t *testing.T) {
	store := decomStore(t)
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	store.targets["orders"] = map[int]string{2: "d"} // a move still aims at d
	store.abortRefused = true                        // and cannot be cleared this pass
	reg := prometheus.NewRegistry()
	c := &Controller{store: store, cfg: Config{}.withDefaults(), m: newMetrics(reg)}

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 0 || len(store.forgotten) != 0 {
		t.Fatalf("removed a node that is still a move target: removed %v, forgotten %v", store.removed, store.forgotten)
	}
	if got := testutil.ToFloat64(c.m.decomBlocked.WithLabelValues("d", BlockedMoveTarget)); got != 1 {
		t.Fatalf("narad_decommission_blocked{d,move_target} = %v, want 1", got)
	}
}

// Moves aimed at a draining node never help its drain: the pass clears
// them with the AbortMove compare-and-set and then finishes the removal.
func TestDecommissionAbortsMovesAimedAtADrainingNode(t *testing.T) {
	store := decomStore(t)
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	store.targets["orders"] = map[int]string{2: "d", 1: "c"}
	log, logs := newLogBuffer()
	c := &Controller{store: store, cfg: Config{Logger: log}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if !slices.Equal(store.abortLog, []string{"orders/2→d"}) {
		t.Fatalf("AbortMove calls = %v, want only the move aimed at d", store.abortLog)
	}
	if store.targets["orders"][1] != "c" {
		t.Fatal("cleared a move aimed at a node that is not draining")
	}
	if !slices.Equal(store.removed, []string{"d"}) || !slices.Equal(store.forgotten, []string{"d"}) {
		t.Fatalf("removed %v, forgotten %v; want d removed once its inbound move was cleared", store.removed, store.forgotten)
	}
	if len(logs.lines("move target cleared", "target=d")) != 1 {
		t.Fatalf("the cleared move was not logged:\n%s", logs)
	}
}

// A drained node leaves Raft only once its ingress WAL has handed every
// accepted record to its owner: a removed node's replica freezes and it
// can never dispatch them after.
func TestDecommissionWaitsForTheDispatchBacklogToDrain(t *testing.T) {
	store := decomStore(t)
	withAddrs(store)
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	ns := &fakeNodeStatus{answers: map[string][]NodeStatusResult{
		"d:7942": {{Status: NodeStatus{Draining: true, DispatchBacklog: 5}}, {Status: NodeStatus{Draining: true}}},
	}}
	reg := prometheus.NewRegistry()
	c := &Controller{store: store, cfg: Config{NodeStatus: ns.status}.withDefaults(), m: newMetrics(reg)}

	c.reconcileDecommission(context.Background())
	if len(store.removed) != 0 {
		t.Fatalf("removed %v while its WAL still held 5 undispatched records", store.removed)
	}
	if got := testutil.ToFloat64(c.m.decomBlocked.WithLabelValues("d", BlockedDispatchBacklog)); got != 1 {
		t.Fatalf("narad_decommission_blocked{d,dispatch_backlog} = %v, want 1", got)
	}

	c.reconcileDecommission(context.Background())
	if !slices.Equal(store.removed, []string{"d"}) {
		t.Fatalf("removed = %v once the backlog reached 0, want [d]", store.removed)
	}
	if n := testutil.CollectAndCount(c.m.decomBlocked); n != 0 {
		t.Fatalf("%d narad_decommission_blocked series left after the removal, want 0", n)
	}
}

// A zero backlog read from a node that does not refuse client produce
// yet is not final: the drain reaches its replica some time after it
// commits, and a produce it accepts meanwhile lands in its WAL after the
// read. Nor is one read while it still answers produce it admitted
// before the drain. Only a node that reports the drain, nothing in
// flight and no backlog is removed.
func TestDecommissionWaitsUntilTheNodeRefusesProduceAndAnswersWhatItAdmitted(t *testing.T) {
	store := decomStore(t)
	withAddrs(store)
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	ns := &fakeNodeStatus{answers: map[string][]NodeStatusResult{"d:7942": {
		{Status: NodeStatus{Draining: false}},
		{Status: NodeStatus{Draining: true, ProduceInFlight: 2}},
		{Status: NodeStatus{Draining: true}},
	}}}
	log, logs := newLogBuffer()
	reg := prometheus.NewRegistry()
	c := &Controller{store: store, cfg: Config{NodeStatus: ns.status, Logger: log}.withDefaults(), m: newMetrics(reg)}

	c.reconcileDecommission(context.Background())
	if len(store.removed) != 0 || len(store.forgotten) != 0 {
		t.Fatalf("removed %v (forgotten %v) on a zero backlog read before the node refused produce", store.removed, store.forgotten)
	}
	if got := testutil.ToFloat64(c.m.decomBlocked.WithLabelValues("d", BlockedDispatchBacklog)); got != 1 {
		t.Fatalf("narad_decommission_blocked{d,dispatch_backlog} = %v, want 1", got)
	}
	if len(logs.lines("level=WARN", "decommission waiting", "node=d", "does not refuse client produce yet")) != 1 {
		t.Fatalf("no warn line saying d does not refuse produce yet:\n%s", logs)
	}

	c.reconcileDecommission(context.Background())
	if len(store.removed) != 0 {
		t.Fatalf("removed %v while it still answered 2 produce requests admitted before the drain", store.removed)
	}

	c.reconcileDecommission(context.Background())
	if !slices.Equal(store.removed, []string{"d"}) || !slices.Equal(store.forgotten, []string{"d"}) {
		t.Fatalf("removed %v, forgotten %v; want d once it refuses produce with nothing in flight and no backlog", store.removed, store.forgotten)
	}
}

// A 3.0.x node cannot report its backlog: it is removed as 3.0.x
// removed it, with a warning that the backlog was not checked.
func TestDecommissionOfAnOlderNodeProceedsWithAWarning(t *testing.T) {
	store := decomStore(t)
	withAddrs(store)
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	ns := &fakeNodeStatus{answers: map[string][]NodeStatusResult{"d:7942": {{Err: ErrNodeStatusUnsupported}}}}
	log, logs := newLogBuffer()
	c := &Controller{store: store, cfg: Config{NodeStatus: ns.status, Logger: log}.withDefaults()}

	c.reconcileDecommission(context.Background())

	if !slices.Equal(store.removed, []string{"d"}) {
		t.Fatalf("removed = %v, want [d]", store.removed)
	}
	if len(logs.lines("level=WARN", "cannot report its dispatch backlog", "node=d")) != 1 {
		t.Fatalf("no warning that d's backlog was not checked:\n%s", logs)
	}
}

// A drained node whose status cannot be read (unreachable, or dead and
// silent) waits, and says why.
func TestDecommissionWaitsWhenTheNodeStatusIsUnavailable(t *testing.T) {
	store := decomStore(t)
	withAddrs(store)
	store.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
	ns := &fakeNodeStatus{answers: map[string][]NodeStatusResult{"d:7942": {{Err: errors.New("dial: connection refused")}}}}
	reg := prometheus.NewRegistry()
	c := &Controller{store: store, cfg: Config{NodeStatus: ns.status}.withDefaults(), m: newMetrics(reg)}

	c.reconcileDecommission(context.Background())

	if len(store.removed) != 0 || len(store.forgotten) != 0 {
		t.Fatalf("removed %v (forgotten %v) without reading its backlog", store.removed, store.forgotten)
	}
	if got := testutil.ToFloat64(c.m.decomBlocked.WithLabelValues("d", BlockedNodeStatusUnavailable)); got != 1 {
		t.Fatalf("narad_decommission_blocked{d,node_status_unavailable} = %v, want 1", got)
	}
}

// Every stalled decommission says why, once per change: an error line
// for a stall that needs an operator, a warn line for a wait that clears
// on its own, and a series per reason.
func TestStalledDecommissionSaysWhy(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(f *fakeControllerStore)
		node   string
		reason string
		level  string
	}{
		{"below min voters", func(f *fakeControllerStore) {
			f.members, f.voters = f.members[:3], []string{"a", "b", "c"}
			f.members[2].Draining = true
			f.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "a"}
		}, "c", BlockedBelowMinVoters, "ERROR"},
		{"owner dead", func(f *fakeControllerStore) {
			f.members[3].Status = metastore.MemberDead
			f.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "d"}
		}, "d", BlockedOwnerDead, "ERROR"},
		{"no receivers", func(f *fakeControllerStore) {
			for i := range f.members {
				f.members[i].Draining = true
			}
			f.leaderID = "zz"
			f.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "d"}
		}, "d", BlockedNoReceivers, "ERROR"},
		{"move budget full", func(f *fakeControllerStore) {
			f.topics = append(f.topics, topic.Topic{Name: "busy", Partitions: 8})
			f.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "d"}
			f.assignments["busy"] = map[int]string{}
			f.targets["busy"] = map[int]string{}
			for p := range 8 {
				f.assignments["busy"][p] = "a"
				f.targets["busy"][p] = "b"
			}
		}, "d", BlockedMoveBudgetFull, "WARN"},
		{"leader transfer", func(f *fakeControllerStore) {
			f.leaderID = "d"
			f.assignments["orders"] = map[int]string{0: "a", 1: "b", 2: "c"}
		}, "d", BlockedLeaderTransfer, "WARN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := decomStore(t)
			tc.setup(store)
			log, logs := newLogBuffer()
			reg := prometheus.NewRegistry()
			c := &Controller{store: store, cfg: Config{Logger: log}.withDefaults(), m: newMetrics(reg)}

			for range 3 {
				c.reconcileDecommission(context.Background())
			}

			lines := logs.lines("decommission", "node="+tc.node, "reason="+tc.reason)
			if len(lines) != 1 {
				t.Fatalf("reason %s logged %d times for %s over 3 passes, want once:\n%s", tc.reason, len(lines), tc.node, logs)
			}
			if !strings.Contains(lines[0], "level="+tc.level) {
				t.Fatalf("reason %s logged as %s, want level %s", tc.reason, lines[0], tc.level)
			}
			if got := testutil.ToFloat64(c.m.decomBlocked.WithLabelValues(tc.node, tc.reason)); got != 1 {
				t.Fatalf("narad_decommission_blocked{%s,%s} = %v, want 1", tc.node, tc.reason, got)
			}
		})
	}
}
