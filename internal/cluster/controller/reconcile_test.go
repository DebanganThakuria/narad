package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type fakeControllerStore struct {
	members            []metastore.Member
	topics             []topic.Topic
	assignments        map[string]map[int]string // topic → partition → owner
	targets            map[string]map[int]string // topic → partition → target (in-flight)
	listAssignmentsErr map[string]error          // per-topic ListAssignments failure
	assignedLog        []string                  // "topic/partition→owner" in call order
	targetLog          []string                  // "topic/partition→target" in call order
	barrierErr         error
	voters             []string // nil ⇒ derive from members
	nonvoters          []string // Raft non-voters (staged joiners)
	removed            []string // RemoveServer calls in order
	forgotten          []string // RemoveMember calls in order
	transferred        int      // TransferLeadership call count
	leaderID           string
	membersVersion     uint64 // RoutingMembersVersion; bump when members change
	listMembersErr     error
	leaderBarrierErr   error
	leaderBarriers     int    // LeaderBarrier call count
	onLeaderBarrier    func() // what the FSM applies once a leader barriers
	onLockAssignments  func() // what lands between a sweep's list and its lock
	// entryTypesUsable makes the store answer the writes newer than
	// 3.0.x (insert-only placement, prune) as a cluster whose members
	// all apply them; false answers metastore.ErrEntryTypeNotYetUsable.
	entryTypesUsable bool
	orphans          []metastore.Assignment // OrphanAssignments
	prunes           int                    // PruneAssignment calls
	deadMarks        []string               // "id@observed" in call order
	// staleAssignments is what ListAssignments answers for a topic, once,
	// instead of the rows on record: a view that missed placements the
	// state machine has already applied.
	staleAssignments map[string][]metastore.Assignment
}

func newFakeControllerStore(memberIDs ...string) *fakeControllerStore {
	f := &fakeControllerStore{
		assignments:        map[string]map[int]string{},
		targets:            map[string]map[int]string{},
		listAssignmentsErr: map[string]error{},
	}
	for _, id := range memberIDs {
		f.members = append(f.members, metastore.Member{ID: id, Status: metastore.MemberAlive})
	}
	return f
}

func (f *fakeControllerStore) IsLeader() bool        { return true }
func (f *fakeControllerStore) LeaderCh() <-chan bool { return nil }
func (f *fakeControllerStore) Barrier() error        { return f.barrierErr }
func (f *fakeControllerStore) ListMembers() ([]metastore.Member, error) {
	if f.listMembersErr != nil {
		return nil, f.listMembersErr
	}
	return f.members, nil
}

func (f *fakeControllerStore) RoutingMembersVersion() uint64 { return f.membersVersion }

func (f *fakeControllerStore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return f.topics, "", nil
}

func (f *fakeControllerStore) ListAssignments(topicName string) ([]metastore.Assignment, error) {
	if err := f.listAssignmentsErr[topicName]; err != nil {
		return nil, err
	}
	if stale, ok := f.staleAssignments[topicName]; ok {
		delete(f.staleAssignments, topicName)
		return slices.Clone(stale), nil
	}
	var out []metastore.Assignment
	for p, owner := range f.assignments[topicName] {
		out = append(out, metastore.Assignment{
			Topic: topicName, Partition: p, OwnerID: owner, TargetID: f.targets[topicName][p],
		})
	}
	return out, nil
}

func (f *fakeControllerStore) LockAssignments() func() {
	if f.onLockAssignments != nil {
		f.onLockAssignments()
	}
	return func() {}
}

func (f *fakeControllerStore) LeaderBarrier(context.Context) error {
	f.leaderBarriers++
	if f.leaderBarrierErr != nil {
		return f.leaderBarrierErr
	}
	if f.onLeaderBarrier != nil {
		f.onLeaderBarrier()
		f.onLeaderBarrier = nil
	}
	return nil
}

func (f *fakeControllerStore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	for _, t := range f.topics {
		if t.Name == name {
			return t, nil
		}
	}
	return topic.Topic{}, metastore.ErrNotFound
}

func (f *fakeControllerStore) SetAssignmentTarget(_ context.Context, topicName string, partition int, targetID string) error {
	if f.targets[topicName] == nil {
		f.targets[topicName] = map[int]string{}
	}
	f.targets[topicName][partition] = targetID
	f.targetLog = append(f.targetLog, fmt.Sprintf("%s/%d→%s", topicName, partition, targetID))
	return nil
}

func (f *fakeControllerStore) AssignPartition(_ context.Context, topicName string, partition int, owner string) error {
	if f.assignments[topicName] == nil {
		f.assignments[topicName] = map[int]string{}
	}
	f.assignments[topicName][partition] = owner
	f.assignedLog = append(f.assignedLog, fmt.Sprintf("%s/%d→%s", topicName, partition, owner))
	return nil
}

func (f *fakeControllerStore) AssignPartitionIfAbsent(ctx context.Context, topicName string, partition int, owner, _ string) error {
	if !f.entryTypesUsable {
		return metastore.ErrEntryTypeNotYetUsable
	}
	if _, ok := f.assignments[topicName][partition]; ok {
		return metastore.ErrPartitionAssigned
	}
	return f.AssignPartition(ctx, topicName, partition, owner)
}

func (f *fakeControllerStore) OrphanAssignments() ([]metastore.Assignment, error) {
	return slices.Clone(f.orphans), nil
}

func (f *fakeControllerStore) PruneAssignment(_ context.Context, topicName string, partition int) error {
	f.prunes++
	if !f.entryTypesUsable {
		return metastore.ErrEntryTypeNotYetUsable
	}
	before := len(f.orphans)
	f.orphans = slices.DeleteFunc(f.orphans, func(a metastore.Assignment) bool {
		return a.Topic == topicName && a.Partition == partition
	})
	if len(f.orphans) == before {
		return metastore.ErrNotFound
	}
	return nil
}

func (f *fakeControllerStore) MarkMemberDeadObserved(_ context.Context, id string, observed int64) error {
	f.deadMarks = append(f.deadMarks, fmt.Sprintf("%s@%d", id, observed))
	return nil
}

func (f *fakeControllerStore) Voters() ([]string, error) {
	if f.voters == nil {
		var ids []string
		for _, m := range f.members {
			ids = append(ids, m.ID)
		}
		return ids, nil
	}
	return f.voters, nil
}

func (f *fakeControllerStore) Nonvoters() ([]string, error) {
	return f.nonvoters, nil
}

func (f *fakeControllerStore) RemoveMember(_ context.Context, id string, _ int64) error {
	f.forgotten = append(f.forgotten, id)
	f.members = slices.DeleteFunc(f.members, func(m metastore.Member) bool { return m.ID == id })
	return nil
}

func (f *fakeControllerStore) RemoveServer(id string) error {
	f.removed = append(f.removed, id)
	f.nonvoters = slices.DeleteFunc(f.nonvoters, func(n string) bool { return n == id })
	// Reflect the removal so a follow-up Voters() no longer lists it.
	if f.voters == nil {
		for _, m := range f.members {
			if m.ID != id {
				f.voters = append(f.voters, m.ID)
			}
		}
	} else {
		out := f.voters[:0:0]
		for _, v := range f.voters {
			if v != id {
				out = append(out, v)
			}
		}
		f.voters = out
	}
	return nil
}

func (f *fakeControllerStore) TransferLeadership() error {
	f.transferred++
	return nil
}

func (f *fakeControllerStore) LeaderID() string { return f.leaderID }

// A transient ListAssignments failure must not make every partition look
// unassigned: without replication, reassigning an already-owned partition
// would round-robin it onto a member that does not hold its data.
func TestReconcileSkipsTopicWhenListAssignmentsFails(t *testing.T) {
	store := newFakeControllerStore("narad-0")
	store.topics = []topic.Topic{{Name: "orders", Partitions: 2}}
	store.assignments["orders"] = map[int]string{0: "narad-0"}
	store.listAssignmentsErr["orders"] = errors.New("transient read failure")
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if len(store.assignedLog) != 0 {
		t.Fatalf("AssignPartition called (%v) during read failure, want none", store.assignedLog)
	}

	// Once the read recovers, only the genuinely unassigned partition is
	// assigned; the existing assignment stays put.
	delete(store.listAssignmentsErr, "orders")
	c.reconcileAssignments(context.Background())
	if len(store.assignedLog) != 1 || store.assignedLog[0] != "orders/1→narad-0" {
		t.Fatalf("assignments after recovery = %v, want only orders/1", store.assignedLog)
	}
	if store.assignments["orders"][0] != "narad-0" {
		t.Fatalf("existing assignment moved to %q", store.assignments["orders"][0])
	}
}

// The reconcile sweep must place a fan-out child's partitions on
// different members than the parent's same-index partitions — the
// replica pattern's whole point — and must handle the alphabetical trap
// where ListTopics yields the child before its parent, within a single
// pass.
func TestReconcileAssignsChildAntiAffineEvenWhenChildSortsFirst(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	// "a-replica" < "orders": the child comes back first from ListTopics.
	store.topics = []topic.Topic{
		{Name: "a-replica", Partitions: 6, Parent: "orders", Role: topic.RoleChild},
		{Name: "orders", Partitions: 6, Role: topic.RoleParent, Children: []string{"a-replica"}},
	}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())

	parent := store.assignments["orders"]
	child := store.assignments["a-replica"]
	if len(parent) != 6 || len(child) != 6 {
		t.Fatalf("one pass assigned %d parent / %d child partitions, want 6/6 (parents must sort first)", len(parent), len(child))
	}
	for p := range 6 {
		if parent[p] == child[p] {
			t.Fatalf("partition %d: parent and child both on %q — copies share a disk", p, parent[p])
		}
	}
}

// A child whose parent's assignments cannot be read must be deferred —
// assigning blind could colocate the copies.
func TestReconcileDefersChildWhenParentAssignmentsUnreadable(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1")
	store.topics = []topic.Topic{
		{Name: "orders", Partitions: 2, Role: topic.RoleParent, Children: []string{"replica"}},
		{Name: "replica", Partitions: 2, Parent: "orders", Role: topic.RoleChild},
	}
	store.listAssignmentsErr["orders"] = errors.New("transient read failure")
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if got := store.assignments["replica"]; len(got) != 0 {
		t.Fatalf("child assigned %v while its parent's owners were unreadable", got)
	}

	delete(store.listAssignmentsErr, "orders")
	c.reconcileAssignments(context.Background())
	parent, child := store.assignments["orders"], store.assignments["replica"]
	if len(parent) != 2 || len(child) != 2 {
		t.Fatalf("after recovery: %d parent / %d child assignments, want 2/2", len(parent), len(child))
	}
	for p := range 2 {
		if parent[p] == child[p] {
			t.Fatalf("partition %d colocated on %q", p, parent[p])
		}
	}
}

// With a single live member a child still gets assigned (colocated —
// there is nowhere else) rather than deferring forever.
func TestReconcileSingleMemberAssignsChildColocated(t *testing.T) {
	store := newFakeControllerStore("narad-0")
	store.topics = []topic.Topic{
		{Name: "orders", Partitions: 2, Role: topic.RoleParent, Children: []string{"replica"}},
		{Name: "replica", Partitions: 2, Parent: "orders", Role: topic.RoleChild},
	}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if len(store.assignments["replica"]) != 2 {
		t.Fatalf("child assignments = %v, want both partitions placed on the only member", store.assignments["replica"])
	}
}

// Child partitions beyond the parent's range have no counterpart to
// avoid and assign unconstrained; the overlapping range stays
// anti-affine.
func TestReconcileChildWiderThanParent(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	store.topics = []topic.Topic{
		{Name: "orders", Partitions: 2, Role: topic.RoleParent, Children: []string{"replica"}},
		{Name: "replica", Partitions: 4, Parent: "orders", Role: topic.RoleChild},
	}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	parent, child := store.assignments["orders"], store.assignments["replica"]
	if len(child) != 4 {
		t.Fatalf("child assignments = %v, want all 4 placed", child)
	}
	for p := range 2 {
		if parent[p] == child[p] {
			t.Fatalf("overlapping partition %d colocated on %q", p, parent[p])
		}
	}
}

// A just-elected leader's FSM may not have applied the placements the
// previous leader committed, so the sweep must barrier before it reads
// assignments: on master the sweep saw orders/0 unassigned and replaced
// the owner the old leader had placed, whose disk may already hold
// records. A failed barrier skips the pass.
func TestAssignSweepBarriersBeforeReading(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	store.topics = []topic.Topic{{Name: "orders", ID: "0000000000000001", Partitions: 3}}
	store.onLeaderBarrier = func() {
		store.assignments["orders"] = map[int]string{0: "narad-2", 1: "narad-0", 2: "narad-1"}
	}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if len(store.assignedLog) != 0 {
		t.Fatalf("assigned %v, want none: the previous leader already placed every partition", store.assignedLog)
	}
	if got := store.assignments["orders"][0]; got != "narad-2" {
		t.Fatalf("orders/0 owner = %q, want narad-2 (the previous leader's placement)", got)
	}

	failing := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	failing.topics = []topic.Topic{{Name: "orders", ID: "0000000000000001", Partitions: 3}}
	failing.leaderBarrierErr = errors.New("barrier timed out")
	c = &Controller{store: failing, cfg: Config{}.withDefaults()}
	c.reconcileAssignments(context.Background())
	if failing.leaderBarriers == 0 || len(failing.assignedLog) != 0 {
		t.Fatalf("barriers = %d, assigned %v after a failed barrier, want the pass skipped", failing.leaderBarriers, failing.assignedLog)
	}
}

// Once every member applies insert-only placement, a sweep whose view
// shows a partition without an owner, while the state machine already
// holds one, leaves that owner in place (its disk may hold records) and
// still places the partitions that really have none. A plain assignment
// here replaced orders/0's owner with the round-robin pick.
func TestAssignSweepNeverReplacesAnOwnerItDidNotSee(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	store.entryTypesUsable = true
	store.topics = []topic.Topic{{Name: "orders", ID: "0000000000000001", Partitions: 3}}
	store.assignments["orders"] = map[int]string{0: "narad-2"}
	store.staleAssignments = map[string][]metastore.Assignment{"orders": nil}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if got := store.assignments["orders"][0]; got != "narad-2" {
		t.Fatalf("orders/0 owner = %q, want narad-2, the owner on record (assigned %v)", got, store.assignedLog)
	}
	if got := store.assignments["orders"]; len(got) != 3 || got[1] == "" || got[2] == "" {
		t.Fatalf("orders assignments = %v, want partitions 1 and 2 placed too", got)
	}
	if slices.ContainsFunc(store.assignedLog, func(s string) bool { return strings.HasPrefix(s, "orders/0") }) {
		t.Fatalf("assigned %v, want no write for orders/0", store.assignedLog)
	}
}

// The sweep places new partitions only on members that are not being
// decommissioned (master placed them round-robin over every live
// member, draining ones included).
func TestAssignSweepSkipsDrainingMembers(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	store.members[1].Draining = true
	store.topics = []topic.Topic{{Name: "orders", ID: "0000000000000001", Partitions: 6}}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if len(store.assignments["orders"]) != 6 {
		t.Fatalf("assigned %v, want all 6 partitions placed", store.assignedLog)
	}
	for p, owner := range store.assignments["orders"] {
		if owner == "narad-1" {
			t.Fatalf("orders/%d placed on the draining member (all: %v)", p, store.assignedLog)
		}
	}
}

// With every live member draining the sweep places nothing: a draining
// member is never given new partitions, and the decommission would have
// nowhere to move them. The partitions wait, and go to the first member
// that is not draining once one is alive.
func TestAssignSweepLeavesPartitionsUnassignedWhenAllDraining(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1")
	store.members[0].Draining = true
	store.members[1].Draining = true
	store.topics = []topic.Topic{{Name: "orders", ID: "0000000000000001", Partitions: 3}}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if len(store.assignedLog) != 0 {
		t.Fatalf("assigned %v with every live member draining, want none", store.assignedLog)
	}

	store.members[1].Draining = false
	c.reconcileAssignments(context.Background())
	if len(store.assignments["orders"]) != 3 {
		t.Fatalf("assigned %v once narad-1 stopped draining, want all 3 partitions placed", store.assignedLog)
	}
	for p, owner := range store.assignments["orders"] {
		if owner != "narad-1" {
			t.Fatalf("orders/%d placed on %q, want narad-1, the only member not draining", p, owner)
		}
	}
}

// A topic deleted between the sweep's topic list and its assignment
// lock must be skipped (master wrote assignment rows for the deleted
// topic, which a later same-named topic inherited).
func TestAssignSweepSkipsTopicDeletedAfterList(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	store.topics = []topic.Topic{{Name: "orders", ID: "0000000000000001", Partitions: 3}}
	store.onLockAssignments = func() { store.topics = nil }
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if len(store.assignedLog) != 0 {
		t.Fatalf("assigned %v for a topic deleted after the list, want none", store.assignedLog)
	}

	// Recreated under the same name in between: the listed incarnation
	// is gone, so the sweep leaves the new one to the next pass.
	store.topics = []topic.Topic{{Name: "orders", ID: "0000000000000001", Partitions: 3}}
	store.onLockAssignments = func() {
		store.topics = []topic.Topic{{Name: "orders", ID: "0000000000000002", Partitions: 3}}
	}
	c.reconcileAssignments(context.Background())
	if len(store.assignedLog) != 0 {
		t.Fatalf("assigned %v for the listed incarnation after a recreate, want none", store.assignedLog)
	}
}

// The sweep places partitions by the counts it reads under the
// assignment lock, not the ones it listed: here the parent was
// recreated with 6 partitions after the list showed 3, and the child's
// partitions 3 to 5 must still avoid the parent's same-index owners
// instead of being placed as if the parent had no partition there.
func TestAssignSweepUsesRecreatedPartitionCount(t *testing.T) {
	store := newFakeControllerStore("narad-0", "narad-1", "narad-2")
	store.topics = []topic.Topic{
		{Name: "orders", ID: "0000000000000001", Partitions: 3, Role: topic.RoleParent, Children: []string{"replica"}},
		{Name: "replica", ID: "0000000000000002", Partitions: 6, Parent: "orders", Role: topic.RoleChild},
	}
	// The parent's six owners are already on record; the round-robin
	// owner of each child partition is the parent's same-index owner.
	store.assignments["orders"] = map[int]string{0: "narad-0", 1: "narad-1", 2: "narad-2", 3: "narad-0", 4: "narad-1", 5: "narad-2"}
	store.onLockAssignments = func() {
		store.topics[0] = topic.Topic{Name: "orders", ID: "0000000000000003", Partitions: 6, Role: topic.RoleParent, Children: []string{"replica"}}
	}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	child := store.assignments["replica"]
	if len(child) != 6 {
		t.Fatalf("child assignments = %v, want all 6 placed", child)
	}
	for p := range 6 {
		if child[p] == store.assignments["orders"][p] {
			t.Fatalf("replica/%d colocated with orders/%d on %q: the sweep used the listed parent count", p, p, child[p])
		}
	}
}
