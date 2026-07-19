package controller

import (
	"context"
	"errors"
	"fmt"
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
	return f.members, nil
}

func (f *fakeControllerStore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return f.topics, "", nil
}

func (f *fakeControllerStore) ListAssignments(topicName string) ([]metastore.Assignment, error) {
	if err := f.listAssignmentsErr[topicName]; err != nil {
		return nil, err
	}
	var out []metastore.Assignment
	for p, owner := range f.assignments[topicName] {
		out = append(out, metastore.Assignment{
			Topic: topicName, Partition: p, OwnerID: owner, TargetID: f.targets[topicName][p],
		})
	}
	return out, nil
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

func (f *fakeControllerStore) MarkMemberDead(context.Context, string) error { return nil }

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
	for p := 0; p < 6; p++ {
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
	for p := 0; p < 2; p++ {
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
	for p := 0; p < 2; p++ {
		if parent[p] == child[p] {
			t.Fatalf("overlapping partition %d colocated on %q", p, parent[p])
		}
	}
}
