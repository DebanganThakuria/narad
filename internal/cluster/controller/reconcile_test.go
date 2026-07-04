package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

type fakeControllerStore struct {
	assignments        []metastore.Assignment
	listAssignmentsErr error
	assigned           []int
}

func (f *fakeControllerStore) IsLeader() bool        { return true }
func (f *fakeControllerStore) LeaderCh() <-chan bool { return nil }
func (f *fakeControllerStore) ListMembers() ([]metastore.Member, error) {
	return []metastore.Member{{ID: "narad-0", Status: metastore.MemberAlive}}, nil
}

func (f *fakeControllerStore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return []topic.Topic{{Name: "orders", Partitions: 2}}, "", nil
}

func (f *fakeControllerStore) ListAssignments(string) ([]metastore.Assignment, error) {
	if f.listAssignmentsErr != nil {
		return nil, f.listAssignmentsErr
	}
	return f.assignments, nil
}

func (f *fakeControllerStore) AssignPartition(_ context.Context, _ string, partition int, _ string) error {
	f.assigned = append(f.assigned, partition)
	return nil
}

func (f *fakeControllerStore) MarkMemberDead(context.Context, string) error { return nil }

// A transient ListAssignments failure must not make every partition look
// unassigned: without replication, reassigning an already-owned partition
// would round-robin it onto a member that does not hold its data.
func TestReconcileSkipsTopicWhenListAssignmentsFails(t *testing.T) {
	store := &fakeControllerStore{
		assignments:        []metastore.Assignment{{Topic: "orders", Partition: 0, OwnerID: "narad-0"}},
		listAssignmentsErr: errors.New("transient read failure"),
	}
	c := &Controller{store: store, cfg: Config{}.withDefaults()}

	c.reconcileAssignments(context.Background())
	if len(store.assigned) != 0 {
		t.Fatalf("AssignPartition called for partitions %v during read failure, want none", store.assigned)
	}

	// Once the read recovers, only the genuinely unassigned partition is
	// assigned; the existing assignment stays put.
	store.listAssignmentsErr = nil
	c.reconcileAssignments(context.Background())
	if len(store.assigned) != 1 || store.assigned[0] != 1 {
		t.Fatalf("assigned partitions = %v after recovery, want [1]", store.assigned)
	}
}
