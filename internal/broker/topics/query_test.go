package topics

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// assignmentFakeMetastore layers partition-assignment lookups on top of
// fakeMetastore so ownership-aware code paths (GetTopicDetails) can be
// exercised. owners maps partition index to owner node ID; partitions
// absent from the map have no assignment.
type assignmentFakeMetastore struct {
	*fakeMetastore
	owners map[int]string
	// assignmentErr, when set, is returned from every GetAssignment
	// call to model non-NotFound lookup failures (db closed, corrupt
	// record).
	assignmentErr error
}

func (f *assignmentFakeMetastore) GetAssignment(topicName string, partition int) (metastore.Assignment, error) {
	if f.assignmentErr != nil {
		return metastore.Assignment{}, f.assignmentErr
	}
	owner, ok := f.owners[partition]
	if !ok {
		return metastore.Assignment{}, errs.ErrNotFound
	}
	return metastore.Assignment{Topic: topicName, Partition: partition, OwnerID: owner}, nil
}

func TestGetTopicDetails_DoesNotOpenUnownedPartitionLogs(t *testing.T) {
	const selfID = "node-a"
	ms := &assignmentFakeMetastore{
		fakeMetastore: newFakeMetastore(),
		// Partition 0 is ours, partition 1 belongs to a peer, and
		// partition 2 has no assignment at all.
		owners: map[int]string{0: selfID, 1: "node-b"},
	}
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	manager := newTestManagerForMetastore(t, ms, nil, nil, selfID)
	t.Cleanup(func() { _ = manager.logs.CloseAll() })

	details, err := manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails() error = %v", err)
	}

	// The slice must stay positional and complete: the HTTP ?partition=
	// path and the cluster stats RPC handler index it by partition.
	if len(details.Partitions) != 3 {
		t.Fatalf("GetTopicDetails() partitions = %d, want 3", len(details.Partitions))
	}
	for i, ps := range details.Partitions {
		if ps.Index != i {
			t.Fatalf("partition stats[%d].Index = %d, want %d", i, ps.Index, i)
		}
	}

	// The owned partition was lazily opened as before.
	if _, ok := manager.logs.Peek(testTopicName, 0); !ok {
		t.Fatal("GetTopicDetails() did not open the log for owned partition 0")
	}

	// Unowned / unassigned partitions must not be opened (no flusher or
	// reaper goroutines, no empty partition dirs) and report zero stats.
	for _, p := range []int{1, 2} {
		if _, ok := manager.logs.Peek(testTopicName, p); ok {
			t.Fatalf("GetTopicDetails() opened a log for partition %d not owned by this node", p)
		}
		dir := storage.TopicPartitionDir(manager.dataDir, testTopicName, p)
		if _, statErr := os.Stat(dir); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("partition %d dir stat error = %v, want not exists", p, statErr)
		}
		ps := details.Partitions[p]
		if ps.Segments != 0 || ps.OldestOffset != 0 || ps.NextOffset != 0 ||
			ps.HighWatermark != 0 || ps.SizeBytes != 0 || ps.OldestSegmentAt != 0 {
			t.Fatalf("unowned partition %d stats = %+v, want zero values", p, ps)
		}
	}
}

func TestGetTopicDetails_PropagatesAssignmentLookupErrors(t *testing.T) {
	// Only ErrNotFound means "unowned". Any other GetAssignment failure
	// must propagate: coercing it to unowned would silently report
	// zero-valued stats for partitions this node actually owns.
	const selfID = "node-a"
	lookupErr := errors.New("get assignment: db closed")
	ms := &assignmentFakeMetastore{
		fakeMetastore: newFakeMetastore(),
		owners:        map[int]string{0: selfID},
		assignmentErr: lookupErr,
	}
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 1}
	manager := newTestManagerForMetastore(t, ms, nil, nil, selfID)
	t.Cleanup(func() { _ = manager.logs.CloseAll() })

	_, err := manager.GetTopicDetails(context.Background(), testTopicName)
	if !errors.Is(err, lookupErr) {
		t.Fatalf("GetTopicDetails() error = %v, want %v", err, lookupErr)
	}
}

func TestGetTopicDetails_SelfOwnedPartitionsReportFullStats(t *testing.T) {
	const selfID = "node-a"
	ms := &assignmentFakeMetastore{
		fakeMetastore: newFakeMetastore(),
		owners:        map[int]string{0: selfID, 1: selfID, 2: selfID},
	}
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	manager := newTestManagerForMetastore(t, ms, nil, nil, selfID)
	t.Cleanup(func() { _ = manager.logs.CloseAll() })

	details, err := manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails() error = %v", err)
	}
	if len(details.Partitions) != 3 {
		t.Fatalf("GetTopicDetails() partitions = %d, want 3", len(details.Partitions))
	}
	// Owning every partition (the single-node case) must still open and
	// report every partition log, as before the owner-only change.
	for i := 0; i < 3; i++ {
		if _, ok := manager.logs.Peek(testTopicName, i); !ok {
			t.Fatalf("GetTopicDetails() did not open the log for owned partition %d", i)
		}
	}
}

func TestGetTopicDetails_NoClusterIdentityOwnsEverything(t *testing.T) {
	// Without a selfID (tests / embedded use) the manager keeps the old
	// behavior: every partition is treated as locally owned.
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 3}
	manager := newTestManager(t, ms, nil)
	t.Cleanup(func() { _ = manager.logs.CloseAll() })

	details, err := manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails() error = %v", err)
	}
	if len(details.Partitions) != 3 {
		t.Fatalf("GetTopicDetails() partitions = %d, want 3", len(details.Partitions))
	}
	for i := 0; i < 3; i++ {
		if _, ok := manager.logs.Peek(testTopicName, i); !ok {
			t.Fatalf("GetTopicDetails() did not open the log for partition %d", i)
		}
	}
}
