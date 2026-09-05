package topics

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

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

	// No partition log is opened by a describe, owned or not (no
	// flusher or reaper goroutines, no empty partition dirs, and no
	// lastAccess stamp that would keep an idle log warm).
	for _, p := range []int{0, 1, 2} {
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
	// Owning every partition (the single-node case) reports every
	// partition, still without opening any log.
	for i := range 3 {
		if details.Partitions[i].Index != i {
			t.Fatalf("partition stats[%d].Index = %d", i, details.Partitions[i].Index)
		}
		if _, ok := manager.logs.Peek(testTopicName, i); ok {
			t.Fatalf("GetTopicDetails() opened the log for owned partition %d", i)
		}
	}
}

// Audit finding 1.7: a describe must report a closed (idle-evicted or
// never-reopened) partition's real stats from its directory, without
// opening the log.
func TestGetTopicDetails_DescribesClosedLogWithoutOpening(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 2}
	manager := newTestManager(t, ms, nil)
	t.Cleanup(func() { _ = manager.logs.CloseAll() })

	l, err := manager.logs.Get(testTopicName, 1)
	if err != nil {
		t.Fatalf("logs.Get: %v", err)
	}
	for i := range 7 {
		if _, err := l.Append([]byte{byte(i)}); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := l.AdvanceHighWatermark(7); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	wantSize := l.SizeBytes()
	wantAt, _ := l.OldestSegmentAt()

	// Live counters while open.
	details, err := manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails() error = %v", err)
	}
	if ps := details.Partitions[1]; ps.HighWatermark != 7 || ps.NextOffset != 7 || ps.Segments != 1 || ps.SizeBytes != wantSize {
		t.Fatalf("open partition stats = %+v, want hwm=7 next=7 segments=1 size=%d", ps, wantSize)
	}

	// Closed (as idle eviction leaves it): identical numbers from disk,
	// and the log stays closed.
	if err := manager.logs.CloseTopic(testTopicName); err != nil {
		t.Fatalf("CloseTopic: %v", err)
	}
	details, err = manager.GetTopicDetails(context.Background(), testTopicName)
	if err != nil {
		t.Fatalf("GetTopicDetails() after close error = %v", err)
	}
	ps := details.Partitions[1]
	if ps.HighWatermark != 7 || ps.NextOffset != 7 || ps.Segments != 1 || ps.OldestOffset != 0 ||
		ps.SizeBytes != wantSize || ps.OldestSegmentAt != wantAt {
		t.Fatalf("closed partition stats = %+v, want hwm=7 next=7 segments=1 oldest=0 size=%d at=%d", ps, wantSize, wantAt)
	}
	if empty := details.Partitions[0]; empty.HighWatermark != 0 || empty.Segments != 0 {
		t.Fatalf("never-written partition stats = %+v, want zero", empty)
	}
	for i := range 2 {
		if _, ok := manager.logs.Peek(testTopicName, i); ok {
			t.Fatalf("GetTopicDetails() reopened the closed log for partition %d", i)
		}
	}
}

// Audit finding 1.7: a monitoring loop that GETs a topic must not keep
// its idle logs warm. Only Get stamps lastAccess; a describe reads
// through Peek, so the log stays evictable.
func TestGetTopicDetails_DoesNotKeepIdleLogWarm(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics[testTopicName] = topic.Topic{Name: testTopicName, Partitions: 1} // keep-forever: evictable regardless of segments
	manager := newTestManager(t, ms, nil)
	t.Cleanup(func() { _ = manager.logs.CloseAll() })

	if _, err := manager.logs.Get(testTopicName, 0); err != nil {
		t.Fatalf("logs.Get: %v", err)
	}
	const idle = 30 * time.Millisecond
	time.Sleep(2 * idle)

	// A describe of a warm log reads its live counters...
	if _, err := manager.GetTopicDetails(context.Background(), testTopicName); err != nil {
		t.Fatalf("GetTopicDetails() error = %v", err)
	}
	// ...and must not have refreshed its idle clock.
	if evicted := manager.logs.EvictIdleOnce(idle); evicted != 1 {
		t.Fatalf("EvictIdleOnce() evicted %d logs after a describe, want 1 (the describe kept the log warm)", evicted)
	}
	if _, ok := manager.logs.Peek(testTopicName, 0); ok {
		t.Fatal("log still open after eviction")
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
	for i := range 3 {
		if details.Partitions[i].Index != i {
			t.Fatalf("partition stats[%d].Index = %d", i, details.Partitions[i].Index)
		}
		if _, ok := manager.logs.Peek(testTopicName, i); ok {
			t.Fatalf("GetTopicDetails() opened the log for partition %d", i)
		}
	}
}
