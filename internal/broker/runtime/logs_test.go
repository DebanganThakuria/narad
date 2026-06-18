package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

type runtimeFakeMetastore struct {
	topics        map[string]topic.Topic
	assignments   map[string]metastore.Assignment
	getTopicErr   error
	listTopicsErr error
}

func runtimeAssignmentKey(topicName string, partition int) string {
	return fmt.Sprintf("%s/%d", topicName, partition)
}

func (f *runtimeFakeMetastore) GetAssignment(topicName string, partition int) (metastore.Assignment, error) {
	assignment, ok := f.assignments[runtimeAssignmentKey(topicName, partition)]
	if !ok {
		return metastore.Assignment{}, errs.ErrNotFound
	}
	return assignment, nil
}

func (f *runtimeFakeMetastore) setAssignment(topicName string, partition int, ownerID string) {
	if f.assignments == nil {
		f.assignments = map[string]metastore.Assignment{}
	}
	f.assignments[runtimeAssignmentKey(topicName, partition)] = metastore.Assignment{
		Topic:     topicName,
		Partition: partition,
		OwnerID:   ownerID,
	}
}

func newRuntimeFakeMetastore() *runtimeFakeMetastore {
	return &runtimeFakeMetastore{topics: map[string]topic.Topic{}, assignments: map[string]metastore.Assignment{}}
}

func (f *runtimeFakeMetastore) CreateTopic(_ context.Context, t topic.Topic) error {
	f.topics[t.Name] = t
	return nil
}

func (f *runtimeFakeMetastore) UpdateTopic(_ context.Context, t topic.Topic) error {
	f.topics[t.Name] = t
	return nil
}

func (f *runtimeFakeMetastore) DeleteTopic(_ context.Context, name string) error {
	delete(f.topics, name)
	return nil
}

func (f *runtimeFakeMetastore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	if f.getTopicErr != nil {
		return topic.Topic{}, f.getTopicErr
	}
	t, ok := f.topics[name]
	if !ok {
		return topic.Topic{}, errs.ErrNotFound
	}
	return t, nil
}

func (f *runtimeFakeMetastore) ListTopics(_ context.Context, _ metastore.ListOptions) ([]topic.Topic, string, error) {
	if f.listTopicsErr != nil {
		return nil, "", f.listTopicsErr
	}
	out := make([]topic.Topic, 0, len(f.topics))
	for _, t := range f.topics {
		out = append(out, t)
	}
	return out, "", nil
}

func (f *runtimeFakeMetastore) PutSchema(_ context.Context, _ string, _ int, _ []byte) error {
	return nil
}

func (f *runtimeFakeMetastore) GetSchema(_ context.Context, _ string, _ int) ([]byte, error) {
	return nil, errs.ErrNotFound
}

func (f *runtimeFakeMetastore) LeaderAddr() string { return "" }

func (f *runtimeFakeMetastore) GetMember(string) (metastore.Member, error) {
	return metastore.Member{}, errs.ErrNotFound
}

func (f *runtimeFakeMetastore) Close() error { return nil }

type runtimeMetastoreWithoutAssignments struct {
	inner *runtimeFakeMetastore
}

func (m runtimeMetastoreWithoutAssignments) CreateTopic(ctx context.Context, t topic.Topic) error {
	return m.inner.CreateTopic(ctx, t)
}

func (m runtimeMetastoreWithoutAssignments) UpdateTopic(ctx context.Context, t topic.Topic) error {
	return m.inner.UpdateTopic(ctx, t)
}

func (m runtimeMetastoreWithoutAssignments) DeleteTopic(ctx context.Context, name string) error {
	return m.inner.DeleteTopic(ctx, name)
}

func (m runtimeMetastoreWithoutAssignments) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	return m.inner.GetTopic(ctx, name)
}

func (m runtimeMetastoreWithoutAssignments) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	return m.inner.ListTopics(ctx, opts)
}

func (m runtimeMetastoreWithoutAssignments) PutSchema(ctx context.Context, topicName string, version int, raw []byte) error {
	return m.inner.PutSchema(ctx, topicName, version, raw)
}

func (m runtimeMetastoreWithoutAssignments) GetSchema(ctx context.Context, topicName string, version int) ([]byte, error) {
	return m.inner.GetSchema(ctx, topicName, version)
}

func (m runtimeMetastoreWithoutAssignments) LeaderAddr() string {
	return m.inner.LeaderAddr()
}

func (m runtimeMetastoreWithoutAssignments) GetMember(id string) (metastore.Member, error) {
	return m.inner.GetMember(id)
}

func (m runtimeMetastoreWithoutAssignments) Close() error {
	return m.inner.Close()
}

func newRuntimeTestLogs(t *testing.T, ms metastore.Metastore) *Logs {
	t.Helper()
	return NewLogs(t.TempDir(), storage.Options{
		FlushInterval: 5 * time.Millisecond,
		Retention: storage.RetentionConfig{
			CheckInterval: time.Second,
		},
	}, ms, nil)
}

func TestKeyOf(t *testing.T) {
	if got := keyOf("orders", 3); got != "orders/3" {
		t.Fatalf("keyOf() = %q, want %q", got, "orders/3")
	}
}

func TestRetentionFromTopic(t *testing.T) {
	cfg := retentionFromTopic(2500, 3*time.Second)
	if cfg.MaxAge != 2500*time.Millisecond {
		t.Fatalf("retentionFromTopic() MaxAge = %v, want %v", cfg.MaxAge, 2500*time.Millisecond)
	}
	if cfg.CheckInterval != 3*time.Second {
		t.Fatalf("retentionFromTopic() CheckInterval = %v, want %v", cfg.CheckInterval, 3*time.Second)
	}
}

func TestLogsGetCachesLogInstance(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)

	first, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() first error = %v", err)
	}
	second, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() second error = %v", err)
	}
	if first != second {
		t.Fatal("Get() returned different log instances for same topic/partition")
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestLogsGetIgnoresMissingTopicRetentionLookup(t *testing.T) {
	logs := newRuntimeTestLogs(t, newRuntimeFakeMetastore())

	l, err := logs.Get("missing", 1)
	if err != nil {
		t.Fatalf("Get() error = %v, want nil", err)
	}
	if l == nil {
		t.Fatal("Get() log = nil, want opened log")
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestLogsGetReturnsMetastoreLookupError(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.getTopicErr = errors.New("boom")
	logs := newRuntimeTestLogs(t, ms)

	_, err := logs.Get("orders", 0)
	if err == nil || err.Error() != "broker/runtime: lookup topic for retention: boom" {
		t.Fatalf("Get() error = %v, want wrapped metastore error", err)
	}
}

func TestLogsCloseTopicRemovesOnlyMatchingEntries(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 1000}
	ms.topics["payments"] = topic.Topic{Name: "payments", RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)

	orderLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get(orders) error = %v", err)
	}
	paymentLog, err := logs.Get("payments", 0)
	if err != nil {
		t.Fatalf("Get(payments) error = %v", err)
	}

	if err := logs.CloseTopic("orders"); err != nil {
		t.Fatalf("CloseTopic() error = %v", err)
	}

	reopenedOrderLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get(orders) reopened error = %v", err)
	}
	reusedPaymentLog, err := logs.Get("payments", 0)
	if err != nil {
		t.Fatalf("Get(payments) reused error = %v", err)
	}
	if reopenedOrderLog == orderLog {
		t.Fatal("CloseTopic() did not evict topic log from cache")
	}
	if reusedPaymentLog != paymentLog {
		t.Fatal("CloseTopic() evicted unrelated topic log")
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestLifecycleReadyAndClose(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	if _, err := logs.Get("orders", 0); err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	lifecycle := NewLifecycle(logs)
	if err := lifecycle.Ready(context.Background()); !IsNotReady(err) {
		t.Fatalf("Ready() error = %v, want not ready", err)
	}
	lifecycle.MarkReady()
	if err := lifecycle.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() after MarkReady error = %v", err)
	}
	if err := lifecycle.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := lifecycle.Ready(context.Background()); !IsNotReady(err) {
		t.Fatalf("Ready() after Close error = %v, want not ready", err)
	}
}

func TestSnapshotterSnapshotBuildsPartitionStats(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte("first")); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if _, err := log.Append([]byte("second")); err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}

	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	if err := offsets.Init(context.Background(), "orders", 0, -1); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	res, err := offsets.ReserveNext(context.Background(), "orders", 0, time.Minute, log.NextOffset())
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !res.Reserved {
		t.Fatalf("ReserveNext() reserved = false, want true (result=%+v)", res)
	}

	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	snapshots, err := snapshotter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("Snapshot() topics = %d, want 1", len(snapshots))
	}
	if len(snapshots[0].Partitions) != 1 {
		t.Fatalf("Snapshot() partitions = %d, want 1", len(snapshots[0].Partitions))
	}
	ps := snapshots[0].Partitions[0]
	if ps.LogStartOffset != 0 || ps.LogEndOffset != 2 {
		t.Fatalf("partition snapshot offsets = %+v, want start=0 end=2", ps)
	}
	if ps.CommittedOffset != 0 {
		t.Fatalf("partition snapshot committed offset = %d, want 0", ps.CommittedOffset)
	}
	if ps.InFlightSize != 1 || ps.AckedAheadSize != 0 {
		t.Fatalf("partition snapshot inflight = %+v", ps)
	}
	if ps.SegmentCount <= 0 {
		t.Fatalf("partition snapshot segment count = %d, want > 0", ps.SegmentCount)
	}
	if ps.SizeBytes < 0 {
		t.Fatalf("partition snapshot size bytes = %d, want non-negative", ps.SizeBytes)
	}
	if ps.OldestUnconsumedAt < 0 {
		t.Fatalf("partition snapshot oldest unconsumed at = %d, want non-negative", ps.OldestUnconsumedAt)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterPartitionSnapshotOmitsPartitionWhenLogOpenFails(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.getTopicErr = errors.New("boom")
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	snapshotter := NewSnapshotter(ms, offsets, newRuntimeTestLogs(t, ms), logger, "")

	ps, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 0)
	if ok {
		t.Fatalf("partitionSnapshot() ok = true, want false when log open fails")
	}
	if ps != (metrics.PartitionSnapshot{}) {
		t.Fatalf("partitionSnapshot() = %+v, want zero value when omitted", ps)
	}
}

func TestSnapshotterSnapshotReturnsMetastoreError(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.listTopicsErr = errors.New("list failed")
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, newRuntimeTestLogs(t, ms), slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	_, err := snapshotter.Snapshot(context.Background())
	if err == nil || err.Error() != "list failed" {
		t.Fatalf("Snapshot() error = %v, want list failed", err)
	}
}

func TestSnapshotterSnapshotSkipsPartitionWhenLogOpenFails(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2}
	logs := newRuntimeTestLogs(t, ms)
	if _, err := logs.Get("orders", 0); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	ms.getTopicErr = errors.New("boom")
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	snapshots, err := snapshotter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("Snapshot() topics = %d, want 1", len(snapshots))
	}
	if len(snapshots[0].Partitions) != 1 {
		t.Fatalf("Snapshot() partitions = %d, want 1 after skipping failing partition", len(snapshots[0].Partitions))
	}
	if snapshots[0].Partitions[0].Partition != 0 {
		t.Fatalf("Snapshot() kept partition = %d, want 0", snapshots[0].Partitions[0].Partition)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterSnapshotOmitsRemotePartitionsWhenSelfIDSet(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, RetentionMs: 1000}
	ms.setAssignment("orders", 0, "node-a")
	ms.setAssignment("orders", 1, "node-b")
	logs := newRuntimeTestLogs(t, ms)
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte("msg")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, logger, "node-a")

	snapshots, err := snapshotter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("Snapshot() topics = %d, want 1", len(snapshots))
	}
	if len(snapshots[0].Partitions) != 1 {
		t.Fatalf("Snapshot() partitions = %d, want 1 owned partition", len(snapshots[0].Partitions))
	}
	if snapshots[0].Partitions[0].Partition != 0 {
		t.Fatalf("Snapshot() kept partition = %d, want 0", snapshots[0].Partitions[0].Partition)
	}
	if _, ok := logs.logs[keyOf("orders", 1)]; ok {
		t.Fatal("Snapshot() opened remote partition log")
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterSnapshotFallsBackWhenSelfIDEmpty(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, RetentionMs: 1000}
	ms.setAssignment("orders", 0, "node-a")
	ms.setAssignment("orders", 1, "node-b")
	logs := newRuntimeTestLogs(t, ms)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	snapshots, err := snapshotter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("Snapshot() topics = %d, want 1", len(snapshots))
	}
	if len(snapshots[0].Partitions) != 2 {
		t.Fatalf("Snapshot() partitions = %d, want 2 with empty selfID fallback", len(snapshots[0].Partitions))
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterPartitionSnapshotOmitsRemotePartitionBeforeLogOpen(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 1000}
	ms.setAssignment("orders", 0, "node-b")
	logs := newRuntimeTestLogs(t, ms)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-a")

	ps, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 0)
	if ok {
		t.Fatalf("partitionSnapshot() ok = true, want false for remote partition: %+v", ps)
	}
	if _, exists := logs.logs[keyOf("orders", 0)]; exists {
		t.Fatal("partitionSnapshot() opened remote partition log")
	}
}

func TestSnapshotterPartitionSnapshotOmitsPartitionWhenAssignmentLookupFails(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-a")

	if _, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 0); ok {
		t.Fatal("partitionSnapshot() ok = true, want false when assignment lookup fails")
	}
	if _, exists := logs.logs[keyOf("orders", 0)]; exists {
		t.Fatal("partitionSnapshot() opened log after assignment lookup failure")
	}
}

func TestSnapshotterPartitionSnapshotIncludesOwnedPartitionWhenSelfIDSet(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 1000}
	ms.setAssignment("orders", 0, "node-a")
	logs := newRuntimeTestLogs(t, ms)
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte("msg")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-a")

	ps, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 0)
	if !ok {
		t.Fatal("partitionSnapshot() ok = false, want true for owned partition")
	}
	if ps.Partition != 0 || ps.LogEndOffset != 1 {
		t.Fatalf("partitionSnapshot() = %+v, want owned partition stats", ps)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterPartitionSnapshotFallsBackWithoutAssignmentReader(t *testing.T) {
	base := newRuntimeFakeMetastore()
	ms := runtimeMetastoreWithoutAssignments{inner: base}
	base.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte("msg")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-a")

	ps, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 0)
	if !ok {
		t.Fatal("partitionSnapshot() ok = false, want fallback success without assignment reader")
	}
	if ps.LogEndOffset != 1 {
		t.Fatalf("partitionSnapshot() = %+v, want opened partition stats", ps)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterPartitionSnapshotOmitsRemotePartitionWhenTopicLookupFailsWouldHaveOpenedLog(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 1000}
	ms.setAssignment("orders", 0, "node-b")
	ms.getTopicErr = errors.New("boom")
	logs := newRuntimeTestLogs(t, ms)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-a")

	if _, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 0); ok {
		t.Fatal("partitionSnapshot() ok = true, want false for remote partition")
	}
	if _, exists := logs.logs[keyOf("orders", 0)]; exists {
		t.Fatal("partitionSnapshot() opened remote log despite ownership guard")
	}
}

func TestSnapshotterPartitionSnapshotReturnsFalseForMissingTopicOpenErrorOnlyWhenMetastoreFails(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.getTopicErr = errors.New("lookup failed")
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, newRuntimeTestLogs(t, ms), slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	_, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 9)
	if ok {
		t.Fatal("partitionSnapshot() ok = true, want false")
	}
}

func TestLogsCloseAllIsIdempotent(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	if _, err := logs.Get("orders", 0); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("first CloseAll() error = %v", err)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("second CloseAll() error = %v", err)
	}
}

func TestCloseTopicWithNoCachedLogsSucceeds(t *testing.T) {
	logs := newRuntimeTestLogs(t, newRuntimeFakeMetastore())
	if err := logs.CloseTopic("missing"); err != nil {
		t.Fatalf("CloseTopic() error = %v, want nil", err)
	}
}

func TestNewLifecycleStoresLogs(t *testing.T) {
	logs := newRuntimeTestLogs(t, newRuntimeFakeMetastore())
	lifecycle := NewLifecycle(logs)
	if lifecycle.logs != logs {
		t.Fatal("NewLifecycle() did not retain logs dependency")
	}
}

func TestLifecycleReadyRequiresMarkReady(t *testing.T) {
	lifecycle := NewLifecycle(newRuntimeTestLogs(t, newRuntimeFakeMetastore()))
	if err := lifecycle.Ready(context.Background()); !IsNotReady(err) {
		t.Fatalf("Ready() error = %v, want not ready", err)
	}

	lifecycle.MarkReady()
	if err := lifecycle.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() after MarkReady error = %v", err)
	}

	lifecycle.MarkNotReady()
	if err := lifecycle.Ready(context.Background()); !IsNotReady(err) {
		t.Fatalf("Ready() after MarkNotReady error = %v, want not ready", err)
	}
}

func TestLifecycleCloseMarksNotReady(t *testing.T) {
	lifecycle := NewLifecycle(newRuntimeTestLogs(t, newRuntimeFakeMetastore()))
	lifecycle.MarkReady()
	if err := lifecycle.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := lifecycle.Ready(context.Background()); !IsNotReady(err) {
		t.Fatalf("Ready() after Close error = %v, want not ready", err)
	}
}

func TestLifecycleIsNotReadyRecognizesWrappedError(t *testing.T) {
	if !IsNotReady(fmt.Errorf("wrap: %w", errNotReady)) {
		t.Fatal("IsNotReady(wrapped) = false, want true")
	}
}

func TestNewSnapshotterStoresDependencies(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	logs := newRuntimeTestLogs(t, ms)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	snapshotter := NewSnapshotter(ms, offsets, logs, logger, "")
	if snapshotter.metastore != ms || snapshotter.offsets != offsets || snapshotter.logs != logs || snapshotter.logger != logger {
		t.Fatal("NewSnapshotter() did not retain dependencies")
	}
}

func TestNewLogsStoresDependencies(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	opts := storage.Options{FlushInterval: time.Second}
	logs := NewLogs("/tmp/test-data", opts, ms, nil)
	if logs.dataDir != "/tmp/test-data" {
		t.Fatalf("NewLogs() dataDir = %q, want /tmp/test-data", logs.dataDir)
	}
	if logs.metastore != ms {
		t.Fatal("NewLogs() did not retain metastore")
	}
	if logs.storageOpts.FlushInterval != time.Second {
		t.Fatalf("NewLogs() flush interval = %v, want %v", logs.storageOpts.FlushInterval, time.Second)
	}
}

func TestSnapshotterPartitionSnapshotUsesOffsetSnapshots(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	log, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte("msg")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 2, MaxAckedAhead: 2}, nil
	}, nil)
	if err := offsets.Init(context.Background(), "orders", 0, -1); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	res, err := offsets.ReserveNext(context.Background(), "orders", 0, time.Minute, log.NextOffset())
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if err := offsets.CommitHandle("orders", 0, res.Offset, res.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
	snapshotter := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	ps, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 0)
	if !ok {
		t.Fatal("partitionSnapshot() ok = false, want true")
	}
	if ps.CommittedOffset != 1 {
		t.Fatalf("partitionSnapshot() committed offset = %d, want 1", ps.CommittedOffset)
	}
	if ps.InFlightSize != 0 || ps.AckedAheadSize != 0 {
		t.Fatalf("partitionSnapshot() inflight stats = %+v, want empty", ps)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterSnapshotReturnsEmptyWhenNoTopics(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, newRuntimeTestLogs(t, ms), slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	snapshots, err := snapshotter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshots) != 0 {
		t.Fatalf("Snapshot() topics = %d, want 0", len(snapshots))
	}
}

func TestPartitionSnapshotReturnsFalseForMissingTopicOpenErrorOnlyWhenMetastoreFails(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.getTopicErr = errors.New("lookup failed")
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, newRuntimeTestLogs(t, ms), slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	_, ok := snapshotter.partitionSnapshot(context.Background(), "orders", 9)
	if ok {
		t.Fatal("partitionSnapshot() ok = true, want false")
	}
}

func TestLogsGetCreatesDistinctPartitions(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	first, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get(partition 0) error = %v", err)
	}
	second, err := logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get(partition 1) error = %v", err)
	}
	if first == second {
		t.Fatal("Get() returned same log for different partitions")
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestLogsCloseTopicAfterCloseAllSucceeds(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	if _, err := logs.Get("orders", 0); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
	if err := logs.CloseTopic("orders"); err != nil {
		t.Fatalf("CloseTopic() after CloseAll error = %v", err)
	}
}

func TestLogsGetCanReopenAfterCloseAll(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", RetentionMs: 1000}
	logs := newRuntimeTestLogs(t, ms)
	first, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() first error = %v", err)
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
	second, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() second error = %v", err)
	}
	if first == second {
		t.Fatal("Get() did not reopen fresh log after CloseAll")
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() second error = %v", err)
	}
}

func TestLogsGetOnUnknownTopicStillCaches(t *testing.T) {
	logs := newRuntimeTestLogs(t, newRuntimeFakeMetastore())
	first, err := logs.Get("ghost", 0)
	if err != nil {
		t.Fatalf("Get() first error = %v", err)
	}
	second, err := logs.Get("ghost", 0)
	if err != nil {
		t.Fatalf("Get() second error = %v", err)
	}
	if first != second {
		t.Fatal("Get() did not cache unknown-topic partition log")
	}
	if err := logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}
}

func TestSnapshotterSnapshotTopicNameIsPreserved(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 0}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	snapshotter := NewSnapshotter(ms, offsets, newRuntimeTestLogs(t, ms), slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	snapshots, err := snapshotter.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	if len(snapshots) != 1 || snapshots[0].Topic != "orders" {
		t.Fatalf("Snapshot() = %+v, want topic orders", snapshots)
	}
}

func TestLifecycleCloseWithNoLogsSucceeds(t *testing.T) {
	logs := newRuntimeTestLogs(t, newRuntimeFakeMetastore())
	lifecycle := NewLifecycle(logs)
	if err := lifecycle.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestLogsGetErrorWrapIncludesContext(t *testing.T) {
	ms := newRuntimeFakeMetastore()
	ms.getTopicErr = errors.New("db down")
	logs := newRuntimeTestLogs(t, ms)
	_, err := logs.Get("orders", 0)
	if err == nil || !strings.Contains(err.Error(), "lookup topic for retention") {
		t.Fatalf("Get() error = %v, want wrapped context", err)
	}
}
