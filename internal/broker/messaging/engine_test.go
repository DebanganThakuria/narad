package messaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/replication"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

type messagingFakeMetastore struct {
	topics      map[string]topic.Topic
	getTopicErr error
}

func newMessagingFakeMetastore() *messagingFakeMetastore {
	return &messagingFakeMetastore{topics: map[string]topic.Topic{}}
}

func (f *messagingFakeMetastore) CreateTopic(_ context.Context, t topic.Topic) error {
	f.topics[t.Name] = t
	return nil
}

func (f *messagingFakeMetastore) UpdateTopic(_ context.Context, t topic.Topic) error {
	f.topics[t.Name] = t
	return nil
}

func (f *messagingFakeMetastore) DeleteTopic(_ context.Context, name string) error {
	delete(f.topics, name)
	return nil
}

func (f *messagingFakeMetastore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	if f.getTopicErr != nil {
		return topic.Topic{}, f.getTopicErr
	}
	t, ok := f.topics[name]
	if !ok {
		return topic.Topic{}, errs.ErrNotFound
	}
	return t, nil
}

func (f *messagingFakeMetastore) ListTopics(_ context.Context, _ metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}

func (f *messagingFakeMetastore) PutSchema(_ context.Context, _ string, _ int, _ []byte) error {
	return nil
}

func (f *messagingFakeMetastore) GetSchema(_ context.Context, _ string, _ int) ([]byte, error) {
	return nil, errs.ErrNotFound
}

func (f *messagingFakeMetastore) GetConsumerOffset(_ context.Context, _ string, _ int) (int64, error) {
	return 0, nil
}

func (f *messagingFakeMetastore) SetConsumerOffset(_ context.Context, _ string, _ int, _ int64) error {
	return nil
}

func (f *messagingFakeMetastore) Close() error { return nil }

type fakeSchemas struct {
	validateErr error
	lastTopic   string
	lastPayload []byte
}

func (f *fakeSchemas) Register(_ context.Context, _ string, _ []byte) (int, error) {
	return 1, nil
}

func (f *fakeSchemas) Validate(_ context.Context, topic string, payload []byte) error {
	f.lastTopic = topic
	f.lastPayload = append([]byte(nil), payload...)
	return f.validateErr
}

type fixedPartitioner struct {
	picked int
}

func (p fixedPartitioner) Pick(string, string, int) int { return p.picked }

type fakeReplicator struct {
	err           error
	called        bool
	lastTopic     string
	lastPartition int
	lastOffset    int64
	lastPayload   []byte
}

func (r *fakeReplicator) Replicate(_ context.Context, topic string, partition int, offset int64, payload []byte) error {
	r.called = true
	r.lastTopic = topic
	r.lastPartition = partition
	r.lastOffset = offset
	r.lastPayload = append([]byte(nil), payload...)
	return r.err
}

func newTestEngine(t *testing.T, ms *messagingFakeMetastore, schemas schema.Registry, partitioner partition.Manager, replicator replication.Replicator) *Engine {
	t.Helper()
	if ms == nil {
		ms = newMessagingFakeMetastore()
	}
	if schemas == nil {
		schemas = &fakeSchemas{}
	}
	if partitioner == nil {
		partitioner = fixedPartitioner{picked: 0}
	}
	if replicator == nil {
		replicator = &fakeReplicator{}
	}
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	return NewEngine(ms, schemas, partitioner, replicator, offsets, logs, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestGetTopicMapsNotFound(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil, nil)

	_, err := engine.getTopic(context.Background(), "missing")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("getTopic() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestGetTopicWrapsUnexpectedError(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.getTopicErr = errors.New("db down")
	engine := newTestEngine(t, ms, nil, nil, nil)

	_, err := engine.getTopic(context.Background(), "orders")
	if err == nil || err.Error() != "messaging: get topic: db down" {
		t.Fatalf("getTopic() error = %v, want wrapped error", err)
	}
}

func TestProduceAppendsAndReplicates(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	schemas := &fakeSchemas{}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schemas, fixedPartitioner{picked: 1}, replicator)

	offset, partitionIdx, err := engine.Produce(context.Background(), "orders", "customer-1", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 {
		t.Fatalf("Produce() offset = %d, want 0", offset)
	}
	if partitionIdx != 1 {
		t.Fatalf("Produce() partition = %d, want 1", partitionIdx)
	}
	if schemas.lastTopic != "orders" || string(schemas.lastPayload) != `{"id":1}` {
		t.Fatalf("schema Validate() args = topic=%q payload=%q", schemas.lastTopic, string(schemas.lastPayload))
	}
	if !replicator.called {
		t.Fatal("Replicate() was not called")
	}
	if replicator.lastTopic != "orders" || replicator.lastPartition != 1 || replicator.lastOffset != 0 {
		t.Fatalf("Replicate() args = topic=%q partition=%d offset=%d", replicator.lastTopic, replicator.lastPartition, replicator.lastOffset)
	}
}

func TestProduceAllowsMissingSchema(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	schemas := &fakeSchemas{validateErr: errs.ErrSchemaNotFound}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schemas, fixedPartitioner{picked: 0}, replicator)

	offset, partitionIdx, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 || partitionIdx != 0 {
		t.Fatalf("Produce() = offset %d partition %d, want 0,0", offset, partitionIdx)
	}
	if !replicator.called {
		t.Fatal("Replicate() was not called for schema-not-found path")
	}
}

func TestProduceRejectsSchemaValidationError(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	schemas := &fakeSchemas{validateErr: errors.New("invalid payload")}
	replicator := &fakeReplicator{}
	engine := newTestEngine(t, ms, schemas, fixedPartitioner{picked: 0}, replicator)

	_, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err == nil || err.Error() != "invalid payload" {
		t.Fatalf("Produce() error = %v, want invalid payload", err)
	}
	if replicator.called {
		t.Fatal("Replicate() was called after schema validation failure")
	}
}

func TestProduceReturnsReplicatorError(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	replicator := &fakeReplicator{err: errors.New("replication failed")}
	engine := newTestEngine(t, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)

	_, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`))
	if err == nil || err.Error() != "messaging: replicate: replication failed" {
		t.Fatalf("Produce() error = %v, want wrapped replicate error", err)
	}
}

func TestReplayReadReturnsMessageWhenOffsetExists(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	msg, found, err := engine.replayRead("orders", 1, 0, 2)
	if err != nil {
		t.Fatalf("replayRead() error = %v", err)
	}
	if !found {
		t.Fatal("replayRead() found = false, want true")
	}
	if msg.Topic != "orders" || msg.Partition != 1 || msg.Offset != 0 {
		t.Fatalf("replayRead() message = %+v", msg)
	}
	if string(msg.Payload) != `{"id":1}` {
		t.Fatalf("replayRead() payload = %q, want %q", string(msg.Payload), `{"id":1}`)
	}
	if msg.ReceiptHandle != "" {
		t.Fatalf("replayRead() receipt handle = %q, want empty", msg.ReceiptHandle)
	}
}

func TestReplayReadReturnsNotFoundWhenOffsetPastTail(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)

	_, found, err := engine.replayRead("orders", 0, 0, 1)
	if err != nil {
		t.Fatalf("replayRead() error = %v", err)
	}
	if found {
		t.Fatal("replayRead() found = true, want false")
	}
}

func TestReplayReadRejectsOutOfRangePartition(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil, nil)

	_, _, err := engine.replayRead("orders", -1, 0, 1)
	if err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("replayRead() error = %v, want %v", err, ErrInvalid)
	}
}

func TestConsumeRequiresPartitionWhenOffsetProvided(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	offset := int64(0)

	_, _, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Offset: &offset})
	if !errors.Is(err, ErrPartitionRequired) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrPartitionRequired)
	}
}

func TestConsumeRejectsOutOfRangePinnedPartition(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	partitionIdx := 1

	_, _, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx})
	if err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrInvalid)
	}
}

func TestAllPartitions(t *testing.T) {
	got := allPartitions(4)
	want := []int{0, 1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("allPartitions() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("allPartitions()[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestWaitForActivityReturnsDeadlineExceededOnTimeout(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)

	err := engine.waitForActivity(context.Background(), "orders", []int{0}, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForActivity() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestWaitForActivityReturnsContextCancellation(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := engine.waitForActivity(ctx, "orders", []int{0}, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForActivity() error = %v, want %v", err, context.Canceled)
	}
}

func TestAckRejectsMissingInputsAndTopicMismatch(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)

	if err := engine.Ack(context.Background(), "", "handle"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Ack() empty topic error = %v, want %v", err, ErrInvalid)
	}
	if err := engine.Ack(context.Background(), "orders", ""); !errors.Is(err, consumer.ErrHandleMalformed) {
		t.Fatalf("Ack() empty handle error = %v, want %v", err, consumer.ErrHandleMalformed)
	}
	handle := consumer.EncodeHandle(consumer.Handle{Topic: "payments", Partition: 0, Offset: 0, Nonce: 1})
	if err := engine.Ack(context.Background(), "orders", handle); !errors.Is(err, consumer.ErrHandleTopicMismatch) {
		t.Fatalf("Ack() topic mismatch error = %v, want %v", err, consumer.ErrHandleTopicMismatch)
	}
}

func TestConsumeReturnsNoMessageWhenWaitIsNonPositive(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)

	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if found {
		t.Fatalf("Consume() found = %v, want false with msg %+v", found, msg)
	}
}

func TestConsumeReturnsNoMessageWhenContextCancelledDuringWait(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg, found, err := engine.Consume(ctx, "orders", ConsumeOpts{Wait: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if found {
		t.Fatalf("Consume() found = %v, want false with msg %+v", found, msg)
	}
}

func TestConsumeReturnsMessageForPinnedPartition(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":42}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	partitionIdx := 1

	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if msg.Partition != 1 || msg.Offset != 0 || string(msg.Payload) != `{"id":42}` {
		t.Fatalf("Consume() message = %+v", msg)
	}
	if msg.ReceiptHandle == "" {
		t.Fatal("Consume() receipt handle = empty, want non-empty")
	}
}

func TestAckReturnsTopicNotFoundWhenTopicDeleted(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	partitionIdx := 0
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	delete(ms.topics, "orders")

	err = engine.Ack(context.Background(), "orders", msg.ReceiptHandle)
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Ack() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestAckCommitsReservedHandle(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	partitionIdx := 0
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}

	if err := engine.Ack(context.Background(), "orders", msg.ReceiptHandle); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestAckRejectsStaleHandleAfterCommit(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	partitionIdx := 0
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if err := engine.Ack(context.Background(), "orders", msg.ReceiptHandle); err != nil {
		t.Fatalf("Ack() first error = %v", err)
	}

	err = engine.Ack(context.Background(), "orders", msg.ReceiptHandle)
	if !errors.Is(err, consumer.ErrHandleStale) {
		t.Fatalf("Ack() second error = %v, want %v", err, consumer.ErrHandleStale)
	}
}

func TestProduceRejectsMissingTopic(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil, nil)

	_, _, err := engine.Produce(context.Background(), "missing", "", []byte(`{"id":1}`))
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Produce() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestTryQueueReadReturnsMessageFromFirstReservablePartition(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)

	log0, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get(0) error = %v", err)
	}
	if _, err := log0.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	if _, _, err := engine.tryQueueRead(context.Background(), "orders", []int{0}, time.Second); err != nil {
		t.Fatalf("tryQueueRead() reserve first partition error = %v", err)
	}

	log1, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get(1) error = %v", err)
	}
	if _, err := log1.Append([]byte(`{"id":2}`)); err != nil {
		t.Fatalf("Append(1) error = %v", err)
	}

	msg, found, err := engine.tryQueueRead(context.Background(), "orders", []int{0, 1}, time.Second)
	if err != nil {
		t.Fatalf("tryQueueRead() error = %v", err)
	}
	if !found {
		t.Fatal("tryQueueRead() found = false, want true")
	}
	if msg.Partition != 1 || msg.Offset != 0 || string(msg.Payload) != `{"id":2}` {
		t.Fatalf("tryQueueRead() message = %+v", msg)
	}
}

func TestConsumeScansPartitionsInOrder(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)

	log1, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log1.Append([]byte(`{"id":99}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if msg.Partition != 1 || msg.Offset != 0 || string(msg.Payload) != `{"id":99}` {
		t.Fatalf("Consume() message = %+v", msg)
	}
}

func TestConsumeReplayReturnsMessageForExistingOffset(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":7}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	partitionIdx := 0
	offset := int64(0)

	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx, Offset: &offset})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if msg.Partition != 0 || msg.Offset != 0 || string(msg.Payload) != `{"id":7}` {
		t.Fatalf("Consume() message = %+v", msg)
	}
	if msg.ReceiptHandle != "" {
		t.Fatalf("Consume() receipt handle = %q, want empty", msg.ReceiptHandle)
	}
}

func TestConsumeReplayReturnsNoMessagePastTail(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	partitionIdx := 0
	offset := int64(5)

	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx, Offset: &offset})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if found {
		t.Fatalf("Consume() found = %v, want false with msg %+v", found, msg)
	}
}

func TestWaitForActivityReturnsNilWhenPartitionNotifies(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.waitForActivity(context.Background(), "orders", []int{0}, time.Second)
	}()

	time.Sleep(20 * time.Millisecond)
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("waitForActivity() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waitForActivity() timed out waiting for notification")
	}
}

func TestAckRejectsMalformedHandle(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)

	err := engine.Ack(context.Background(), "orders", "not-a-valid-handle")
	if !errors.Is(err, consumer.ErrHandleMalformed) {
		t.Fatalf("Ack() error = %v, want %v", err, consumer.ErrHandleMalformed)
	}
}

func TestAckRejectsUnreservedHandle(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)
	handle := consumer.EncodeHandle(consumer.Handle{Topic: "orders", Partition: 0, Offset: 0, Nonce: 123})

	err := engine.Ack(context.Background(), "orders", handle)
	if !errors.Is(err, consumer.ErrHandleStale) {
		t.Fatalf("Ack() error = %v, want %v", err, consumer.ErrHandleStale)
	}
}

func TestConsumeReturnsTopicNotFoundForMissingTopic(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil, nil)

	_, _, err := engine.Consume(context.Background(), "missing", ConsumeOpts{})
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestAckReturnsTopicNotFoundForMissingTopic(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil, nil)
	handle := consumer.EncodeHandle(consumer.Handle{Topic: "missing", Partition: 0, Offset: 0, Nonce: 1})

	err := engine.Ack(context.Background(), "missing", handle)
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Ack() error = %v, want %v", err, ErrTopicNotFound)
	}
}
