package messaging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

type messagingFakeMetastore struct {
	topics                map[string]topic.Topic
	schemas               map[string]map[int][]byte
	metadataVersion       uint64
	nextDomainVersion     uint64
	topicVersions         map[string]uint64
	assignmentVersions    map[string]uint64
	schemaVersions        map[string]uint64
	routingMembersVersion uint64
	getTopicErr           error
	getTopicCalls         int
	getSchemaCalls        int
}

func newMessagingFakeMetastore() *messagingFakeMetastore {
	return &messagingFakeMetastore{
		topics:             map[string]topic.Topic{},
		schemas:            map[string]map[int][]byte{},
		topicVersions:      map[string]uint64{},
		assignmentVersions: map[string]uint64{},
		schemaVersions:     map[string]uint64{},
	}
}

func (f *messagingFakeMetastore) CreateTopic(_ context.Context, t topic.Topic) error {
	f.topics[t.Name] = t
	f.bumpMetadataVersion()
	f.bumpTopicVersion(t.Name)
	return nil
}

func (f *messagingFakeMetastore) UpdateTopic(_ context.Context, t topic.Topic) error {
	f.topics[t.Name] = t
	f.bumpMetadataVersion()
	f.bumpTopicVersion(t.Name)
	return nil
}

func (f *messagingFakeMetastore) DeleteTopic(_ context.Context, name string) error {
	delete(f.topics, name)
	f.bumpMetadataVersion()
	f.bumpTopicVersion(name)
	f.bumpAssignmentVersion(name)
	f.bumpSchemaVersion(name)
	return nil
}

func (f *messagingFakeMetastore) AttachChild(context.Context, string, string) error { return nil }
func (f *messagingFakeMetastore) DetachChild(context.Context, string, string) error { return nil }

func (f *messagingFakeMetastore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	f.getTopicCalls++
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

func (f *messagingFakeMetastore) PutSchema(_ context.Context, topicName string, version int, raw []byte) error {
	if f.schemas[topicName] == nil {
		f.schemas[topicName] = map[int][]byte{}
	}
	f.schemas[topicName][version] = append([]byte(nil), raw...)
	f.bumpMetadataVersion()
	f.bumpSchemaVersion(topicName)
	return nil
}

func (f *messagingFakeMetastore) GetSchema(_ context.Context, topicName string, version int) ([]byte, error) {
	f.getSchemaCalls++
	if versions, ok := f.schemas[topicName]; ok {
		if raw, ok := versions[version]; ok {
			return append([]byte(nil), raw...), nil
		}
	}
	return nil, errs.ErrNotFound
}

func (f *messagingFakeMetastore) LeaderAddr() string { return "" }

func (f *messagingFakeMetastore) GetMember(string) (metastore.Member, error) {
	return metastore.Member{}, errs.ErrNotFound
}

func (f *messagingFakeMetastore) Close() error { return nil }

func (f *messagingFakeMetastore) MetadataVersion() uint64 {
	return f.metadataVersion
}

func (f *messagingFakeMetastore) TopicVersion(name string) uint64 {
	return f.topicVersions[name]
}

func (f *messagingFakeMetastore) AssignmentVersion(topicName string) uint64 {
	return f.assignmentVersions[topicName]
}

func (f *messagingFakeMetastore) SchemaVersion(topicName string) uint64 {
	return f.schemaVersions[topicName]
}

func (f *messagingFakeMetastore) RoutingMembersVersion() uint64 {
	return f.routingMembersVersion
}

func (f *messagingFakeMetastore) bumpMetadataVersion() {
	f.metadataVersion++
}

func (f *messagingFakeMetastore) bumpTopicVersion(name string) {
	f.nextDomainVersion++
	f.topicVersions[name] = f.nextDomainVersion
}

func (f *messagingFakeMetastore) bumpAssignmentVersion(topicName string) {
	f.nextDomainVersion++
	f.assignmentVersions[topicName] = f.nextDomainVersion
}

func (f *messagingFakeMetastore) bumpSchemaVersion(topicName string) {
	f.nextDomainVersion++
	f.schemaVersions[topicName] = f.nextDomainVersion
}

type fakeSchemas struct {
	mu          sync.Mutex
	validateErr error
	lastTopic   string
	lastPayload []byte
	loads       []schemaLoad
}

type schemaLoad struct {
	topic   string
	version int
	raw     []byte
}

func (f *fakeSchemas) ValidateDefinition(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (f *fakeSchemas) Register(_ context.Context, _ string, _ []byte) (int, error) {
	return 1, nil
}

func (f *fakeSchemas) Load(_ context.Context, topic string, version int, raw []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads = append(f.loads, schemaLoad{topic: topic, version: version, raw: append([]byte(nil), raw...)})
	f.validateErr = nil
	return nil
}

func (f *fakeSchemas) Unload(_ context.Context, _ string, _ int) error {
	return nil
}

func (f *fakeSchemas) DropTopic(_ context.Context, _ string) error {
	return nil
}

func (f *fakeSchemas) Validate(_ context.Context, topic string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTopic = topic
	f.lastPayload = append([]byte(nil), payload...)
	return f.validateErr
}

type fixedPartitioner struct {
	picked int
}

func (p fixedPartitioner) Pick(string, string, int) int { return p.picked }

func newTestStore(t *testing.T) *metastore.Store {
	t.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   "node-self",
		DataDir:  t.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 3}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			return store
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("timed out waiting for leader")
	return nil
}

type fixedPartitionManager struct {
	picked int
}

func (f fixedPartitionManager) Pick(string, string, int) int { return f.picked }

func newClusterTestEngine(t *testing.T, store *metastore.Store, mgr partition.Manager) *Engine {
	t.Helper()
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	return NewEngine(
		store,
		schema.NewAlwaysValid(),
		mgr,
		offsets,
		logs,
		nil,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"node-self",
	)
}

func newTestEngine(t *testing.T, ms *messagingFakeMetastore, schemas schema.Registry, partitioner partition.Manager) *Engine {
	t.Helper()
	return newTestEngineWithDir(t, t.TempDir(), ms, schemas, partitioner)
}

func newTestEngineWithDir(t *testing.T, dataDir string, ms *messagingFakeMetastore, schemas schema.Registry, partitioner partition.Manager) *Engine {
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
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(topic string, partition int, offset int64) {
		partitionDir := storage.TopicPartitionDir(dataDir, topic, partition)
		if err := storage.WriteConsumerOffset(partitionDir, offset); err != nil {
			panic(err)
		}
	})
	for topicName, cfg := range ms.topics {
		for partition := 0; partition < cfg.Partitions; partition++ {
			partitionDir := storage.TopicPartitionDir(dataDir, topicName, partition)
			committed, ok, err := storage.ReadConsumerOffset(partitionDir)
			if err != nil || !ok {
				continue
			}
			if err := offsets.Init(context.Background(), topicName, partition, committed); err != nil {
				panic(err)
			}
		}
	}
	return NewEngine(ms, schemas, partitioner, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
}

func decodeHandleForTest(t *testing.T, receiptHandle string) consumer.Handle {
	t.Helper()
	h, err := consumer.DecodeHandle(receiptHandle)
	if err != nil {
		t.Fatalf("DecodeHandle(%q) error = %v", receiptHandle, err)
	}
	return h
}

func partitionHWMPath(dataDir, topicName string, partition int) string {
	return filepath.Join(storage.TopicPartitionDir(dataDir, topicName, partition), "hwm")
}

func TestGetTopicMapsNotFound(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil)

	_, err := engine.getTopic(context.Background(), "missing")
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("getTopic() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestGetTopicWrapsUnexpectedError(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.getTopicErr = errors.New("db down")
	engine := newTestEngine(t, ms, nil, nil)

	_, err := engine.getTopic(context.Background(), "orders")
	if err == nil || err.Error() != "messaging: get topic: db down" {
		t.Fatalf("getTopic() error = %v, want wrapped error", err)
	}
}

func TestGetTopicUsesVersionedCache(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	engine := newTestEngine(t, ms, nil, nil)

	first, err := engine.getTopic(context.Background(), "orders")
	if err != nil {
		t.Fatalf("first getTopic() error = %v", err)
	}
	second, err := engine.getTopic(context.Background(), "orders")
	if err != nil {
		t.Fatalf("second getTopic() error = %v", err)
	}
	if first.Partitions != 3 || second.Partitions != 3 {
		t.Fatalf("cached topics = %+v %+v, want partitions=3", first, second)
	}
	if ms.getTopicCalls != 1 {
		t.Fatalf("GetTopic calls = %d, want 1", ms.getTopicCalls)
	}
}

func TestGetTopicInvalidatesCacheOnTopicVersionChange(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	engine := newTestEngine(t, ms, nil, nil)

	first, err := engine.getTopic(context.Background(), "orders")
	if err != nil {
		t.Fatalf("first getTopic() error = %v", err)
	}
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 5}
	ms.bumpTopicVersion("orders")
	second, err := engine.getTopic(context.Background(), "orders")
	if err != nil {
		t.Fatalf("second getTopic() error = %v", err)
	}

	if first.Partitions != 3 || second.Partitions != 5 {
		t.Fatalf("topics = %+v then %+v, want partitions 3 then 5", first, second)
	}
	if ms.getTopicCalls != 2 {
		t.Fatalf("GetTopic calls = %d, want 2", ms.getTopicCalls)
	}
}

func TestReplayReadReturnsMessageWhenOffsetExists(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2}
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
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
	engine := newTestEngine(t, ms, nil, nil)

	_, found, err := engine.replayRead("orders", 0, 0, 1)
	if err != nil {
		t.Fatalf("replayRead() error = %v", err)
	}
	if found {
		t.Fatal("replayRead() found = true, want false")
	}
}

func TestReplayReadRejectsOutOfRangePartition(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil)

	_, _, err := engine.replayRead("orders", -1, 0, 1)
	if err == nil || !errors.Is(err, ErrInvalid) {
		t.Fatalf("replayRead() error = %v, want %v", err, ErrInvalid)
	}
}

func TestConsumeRequiresPartitionWhenOffsetProvided(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil)
	_, _, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Offset: new(int64(0))})
	if !errors.Is(err, ErrPartitionRequired) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrPartitionRequired)
	}
}

func TestConsumeRejectsOutOfRangePinnedPartition(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil)
	_, _, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(1)})
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
	engine := newTestEngine(t, ms, nil, nil)

	chans, err := engine.notifyChannels("orders", []int{0})
	if err != nil {
		t.Fatalf("notifyChannels() error = %v", err)
	}
	err = engine.waitForActivity(context.Background(), chans, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForActivity() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestWaitForActivityReturnsContextCancellation(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	chans, err := engine.notifyChannels("orders", []int{0})
	if err != nil {
		t.Fatalf("notifyChannels() error = %v", err)
	}
	err = engine.waitForActivity(ctx, chans, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForActivity() error = %v, want %v", err, context.Canceled)
	}
}

func TestAckRejectsMissingInputs(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil)

	validHandle := consumer.Handle{Partition: 0, Offset: 0, Nonce: 1}
	if err := engine.Ack(context.Background(), "", validHandle); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Ack() empty topic error = %v, want %v", err, ErrInvalid)
	}
	if err := engine.Ack(context.Background(), "orders", consumer.Handle{}); !errors.Is(err, consumer.ErrHandleMalformed) {
		t.Fatalf("Ack() empty handle error = %v, want %v", err, consumer.ErrHandleMalformed)
	}
}

func TestConsumeReturnsNoMessageWhenWaitIsNonPositive(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil)

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
	engine := newTestEngine(t, ms, nil, nil)
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
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":42}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(1)})
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
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(0)})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if err := ms.DeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("DeleteTopic() error = %v", err)
	}

	err = engine.Ack(context.Background(), "orders", decodeHandleForTest(t, msg.ReceiptHandle))
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Ack() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestAckCommitsReservedHandle(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(0)})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}

	if err := engine.Ack(context.Background(), "orders", decodeHandleForTest(t, msg.ReceiptHandle)); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestAckUsesVersionedTopicCache(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}

	ms.getTopicCalls = 0
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(0)})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if ms.getTopicCalls != 1 {
		t.Fatalf("GetTopic calls after consume = %d, want 1", ms.getTopicCalls)
	}

	if err := engine.Ack(context.Background(), "orders", decodeHandleForTest(t, msg.ReceiptHandle)); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	if ms.getTopicCalls != 1 {
		t.Fatalf("GetTopic calls after ack = %d, want still 1", ms.getTopicCalls)
	}
}

func TestAckRejectsStaleHandleAfterCommit(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(0)})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if err := engine.Ack(context.Background(), "orders", decodeHandleForTest(t, msg.ReceiptHandle)); err != nil {
		t.Fatalf("Ack() first error = %v", err)
	}

	err = engine.Ack(context.Background(), "orders", decodeHandleForTest(t, msg.ReceiptHandle))
	if !errors.Is(err, consumer.ErrHandleStale) {
		t.Fatalf("Ack() second error = %v, want %v", err, consumer.ErrHandleStale)
	}
}

func TestAckAckedAheadCapWithNilMetricsReturnsError(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 3, MaxAckedAhead: 1}, nil
	}, nil)
	engine := NewEngine(ms, nil, nil, offsets, nil, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	if _, err := offsets.ReserveNext(context.Background(), "orders", 0, time.Second, 2); err != nil {
		t.Fatalf("ReserveNext(offset 0) error = %v", err)
	}
	r1, err := offsets.ReserveNext(context.Background(), "orders", 0, time.Second, 3)
	if err != nil {
		t.Fatalf("ReserveNext(offset 1) error = %v", err)
	}
	handle := consumer.Handle{Partition: 0, Offset: r1.Offset, Nonce: r1.Nonce}
	if err := engine.Ack(context.Background(), "orders", handle); err != nil {
		t.Fatalf("Ack(offset 1) error = %v", err)
	}
	r2, err := offsets.ReserveNext(context.Background(), "orders", 0, time.Second, 3)
	if err != nil {
		t.Fatalf("ReserveNext(offset 2) error = %v", err)
	}
	handle = consumer.Handle{Partition: 0, Offset: r2.Offset, Nonce: r2.Nonce}

	err = engine.Ack(context.Background(), "orders", handle)
	if !errors.Is(err, consumer.ErrAckedAheadFull) {
		t.Fatalf("Ack() error = %v, want %v", err, consumer.ErrAckedAheadFull)
	}
}

func TestProduceRejectsMissingTopic(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil)

	_, _, err := engine.Produce(context.Background(), "missing", "", []byte(`{"id":1}`))
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Produce() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestTryQueueReadReturnsMessageFromFirstReservablePartition(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil)

	log0, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get(0) error = %v", err)
	}
	if _, err := log0.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append(0) error = %v", err)
	}
	if err := log0.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark(0) error = %v", err)
	}
	if _, _, err := engine.tryQueueRead(context.Background(), "orders", []int{0}, 0, time.Second); err != nil {
		t.Fatalf("tryQueueRead() reserve first partition error = %v", err)
	}

	log1, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get(1) error = %v", err)
	}
	if _, err := log1.Append([]byte(`{"id":2}`)); err != nil {
		t.Fatalf("Append(1) error = %v", err)
	}
	if err := log1.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark(1) error = %v", err)
	}

	msg, found, err := engine.tryQueueRead(context.Background(), "orders", []int{0, 1}, 0, time.Second)
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
	engine := newTestEngine(t, ms, nil, nil)

	log1, err := engine.logs.Get("orders", 1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log1.Append([]byte(`{"id":99}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log1.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
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

func TestConsumeRotatesQueueScanStartAcrossLocalPartitions(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil)

	for partition := range 2 {
		log, err := engine.logs.Get("orders", partition)
		if err != nil {
			t.Fatalf("Get(%d) error = %v", partition, err)
		}
		for offset := range 2 {
			if _, err := log.Append(fmt.Appendf(nil, `{"partition":%d,"offset":%d}`, partition, offset)); err != nil {
				t.Fatalf("Append(%d,%d) error = %v", partition, offset, err)
			}
		}
		if err := log.AdvanceHighWatermark(2); err != nil {
			t.Fatalf("AdvanceHighWatermark(%d) error = %v", partition, err)
		}
	}

	first, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0})
	if err != nil {
		t.Fatalf("first Consume() error = %v", err)
	}
	if !found {
		t.Fatal("first Consume() found = false, want true")
	}
	if first.Partition != 0 {
		t.Fatalf("first Consume() partition = %d, want 0", first.Partition)
	}

	second, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0})
	if err != nil {
		t.Fatalf("second Consume() error = %v", err)
	}
	if !found {
		t.Fatal("second Consume() found = false, want true")
	}
	if second.Partition != 1 {
		t.Fatalf("second Consume() partition = %d, want 1", second.Partition)
	}
}

func TestConsumeReplayReturnsMessageForExistingOffset(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":7}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(0), Offset: new(int64(0))})
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
	engine := newTestEngine(t, ms, nil, nil)
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(0), Offset: new(int64(5))})
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
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	chans, err := engine.notifyChannels("orders", []int{0})
	if err != nil {
		t.Fatalf("notifyChannels() error = %v", err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- engine.waitForActivity(context.Background(), chans, time.Second)
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
	engine := newTestEngine(t, ms, nil, nil)

	err := engine.Ack(context.Background(), "orders", consumer.Handle{Partition: -1, Offset: 0, Nonce: 1})
	if !errors.Is(err, consumer.ErrHandleMalformed) {
		t.Fatalf("Ack() error = %v, want %v", err, consumer.ErrHandleMalformed)
	}
}

func TestAckRejectsUnreservedHandle(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil)
	handle := consumer.Handle{Partition: 0, Offset: 0, Nonce: 123}

	err := engine.Ack(context.Background(), "orders", handle)
	if !errors.Is(err, consumer.ErrHandleStale) {
		t.Fatalf("Ack() error = %v, want %v", err, consumer.ErrHandleStale)
	}
}

func TestConsumeReturnsTopicNotFoundForMissingTopic(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil)

	_, _, err := engine.Consume(context.Background(), "missing", ConsumeOpts{})
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrTopicNotFound)
	}
}

func TestAckReturnsTopicNotFoundForMissingTopic(t *testing.T) {
	engine := newTestEngine(t, newMessagingFakeMetastore(), nil, nil)
	handle := consumer.Handle{Partition: 0, Offset: 0, Nonce: 1}

	err := engine.Ack(context.Background(), "missing", handle)
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Ack() error = %v, want %v", err, ErrTopicNotFound)
	}
}
