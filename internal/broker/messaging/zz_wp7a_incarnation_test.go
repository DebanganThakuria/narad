package messaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// produce-accept#0, owner side. A record accepted for one incarnation
// of a topic is not committed into another incarnation of the same
// name. Records without an incarnation (accepted before they were
// stamped) still commit by name.
func TestZZWP7aCommitRefusesOtherIncarnation(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "inc-2", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 2, "stale")); err == nil {
		t.Fatal("records of incarnation inc-1 were committed into inc-2")
	}
	if _, err := e.CommitAcceptedProduce(ctx, zzWP7aRecords("orders", "inc-1", 0, 1, "stale")[0]); err == nil {
		t.Fatal("a record of incarnation inc-1 was committed into inc-2")
	}
	mixed := append(zzWP7aRecords("orders", "inc-2", 0, 1, "live"), zzWP7aRecords("orders", "inc-1", 0, 1, "stale")...)
	if _, err := e.CommitAcceptedProduceBatch(ctx, mixed); err == nil {
		t.Fatal("a batch holding a record of incarnation inc-1 was committed into inc-2")
	}
	if got := zzWP7aHWM(t, e, "orders", 0); got != 0 {
		t.Fatalf("high-watermark = %d after refused commits, want 0", got)
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-2", 0, 2, "live")); err != nil {
		t.Fatalf("records of the live incarnation: %v", err)
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "legacy")); err != nil {
		t.Fatalf("records without an incarnation: %v", err)
	}
	if got := zzWP7aHWM(t, e, "orders", 0); got != 3 {
		t.Fatalf("high-watermark = %d, want 3", got)
	}
}

func TestZZWP7aIncarnationMismatchIsTyped(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: "inc-2", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	_, err := e.CommitAcceptedProduceBatch(context.Background(), zzWP7aRecords("orders", "inc-1", 0, 1, "stale"))
	if !errors.Is(err, ErrTopicIncarnationMismatch) {
		t.Fatalf("err = %v, want ErrTopicIncarnationMismatch", err)
	}
}

// The incarnation is checked again under the produce lock: a topic
// deleted and recreated while a commit waited for the lock does not
// receive the old incarnation's records.
func TestZZWP7aCommitRechecksIncarnationUnderProduceLock(t *testing.T) {
	ms := &zzWP7aLockedMetastore{messagingFakeMetastore: newMessagingFakeMetastore()}
	if err := ms.CreateTopic(context.Background(), topic.Topic{Name: "orders", ID: "inc-1", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	e := zzWP7aEngineOver(t, ms)
	ctx := context.Background()
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 1, "first")); err != nil {
		t.Fatal(err)
	}
	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	out := make(chan error, 1)
	go func() {
		_, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 2, "late"))
		out <- err
	}()
	zzWP7aWaitStack(t, "the commit to queue on the produce lock", func(count func(string) int) bool {
		return count("(*Logs).lockProduce(") >= 1
	})
	if err := ms.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "inc-2", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-out; err == nil {
		t.Fatal("records of incarnation inc-1 were committed after the topic was recreated as inc-2")
	}
	if got := zzWP7aHWM(t, e, "orders", 0); got != 0 {
		t.Fatalf("the recreated topic's high-watermark = %d, want 0", got)
	}
}

// zzWP7aLockedMetastore guards the fake metastore with a lock, for a
// test that changes a topic while an engine goroutine reads it.
type zzWP7aLockedMetastore struct {
	mu sync.RWMutex
	*messagingFakeMetastore
}

func (m *zzWP7aLockedMetastore) CreateTopic(ctx context.Context, t topic.Topic) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messagingFakeMetastore.CreateTopic(ctx, t)
}

func (m *zzWP7aLockedMetastore) DeleteTopic(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messagingFakeMetastore.DeleteTopic(ctx, name)
}

func (m *zzWP7aLockedMetastore) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	m.mu.Lock() // GetTopic counts its calls
	defer m.mu.Unlock()
	return m.messagingFakeMetastore.GetTopic(ctx, name)
}

func (m *zzWP7aLockedMetastore) TopicVersion(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messagingFakeMetastore.TopicVersion(name)
}

func (m *zzWP7aLockedMetastore) AssignmentVersion(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messagingFakeMetastore.AssignmentVersion(name)
}

func (m *zzWP7aLockedMetastore) SchemaVersion(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messagingFakeMetastore.SchemaVersion(name)
}

func (m *zzWP7aLockedMetastore) MetadataVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.messagingFakeMetastore.MetadataVersion()
}

// zzWP7aEngineOver is newTestEngine over any metastore.
func zzWP7aEngineOver(t *testing.T, ms metastore.Metastore) *Engine {
	t.Helper()
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	return e
}
