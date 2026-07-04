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
)

func TestConsumePinnedRejectsWhenNotPartitionOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-other", Addr: "other.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(other) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-other"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Consume(ctx, "orders", ConsumeOpts{Partition: new(0), Wait: 0})
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrNotPartitionOwner)
	}
}

func TestConsumeReplayRejectsWhenNotPartitionOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-other", Addr: "other.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(other) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-other"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Consume(ctx, "orders", ConsumeOpts{Partition: new(0), Offset: new(int64(0))})
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrNotPartitionOwner)
	}
}

func TestConsumePinnedRejectsWhenAssignmentMissing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Consume(ctx, "orders", ConsumeOpts{Partition: new(0), Wait: 0})
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrNotPartitionOwner)
	}
}

func TestConsumeQueueRejectsWhenNoAssignmentsExist(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Consume(ctx, "orders", ConsumeOpts{Wait: 0})
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Consume() error = %v, want %v", err, ErrNotPartitionOwner)
	}
}

func TestConsumePinnedAllowsOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	if _, _, err := engine.Produce(ctx, "orders", "", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	msg, found, err := engine.Consume(ctx, "orders", ConsumeOpts{Partition: new(0), Wait: 0})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if msg.Partition != 0 || msg.Offset != 0 {
		t.Fatalf("Consume() message = %+v, want partition 0 offset 0", msg)
	}
}

// failingAssignmentsMetastore implements assignmentReader with a failing
// ListAssignments, simulating a metastore hiccup during ownership lookup.
type failingAssignmentsMetastore struct {
	*messagingFakeMetastore
	listErr error
}

func (f *failingAssignmentsMetastore) ListAssignments(string) ([]metastore.Assignment, error) {
	return nil, f.listErr
}

func (f *failingAssignmentsMetastore) GetAssignment(string, int) (metastore.Assignment, error) {
	return metastore.Assignment{}, errs.ErrNotFound
}

// A ListAssignments failure is a retryable internal error, not proof that
// this node owns nothing — it must not surface as ErrNotPartitionOwner
// (which clients treat as a routing verdict).
func TestConsumeQueueSurfacesAssignmentListError(t *testing.T) {
	inner := newMessagingFakeMetastore()
	inner.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	listErr := errors.New("raft leadership lost")
	ms := &failingAssignmentsMetastore{messagingFakeMetastore: inner, listErr: listErr}

	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	engine := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")

	_, _, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0})
	if errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Consume() error = %v, want the underlying assignment error, not %v", err, ErrNotPartitionOwner)
	}
	if !errors.Is(err, listErr) {
		t.Fatalf("Consume() error = %v, want wrapped %v", err, listErr)
	}
}

func TestConsumePinnedAllowsWhenOwnershipUnavailable(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil)
	engine.selfID = "node-self"
	if _, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: new(0), Wait: 0})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if msg.Offset != 0 {
		t.Fatalf("Consume() offset = %d, want 0", msg.Offset)
	}
}
