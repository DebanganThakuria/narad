package messaging

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-other", ""); err != nil {
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-other", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	engine := newClusterTestEngine(t, store, fixedPartitionManager{picked: 0})
	_, _, err := engine.Consume(ctx, "orders", ConsumeOpts{Partition: new(0), Offset: new(int64(0))})
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-self", ""); err != nil {
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

func TestConsumePinnedAllowsWhenOwnershipUnavailable(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)
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
