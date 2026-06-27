package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

func TestCommitAcceptedProduceMakesRecordVisible(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}
	engine := newTestEngineWithIngress(t, t.TempDir(), ms, schema.NewAlwaysValid(), fixedPartitioner{picked: 0}, newTestIngressManager(t), "")

	offset, err := engine.CommitAcceptedProduce(context.Background(), ingress.ProduceRecord{
		Topic:           "orders",
		Key:             "customer-1",
		TargetPartition: 1,
		Payload:         []byte(`{"id":1}`),
		CreatedAtUnixMs: time.Now().Add(-time.Millisecond).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("CommitAcceptedProduce() error = %v", err)
	}
	if offset != 0 {
		t.Fatalf("CommitAcceptedProduce() offset = %d, want 0", offset)
	}

	partition := 1
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partition})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if msg.Topic != "orders" || msg.Partition != 1 || msg.Offset != 0 || string(msg.Payload) != `{"id":1}` {
		t.Fatalf("Consume() message = %+v", msg)
	}
}

func TestCommitAcceptedProduceBatchMakesRecordsVisible(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 1000}
	engine := newTestEngineWithIngress(t, t.TempDir(), ms, schema.NewAlwaysValid(), fixedPartitioner{picked: 0}, newTestIngressManager(t), "")

	offsets, err := engine.CommitAcceptedProduceBatch(context.Background(), []ingress.ProduceRecord{
		{
			Topic:           "orders",
			Key:             "customer-1",
			TargetPartition: 1,
			Payload:         []byte(`{"id":1}`),
			CreatedAtUnixMs: time.Now().Add(-time.Millisecond).UnixMilli(),
		},
		{
			Topic:           "orders",
			Key:             "customer-2",
			TargetPartition: 1,
			Payload:         []byte(`{"id":2}`),
			CreatedAtUnixMs: time.Now().Add(-time.Millisecond).UnixMilli(),
		},
	})
	if err != nil {
		t.Fatalf("CommitAcceptedProduceBatch() error = %v", err)
	}
	if len(offsets) != 2 || offsets[0] != 0 || offsets[1] != 1 {
		t.Fatalf("CommitAcceptedProduceBatch() offsets = %v, want [0 1]", offsets)
	}

	partition := 1
	wantPayloads := []string{`{"id":1}`, `{"id":2}`}
	for offset := int64(0); offset < 2; offset++ {
		msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partition, Offset: &offset})
		if err != nil {
			t.Fatalf("Consume(offset=%d) error = %v", offset, err)
		}
		if !found {
			t.Fatalf("Consume(offset=%d) found = false, want true", offset)
		}
		want := wantPayloads[offset]
		if msg.Topic != "orders" || msg.Partition != 1 || msg.Offset != offset || string(msg.Payload) != want {
			t.Fatalf("Consume(offset=%d) message = %+v, want payload %s", offset, msg, want)
		}
	}
}

func TestCommitAcceptedProduceRejectsRemotePartition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	engine := newTestEngineWithIngress(t, t.TempDir(), store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, newTestIngressManager(t), "node-self")
	_, err := engine.CommitAcceptedProduce(ctx, ingress.ProduceRecord{
		Topic:           "orders",
		TargetPartition: 1,
		Payload:         []byte(`{"id":1}`),
	})
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("CommitAcceptedProduce() error = %v, want %v", err, ErrNotPartitionOwner)
	}
}
