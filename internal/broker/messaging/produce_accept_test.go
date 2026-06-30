package messaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

func TestAcceptProducePersistsToIngressWAL(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	ingressManager := newTestIngressManager(t)
	engine := newTestEngineWithIngress(t, t.TempDir(), ms, schema.NewAlwaysValid(), fixedPartitioner{picked: 2}, ingressManager, "")

	accepted, err := engine.AcceptProduce(context.Background(), "orders", "customer-1", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	if accepted.Topic != "orders" || accepted.TargetPartition != 2 {
		t.Fatalf("accepted = %+v, want topic orders partition 2", accepted)
	}

	var records []ingress.ProduceRecord
	if err := ingressManager.ReplayProduce(0, func(record ingress.ProduceRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduce() error = %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("replayed records = %d, want 1", len(records))
	}
	if records[0].Topic != "orders" ||
		records[0].Key != "customer-1" ||
		records[0].TargetPartition != 2 ||
		string(records[0].Payload) != `{"id":1}` {
		t.Fatalf("replayed record = %+v", records[0])
	}
}

func TestAcceptProduceAllowsRemoteTargetPartition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
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

	ingressManager := newTestIngressManager(t)
	engine := newTestEngineWithIngress(t, t.TempDir(), store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 1}, ingressManager, "node-self")

	if _, _, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`)); !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("Produce() error = %v, want %v", err, ErrNotPartitionOwner)
	}

	accepted, err := engine.AcceptProduce(ctx, "orders", "customer-1", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	if accepted.TargetPartition != 1 {
		t.Fatalf("accepted partition = %d, want 1", accepted.TargetPartition)
	}
}

func TestAcceptProduceUsesPinnedPartitionWithoutOwnershipCheck(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	ingressManager := newTestIngressManager(t)
	engine := newTestEngineWithIngress(t, t.TempDir(), ms, schema.NewAlwaysValid(), fixedPartitioner{picked: 0}, ingressManager, "")

	accepted, err := engine.AcceptProduce(context.Background(), "orders", "customer-1", []byte(`{"id":1}`), 2)
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	if accepted.TargetPartition != 2 {
		t.Fatalf("accepted partition = %d, want 2", accepted.TargetPartition)
	}
}

func TestAcceptProduceRejectsInvalidPinnedPartition(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2}
	ingressManager := newTestIngressManager(t)
	engine := newTestEngineWithIngress(t, t.TempDir(), ms, schema.NewAlwaysValid(), fixedPartitioner{picked: 0}, ingressManager, "")

	_, err := engine.AcceptProduce(context.Background(), "orders", "customer-1", []byte(`{"id":1}`), 2)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("AcceptProduce() error = %v, want %v", err, ErrInvalid)
	}
}

func newTestIngressManager(t *testing.T) *ingress.Manager {
	t.Helper()
	manager, err := ingress.OpenManager(t.TempDir(), acceptProduceWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func acceptProduceWALOptions() wal.Options {
	return wal.Options{
		SegmentBytes: 1024,
		SyncInterval: time.Hour,
		MaxRecord:    1024,
	}
}

func newTestEngineWithIngress(
	t *testing.T,
	dataDir string,
	ms metastore.Metastore,
	schemas schema.Registry,
	partitioner partition.Manager,
	ingressManager *ingress.Manager,
	selfID string,
) *Engine {
	t.Helper()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	return NewEngine(
		ms,
		schemas,
		partitioner,
		offsets,
		logs,
		ingressManager,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		selfID,
	)
}
