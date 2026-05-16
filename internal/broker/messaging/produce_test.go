package messaging

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/replication"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

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
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 3, ReplicationFactor: 2}); err == nil {
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

func TestProduceSkipsDeadOwnerPartition(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3, ReplicationFactor: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember(dead) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-follower", Addr: "follower.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(follower) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-alive", Addr: "alive.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(alive) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-dead", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-alive", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-alive", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(2) error = %v", err)
	}

	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	engine := NewEngine(
		store,
		schema.NewAlwaysValid(),
		fixedPartitionManager{picked: 0},
		replication.NewLocal(),
		offsets,
		logs,
		nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	offset, partitionIdx, err := engine.Produce(ctx, "orders", "customer-1", []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if offset != 0 {
		t.Fatalf("offset = %d, want 0", offset)
	}
	if partitionIdx != 1 {
		t.Fatalf("partition = %d, want 1", partitionIdx)
	}
}
