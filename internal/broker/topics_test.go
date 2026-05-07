package broker_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
	"github.com/debanganthakuria/narad/internal/storage"
)

// newTestBroker constructs a broker backed by a per-test temp data dir
// and the standard set of single-node implementations.
func newTestBroker(t *testing.T, policy broker.TopicPolicy) (broker.Broker, string) {
	t.Helper()
	dir := t.TempDir()
	ms, err := metastore.NewJSONFileStore(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	br, err := broker.New(broker.Deps{
		DataDir:     dir,
		LogOptions:  storage.DefaultOptions(),
		TopicPolicy: policy,
		Metastore:   ms,
		Partitions:  partition.NewHashRoundRobin(),
		Schemas:     schema.NewAlwaysValid(),
		Offsets:     consumer.NewMetastoreBacked(ms),
		Replicator:  replication.NewLocal(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	t.Cleanup(func() { _ = br.Close() })
	return br, dir
}

// Default policy used by tests that aren't exercising bounds. Mirrors
// the production defaults from internal/config.
var defaultPolicy = broker.TopicPolicy{
	DefaultPartitions:        8,
	MaxPartitions:            1024,
	DefaultReplicationFactor: 1,
}

func TestCreateTopicAppliesDefaultPartitions(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	// partitions = 0 → use default (8).
	got, err := br.CreateTopic(ctx, "orders", 0, 0)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if got.Partitions != 8 {
		t.Fatalf("partitions: got %d want 8", got.Partitions)
	}
	if got.ReplicationFactor != 1 {
		t.Fatalf("replication_factor: got %d want 1", got.ReplicationFactor)
	}
}

func TestCreateTopicRespectsExplicitValues(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	got, err := br.CreateTopic(ctx, "orders", 16, 1)
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if got.Partitions != 16 {
		t.Fatalf("partitions: got %d want 16", got.Partitions)
	}
}

func TestCreateTopicRejectsAboveMax(t *testing.T) {
	br, _ := newTestBroker(t, broker.TopicPolicy{
		DefaultPartitions:        4,
		MaxPartitions:            8,
		DefaultReplicationFactor: 1,
	})
	ctx := context.Background()

	_, err := br.CreateTopic(ctx, "too-big", 9, 1)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
}

func TestCreateTopicRejectsNegativePartitions(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	_, err := br.CreateTopic(ctx, "neg", -1, 1)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
}

func TestIncreaseTopicPartitionsHappyPath(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	updated, err := br.IncreaseTopicPartitions(ctx, "orders", 16)
	if err != nil {
		t.Fatalf("IncreaseTopicPartitions: %v", err)
	}
	if updated.Partitions != 16 {
		t.Fatalf("returned topic has %d partitions, want 16", updated.Partitions)
	}
	got, err := br.GetTopic(ctx, "orders")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.Partitions != 16 {
		t.Fatalf("GetTopic after increase: %d, want 16", got.Partitions)
	}
}

func TestIncreaseTopicPartitionsRejectsDecrease(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "t", 16, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err := br.IncreaseTopicPartitions(ctx, "t", 8)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument for decrease, got %v", err)
	}
}

func TestIncreaseTopicPartitionsRejectsEqual(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "t", 8, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err := br.IncreaseTopicPartitions(ctx, "t", 8)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument for equal, got %v", err)
	}
}

func TestIncreaseTopicPartitionsRejectsAboveMax(t *testing.T) {
	br, _ := newTestBroker(t, broker.TopicPolicy{
		DefaultPartitions:        4,
		MaxPartitions:            16,
		DefaultReplicationFactor: 1,
	})
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "t", 4, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	_, err := br.IncreaseTopicPartitions(ctx, "t", 32)
	if !errors.Is(err, broker.ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument above max, got %v", err)
	}
}

func TestIncreaseTopicPartitionsTopicNotFound(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	_, err := br.IncreaseTopicPartitions(ctx, "missing", 16)
	if !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("want ErrTopicNotFound, got %v", err)
	}
}

func TestDeleteTopicWipesEverything(t *testing.T) {
	br, dir := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, _, err := br.Produce(ctx, "orders", "k1", []byte(`{"x":1}`)); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if err := br.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}

	if _, err := br.GetTopic(ctx, "orders"); !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("GetTopic after delete: want ErrTopicNotFound, got %v", err)
	}

	topicDir := filepath.Join(dir, "topics", "orders")
	if _, err := os.Stat(topicDir); !os.IsNotExist(err) {
		t.Fatalf("topic directory still present: %v", err)
	}
}

func TestDeleteTopicNotFound(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if err := br.DeleteTopic(ctx, "missing"); !errors.Is(err, broker.ErrTopicNotFound) {
		t.Fatalf("want ErrTopicNotFound, got %v", err)
	}
}

func TestGetTopicDetailsReportsStats(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "orders", 4, 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Produce 3 records on the same key so they all land on the same
	// partition (HashRoundRobin keyed mode is deterministic).
	for i := 0; i < 3; i++ {
		if _, _, err := br.Produce(ctx, "orders", "fixed-key", []byte(`{"v":1}`)); err != nil {
			t.Fatalf("Produce %d: %v", i, err)
		}
	}

	d, err := br.GetTopicDetails(ctx, "orders")
	if err != nil {
		t.Fatalf("GetTopicDetails: %v", err)
	}
	if d.Name != "orders" {
		t.Fatalf("topic name: %q", d.Name)
	}
	if len(d.Partitions) != 4 {
		t.Fatalf("partition stats: %d (want 4)", len(d.Partitions))
	}
	// Sum of NextOffset across partitions == total produced.
	var sum int64
	for _, p := range d.Partitions {
		sum += p.NextOffset
	}
	if sum != 3 {
		t.Fatalf("sum of NextOffset = %d, want 3", sum)
	}
}

func TestMultipleTopicsAreIndependent(t *testing.T) {
	br, _ := newTestBroker(t, defaultPolicy)
	ctx := context.Background()

	if _, err := br.CreateTopic(ctx, "a", 4, 1); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := br.CreateTopic(ctx, "b", 0, 0); err != nil {
		t.Fatalf("create b: %v", err)
	}

	a, err := br.GetTopic(ctx, "a")
	if err != nil {
		t.Fatalf("get a: %v", err)
	}
	b, err := br.GetTopic(ctx, "b")
	if err != nil {
		t.Fatalf("get b: %v", err)
	}
	if a.Partitions != 4 || b.Partitions != 8 {
		t.Fatalf("partitions: a=%d b=%d (want 4 / 8)", a.Partitions, b.Partitions)
	}

	// Producing to one topic doesn't affect the other's offset space.
	off1, _, err := br.Produce(ctx, "a", "k1", []byte(`{"x":1}`))
	if err != nil {
		t.Fatalf("produce a: %v", err)
	}
	off2, _, err := br.Produce(ctx, "b", "k1", []byte(`{"x":2}`))
	if err != nil {
		t.Fatalf("produce b: %v", err)
	}
	if off1 != 0 || off2 != 0 {
		t.Fatalf("each topic's first record should be offset 0; got a=%d b=%d", off1, off2)
	}
}
