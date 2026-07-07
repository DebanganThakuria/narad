package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

func TestBuildBrokerRejectsNonStoreMetastore(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeMS := stubMetastore{}

	if _, err := buildBroker(cfg, "node-1", fakeMS, schema.NewAlwaysValid(), m, log); err == nil {
		t.Fatal("buildBroker() error = nil, want error")
	}
}

func TestBuildBrokerReturnsLogs(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := metastore.New(metastore.Config{NodeID: "node-1", DataDir: filepath.Join(t.TempDir(), "metastore"), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()

	bc, err := buildBroker(cfg, "node-1", store, schema.NewJSONSchema(), m, log)
	if err != nil {
		t.Fatalf("buildBroker() error = %v", err)
	}
	if bc.broker == nil || bc.logs == nil {
		t.Fatalf("buildBroker() = (%v, %v), want non-nil", bc.broker, bc.logs)
	}
	if bc.createGate == nil {
		t.Fatal("buildBroker() createGate = nil, want the broker's create gate handle")
	}
}

// stubMetastore satisfies metastore.Metastore without being a
// *metastore.Store, exercising buildBroker's type requirement.
type stubMetastore struct{}

func (stubMetastore) CreateTopic(context.Context, topic.Topic) error { return nil }
func (stubMetastore) UpdateTopic(context.Context, topic.Topic) error { return nil }
func (stubMetastore) DeleteTopic(context.Context, string) error      { return nil }
func (stubMetastore) GetTopic(context.Context, string) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubMetastore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}
func (stubMetastore) AttachChild(context.Context, string, string) error      { return nil }
func (stubMetastore) DetachChild(context.Context, string, string) error      { return nil }
func (stubMetastore) PutSchema(context.Context, string, int, []byte) error   { return nil }
func (stubMetastore) GetSchema(context.Context, string, int) ([]byte, error) { return nil, nil }
func (stubMetastore) LeaderAddr() string                                     { return "" }
func (stubMetastore) GetMember(string) (metastore.Member, error) {
	return metastore.Member{}, metastore.ErrNotFound
}
func (stubMetastore) Close() error { return nil }

func TestInitializeConsumerOffsetsRestoresOnlyOwnedPartitions(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()
	waitForLeadership(t, store)
	if err := store.CreateTopic(ctx, topic.Topic{
		Name:                      "orders",
		Partitions:                2,
		VisibilityTimeoutMs:       1000,
		MaxInFlightPerPartition:   16,
		MaxAckedAheadPerPartition: 16,
	}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-1"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-2"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
	for partition, offset := range map[int]int64{0: 3, 1: 9} {
		partitionDir := storage.TopicPartitionDir(dataDir, "orders", partition)
		if err := storage.WriteConsumerOffset(partitionDir, offset); err != nil {
			t.Fatalf("WriteConsumerOffset(%d) error = %v", partition, err)
		}
	}
	inFlight := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 16, MaxAckedAhead: 16}, nil
	}, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if err := initializeConsumerOffsets(ctx, dataDir, store, inFlight, logger, "node-1"); err != nil {
		t.Fatalf("initializeConsumerOffsets() error = %v", err)
	}
	if got := inFlight.Next("orders", 0); got != 4 {
		t.Fatalf("Next(orders,0) = %d, want 4", got)
	}
	if got := inFlight.Next("orders", 1); got != 0 {
		t.Fatalf("Next(orders,1) = %d, want 0", got)
	}
}

func TestInitializeSchemasLoadsPersistedSchemas(t *testing.T) {
	ctx := context.Background()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()
	waitForLeadership(t, store)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	raw := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	if err := store.PutSchema(ctx, "orders", 1, raw); err != nil {
		t.Fatalf("PutSchema() error = %v", err)
	}

	registry := schema.NewJSONSchema()
	if err := initializeSchemas(ctx, store, registry); err != nil {
		t.Fatalf("initializeSchemas() error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{"id":"o_123"}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestInitializeSchemasSkipsTopicsWithoutSchemas(t *testing.T) {
	ctx := context.Background()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()
	waitForLeadership(t, store)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	registry := schema.NewJSONSchema()
	if err := initializeSchemas(ctx, store, registry); err != nil {
		t.Fatalf("initializeSchemas() error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{"id":"o_123"}`)); !errors.Is(err, schema.ErrSchemaNotFound) {
		t.Fatalf("Validate() error = %v, want %v", err, schema.ErrSchemaNotFound)
	}
}

func TestBuildMetricsReturnsUsableRegistry(t *testing.T) {
	reg, m := buildMetrics()
	if reg == nil || m == nil {
		t.Fatal("buildMetrics() returned nil values")
	}
	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(metricFamilies) == 0 {
		t.Fatal("Gather() returned no metric families")
	}
}

func TestCloseWithLogDoesNothingOnNilError(t *testing.T) {
	closeWithLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "metastore", func() error { return nil })
}

func TestCloseWithLogHandlesError(t *testing.T) {
	closeWithLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "broker", func() error {
		return errors.New("close failed")
	})
}

func TestBuildAPIServerPanicsWithoutBroker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("buildAPIServer() did not panic")
		}
	}()

	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	_ = buildAPIServer(context.Background(), cfg, nil, nil, nil, nil, m, reg, nil, log)
}

func TestBuildAPIServerReturnsServer(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	broker := stubBroker{}
	srv := buildAPIServer(context.Background(), cfg, broker, nil, nil, nil, m, reg, nil, log)
	if srv == nil {
		t.Fatal("buildAPIServer() returned nil")
	}
}

// stubBroker is a no-op broker.Broker for wiring tests.
type stubBroker struct{}

func (stubBroker) CreateTopic(context.Context, brokertopics.CreateOpts) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) IncreaseTopicPartitions(context.Context, string, int) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) UpdateTopicRetention(context.Context, string, int64) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) UpdateTopicCaps(context.Context, string, int64, int64) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) UpdateTopicSchema(context.Context, string, []byte) (topic.Topic, error) {
	return topic.Topic{}, nil
}
func (stubBroker) DeleteTopic(context.Context, string) error             { return nil }
func (stubBroker) PurgeTopic(context.Context, string) error              { return nil }
func (stubBroker) GetTopic(context.Context, string) (topic.Topic, error) { return topic.Topic{}, nil }
func (stubBroker) GetTopicDetails(context.Context, string) (topic.Details, error) {
	return topic.Details{}, nil
}

func (stubBroker) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}

func (stubBroker) Produce(context.Context, string, string, []byte, ...int) (int64, int, error) {
	return 0, 0, nil
}

func (stubBroker) AcceptProduce(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
	return ingress.AcceptedProduce{}, nil
}

func (stubBroker) CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error) {
	return 0, nil
}

func (stubBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	return make([]int64, len(records)), nil
}

func (stubBroker) Consume(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return topic.Message{}, false, nil
}
func (stubBroker) Ack(context.Context, string, consumer.Handle) error        { return nil }
func (stubBroker) Snapshot(context.Context) ([]metrics.TopicSnapshot, error) { return nil, nil }
func (stubBroker) Ready(context.Context) error                               { return nil }
func (stubBroker) Close() error                                              { return nil }
