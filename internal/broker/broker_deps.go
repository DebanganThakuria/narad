package broker

import (
	"log/slog"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/observability/metrics"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
	"github.com/debanganthakuria/narad/internal/storage"
)

type Deps struct {
	DataDir         string
	StorageOptions  storage.Options
	TopicConfig     TopicConfig
	Metastore       metastore.Metastore
	Partitions      partition.Manager
	Schemas         schema.Registry
	ConsumerOffsets *consumer.InFlight
	Replicator      replication.Replicator
	Logger          *slog.Logger

	// Metrics is optional. When nil, instrumentation short-circuits to
	// noops. Tests typically pass nil; serve.go wires a real instance.
	Metrics *metrics.Metrics
}

// TopicConfig supplies CreateTopic's defaults and bounds. Lives in the
// broker package so the broker stays decoupled from internal/config;
// serve.go translates config.TopicConfig → TopicConfig at startup.
type TopicConfig struct {
	DefaultPartitions          int
	MaxPartitions              int
	DefaultReplicationFactor   int
	DefaultRetentionMs         int64
	DefaultVisibilityTimeoutMs int64
}
