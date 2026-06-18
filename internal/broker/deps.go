package broker

import (
	"log/slog"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/replication"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Deps is the bag of collaborators the broker facade hands to each
// sub-manager at construction time. Every field is required unless
// noted otherwise.
type Deps struct {
	DataDir         string
	StorageOptions  storage.Options
	TopicConfig     TopicConfig
	Metastore       metastore.Metastore
	Partitions      partition.Manager
	Schemas         schema.Registry
	ConsumerOffsets *consumer.InFlight
	Replicator      replication.Replicator
	Logs            *runtime.Logs
	Logger          *slog.Logger
	SelfID          string
	Lifecycle       *runtime.Lifecycle

	// MaxConsumeWait caps how long a long-poll consume can block.
	// Plumbed through to the messaging engine.
	MaxConsumeWait int64 // milliseconds; 0 → no cap

	// Metrics is optional. When nil, instrumentation short-circuits
	// to noops. Tests typically pass nil; serve.go wires a real
	// instance.
	Metrics *metrics.Metrics
}

// TopicConfig supplies CreateTopic's defaults and bounds. Lives in
// the broker package so callers stay decoupled from
// internal/platform/config; serve.go translates config.TopicConfig →
// TopicConfig at startup.
type TopicConfig struct {
	DefaultPartitions                int
	MaxPartitions                    int
	DefaultReplicationFactor         int
	DefaultRetentionMs               int64
	DefaultVisibilityTimeoutMs       int64
	DefaultMaxInFlightPerPartition   int64
	DefaultMaxAckedAheadPerPartition int64
}

// TopicPolicy is a convenience configuration bag for tests and
// integration harnesses that want to override topic defaults without
// dealing with raw millisecond fields.
type TopicPolicy struct {
	DefaultPartitions        int
	MaxPartitions            int
	DefaultReplicationFactor int
	DefaultRetentionMs       int64
}
