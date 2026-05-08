package broker

import (
	"log/slog"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
	"github.com/debanganthakuria/narad/internal/storage"
	"github.com/debanganthakuria/narad/internal/topic"
)

type Deps struct {
	DataDir     string
	LogOptions  storage.Options
	TopicPolicy TopicPolicy
	Metastore   metastore.Metastore
	Partitions  partition.Manager
	Schemas     schema.Registry
	Offsets     consumer.OffsetTracker
	Replicator  replication.Replicator
	Logger      *slog.Logger
}

// TopicPolicy supplies CreateTopic's defaults and bounds. Lives in the
// broker package so the broker stays decoupled from internal/config;
// serve.go translates config.TopicConfig → TopicPolicy at startup.
type TopicPolicy struct {
	DefaultPartitions        int
	MaxPartitions            int
	DefaultReplicationFactor int
	DefaultRetention         topic.Retention
}
