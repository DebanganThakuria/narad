package broker

import (
	"fmt"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/broker/topics"
)

// impl is the broker facade. It composes per-domain managers via Go
// type embedding; method promotion makes every embedded manager's
// methods land directly on *impl, satisfying the Broker interface
// without explicit forwarding code.
//
// The deps field is retained for any future broker-level methods
// that don't naturally belong to one of the embedded managers.
type impl struct {
	*topics.Manager
	*messaging.Engine
	*runtime.Snapshotter
	*runtime.Lifecycle

	deps Deps
}

// New constructs a Broker from the supplied dependencies. It
// validates required fields and TopicConfig bounds, then wires each
// sub-manager with the slice of dependencies it needs.
func New(d Deps) (Broker, error) {
	if d.DataDir == "" {
		return nil, fmt.Errorf("%w: data_dir empty", ErrInvalidArgument)
	}
	if d.Metastore == nil || d.Partitions == nil || d.Schemas == nil ||
		d.ConsumerOffsets == nil || d.Replicator == nil || d.Logger == nil {
		return nil, fmt.Errorf("%w: missing dependency", ErrInvalidArgument)
	}
	if len(d.HandleSecret) < 16 {
		return nil, fmt.Errorf("%w: HandleSecret must be at least 16 bytes", ErrInvalidArgument)
	}
	if d.TopicConfig.DefaultPartitions <= 0 {
		return nil, fmt.Errorf("%w: TopicConfig.DefaultPartitions must be > 0", ErrInvalidArgument)
	}
	if d.TopicConfig.DefaultReplicationFactor <= 0 {
		return nil, fmt.Errorf("%w: TopicConfig.DefaultReplicationFactor must be > 0", ErrInvalidArgument)
	}
	if d.TopicConfig.DefaultMaxInFlightPerPartition <= 0 {
		return nil, fmt.Errorf("%w: TopicConfig.DefaultMaxInFlightPerPartition must be > 0", ErrInvalidArgument)
	}
	if d.TopicConfig.DefaultMaxAckedAheadPerPartition <= 0 {
		return nil, fmt.Errorf("%w: TopicConfig.DefaultMaxAckedAheadPerPartition must be > 0", ErrInvalidArgument)
	}

	logs := runtime.NewLogs(d.DataDir, d.StorageOptions, d.Metastore, d.Metrics)

	topicCfg := topics.Config{
		DefaultPartitions:                d.TopicConfig.DefaultPartitions,
		MaxPartitions:                    d.TopicConfig.MaxPartitions,
		DefaultReplicationFactor:         d.TopicConfig.DefaultReplicationFactor,
		DefaultRetentionMs:               d.TopicConfig.DefaultRetentionMs,
		DefaultVisibilityTimeoutMs:       d.TopicConfig.DefaultVisibilityTimeoutMs,
		DefaultMaxInFlightPerPartition:   d.TopicConfig.DefaultMaxInFlightPerPartition,
		DefaultMaxAckedAheadPerPartition: d.TopicConfig.DefaultMaxAckedAheadPerPartition,
	}

	return &impl{
		Manager:     topics.NewManager(d.DataDir, d.Metastore, d.Schemas, d.ConsumerOffsets, logs, topicCfg, d.Logger),
		Engine:      messaging.NewEngine(d.Metastore, d.Schemas, d.Partitions, d.Replicator, d.ConsumerOffsets, logs, d.Metrics, d.HandleSecret, d.Logger),
		Snapshotter: runtime.NewSnapshotter(d.Metastore, d.ConsumerOffsets, logs, d.Logger),
		Lifecycle:   runtime.NewLifecycle(logs),
		deps:        d,
	}, nil
}
