package topics

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// CreateTopic registers a new topic and prepares its on-disk
// directory. Partition log files are opened lazily on first use.
//
// Zero values for any policy field inherit the matching default from
// Config. Negative values and partitions exceeding Config.MaxPartitions
// are rejected.
func (m *Manager) CreateTopic(ctx context.Context, opts CreateOpts) (topic.Topic, error) {
	if opts.Name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalid)
	}

	partitions := opts.Partitions
	if partitions == 0 {
		partitions = m.cfg.DefaultPartitions
	}
	if partitions < 0 {
		return topic.Topic{}, fmt.Errorf("%w: partitions must be >= 0 (0 = use default)", ErrInvalid)
	}
	if maximum := m.cfg.MaxPartitions; maximum > 0 && partitions > maximum {
		return topic.Topic{}, fmt.Errorf("%w: partitions (%d) exceeds topic.max_partitions (%d)",
			ErrInvalid, partitions, maximum)
	}

	rf := opts.ReplicationFactor
	if rf == 0 {
		rf = m.cfg.DefaultReplicationFactor
	}
	if rf < 2 {
		return topic.Topic{}, fmt.Errorf("%w: replication_factor must be >= 2 (0 = use default)", ErrInvalid)
	}

	retentionMs := opts.RetentionMs
	if retentionMs == 0 {
		retentionMs = m.cfg.DefaultRetentionMs
	}
	if retentionMs < 0 {
		return topic.Topic{}, fmt.Errorf("%w: retention_ms must be >= 0 (0 = use default)", ErrInvalid)
	}

	visibilityMs := opts.VisibilityTimeoutMs
	if visibilityMs == 0 {
		visibilityMs = m.cfg.DefaultVisibilityTimeoutMs
	}
	if visibilityMs < 0 {
		return topic.Topic{}, fmt.Errorf("%w: visibility_timeout_ms must be >= 0 (0 = use default)", ErrInvalid)
	}

	maxIF := opts.MaxInFlightPerPartition
	if maxIF == 0 {
		maxIF = m.cfg.DefaultMaxInFlightPerPartition
	}
	if maxIF < 0 {
		return topic.Topic{}, fmt.Errorf("%w: max_in_flight_per_partition must be >= 0 (0 = use default)", ErrInvalid)
	}

	maxAA := opts.MaxAckedAheadPerPartition
	if maxAA == 0 {
		maxAA = m.cfg.DefaultMaxAckedAheadPerPartition
	}
	if maxAA < 0 {
		return topic.Topic{}, fmt.Errorf("%w: max_acked_ahead_per_partition must be >= 0 (0 = use default)", ErrInvalid)
	}

	t := topic.Topic{
		Name:                      opts.Name,
		Partitions:                partitions,
		ReplicationFactor:         rf,
		RetentionMs:               retentionMs,
		VisibilityTimeoutMs:       visibilityMs,
		MaxInFlightPerPartition:   maxIF,
		MaxAckedAheadPerPartition: maxAA,
		CreatedAt:                 time.Now().Unix(),
	}

	dir := filepath.Join(m.dataDir, "topics", opts.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return topic.Topic{}, fmt.Errorf("topics: create topic dir: %w", err)
	}

	if err := m.metastore.CreateTopic(ctx, t); err != nil {
		if errors.Is(err, metastore.ErrAlreadyExists) {
			return topic.Topic{}, ErrAlreadyExists
		}
		return topic.Topic{}, err
	}

	m.logger.Info("topic created",
		"topic", opts.Name,
		"partitions", partitions,
		"replication_factor", rf,
		"retention_ms", retentionMs,
		"visibility_timeout_ms", visibilityMs,
		"max_in_flight_per_partition", maxIF,
		"max_acked_ahead_per_partition", maxAA)

	return t, nil
}
