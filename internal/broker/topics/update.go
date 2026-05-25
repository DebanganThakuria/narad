package topics

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// IncreaseTopicPartitions raises the partition count of an existing
// topic. Increase-only — decreasing would require renumbering offsets,
// which we don't support.
//
// Caller-visible side effect: future records' partition assignment
// uses hash(key) % newPartitions, so a key that previously hashed to
// partition 3 may now hash to partition 11. Existing records stay in
// their original partitions.

// TODO need to assign new partitions to cluster memebers and also assign replicas
func (m *Manager) IncreaseTopicPartitions(ctx context.Context, name string, newPartitions int) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalid)
	}
	if newPartitions <= 0 {
		return topic.Topic{}, fmt.Errorf("%w: partitions must be > 0", ErrInvalid)
	}
	if maximum := m.cfg.MaxPartitions; maximum > 0 && newPartitions > maximum {
		return topic.Topic{}, fmt.Errorf("%w: partitions (%d) exceeds topic.max_partitions (%d)",
			ErrInvalid, newPartitions, maximum)
	}

	current, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}
	if newPartitions <= current.Partitions {
		return topic.Topic{}, fmt.Errorf("%w: new partition count (%d) must be greater than current (%d); decrease is not supported",
			ErrInvalid, newPartitions, current.Partitions)
	}

	updated := current
	updated.Partitions = newPartitions

	if err = m.metastore.UpdateTopic(ctx, updated); err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrNotFound
		}
		return topic.Topic{}, err
	}

	m.logger.Info("topic partitions increased",
		"topic", name,
		"old_partitions", current.Partitions,
		"new_partitions", newPartitions)

	return updated, nil
}

// UpdateTopicRetention changes the retention policy of an existing
// topic. Cached partition logs are closed so the next access reopens
// them with the new bounds.
//
// retentionMs == 0 inherits Config.DefaultRetentionMs; negative values
// are rejected.
func (m *Manager) UpdateTopicRetention(ctx context.Context, name string, retentionMs int64) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalid)
	}
	if retentionMs < 0 {
		return topic.Topic{}, fmt.Errorf("%w: retention_ms must be >= 0 (0 = use default)", ErrInvalid)
	}
	if retentionMs == 0 {
		retentionMs = m.cfg.DefaultRetentionMs
	}

	current, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}

	updated := current
	updated.RetentionMs = retentionMs

	if err := m.metastore.UpdateTopic(ctx, updated); err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrNotFound
		}
		return topic.Topic{}, err
	}

	if firstCloseErr := m.logs.CloseTopic(name); firstCloseErr != nil {
		m.logger.Error("update retention: close cached logs", "topic", name, "err", firstCloseErr)
		return updated, fmt.Errorf("topics: close partition logs after retention update: %w", firstCloseErr)
	}

	m.logger.Info("topic retention updated",
		"topic", name,
		"old_retention_ms", current.RetentionMs,
		"new_retention_ms", retentionMs)

	return updated, nil
}

// UpdateTopicCaps changes the per-partition in-flight and acked-ahead
// caps for an existing topic. Zero in either field inherits the
// matching Config default. Effective immediately for all existing
// in-flight shards via consumer.InFlight.RefreshCaps.
func (m *Manager) UpdateTopicCaps(ctx context.Context, name string, maxInFlight, maxAckedAhead int64) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalid)
	}
	if maxInFlight < 0 || maxAckedAhead < 0 {
		return topic.Topic{}, fmt.Errorf("%w: caps must be >= 0", ErrInvalid)
	}
	if maxInFlight == 0 {
		maxInFlight = m.cfg.DefaultMaxInFlightPerPartition
	}
	if maxAckedAhead == 0 {
		maxAckedAhead = m.cfg.DefaultMaxAckedAheadPerPartition
	}

	current, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}

	updated := current
	updated.MaxInFlightPerPartition = maxInFlight
	updated.MaxAckedAheadPerPartition = maxAckedAhead

	if err := m.metastore.UpdateTopic(ctx, updated); err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrNotFound
		}
		return topic.Topic{}, err
	}

	if err := m.offsets.RefreshCaps(ctx, name); err != nil {
		// Metastore landed; in-flight shards will pick up new caps on
		// next access (RefreshCaps failure is non-fatal because shard
		// creation always re-resolves from the metastore).
		m.logger.Error("update caps: refresh in-flight shards", "topic", name, "err", err)
	}

	m.logger.Info("topic caps updated",
		"topic", name,
		"max_in_flight_per_partition", maxInFlight,
		"max_acked_ahead_per_partition", maxAckedAhead)
	return updated, nil
}

// UpdateTopicSchema registers a new JSON Schema version for the topic.
// The schema registry enforces backwards compatibility: new schemas
// must be additive-only with no type changes on existing fields. The
// raw schema bytes are persisted via the metastore.
func (m *Manager) UpdateTopicSchema(ctx context.Context, name string, rawSchema []byte) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalid)
	}
	if len(rawSchema) == 0 {
		return topic.Topic{}, fmt.Errorf("%w: schema must not be empty", ErrInvalid)
	}

	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}

	version, err := m.schemas.Register(ctx, name, rawSchema)
	if err != nil {
		return topic.Topic{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	if err := m.metastore.PutSchema(ctx, name, version, rawSchema); err != nil {
		return topic.Topic{}, fmt.Errorf("topics: persist schema: %w", err)
	}

	m.logger.Info("topic schema updated",
		"topic", name,
		"version", version)

	return t, nil
}
