package topics

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// TopicSchemaHistory returns every persisted schema version of the
// topic in ascending order. A topic without a schema has an empty
// history and version 0.
func (m *Manager) TopicSchemaHistory(ctx context.Context, name string) (topic.SchemaHistory, error) {
	if name == "" {
		return topic.SchemaHistory{}, fmt.Errorf("%w: name required", ErrInvalid)
	}
	if _, err := m.GetTopic(ctx, name); err != nil {
		return topic.SchemaHistory{}, err
	}
	history, err := schema.PersistedHistory(ctx, m.metastore, name)
	if err != nil {
		return topic.SchemaHistory{}, fmt.Errorf("topics: read schema history: %w", err)
	}
	out := topic.SchemaHistory{Topic: name, Versions: make([]topic.SchemaVersion, 0, len(history))}
	for _, v := range history {
		out.Versions = append(out.Versions, topic.SchemaVersion{Version: v.Number, Schema: v.Raw})
		out.Version = v.Number
	}
	return out, nil
}

// IncreaseTopicPartitions raises the partition count of an existing
// topic. Increase-only — decreasing would require renumbering offsets,
// which we don't support.
//
// Caller-visible side effect: future records' partition assignment
// uses hash(key) % newPartitions, so a key that previously hashed to
// partition 3 may now hash to partition 11. Existing records stay in
// their original partitions.
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
	unlock := m.lockTopicName(name)
	defer unlock()

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
	if m.assigner != nil {
		if err := m.assigner.AssignNewPartitions(ctx, name, current.Partitions, newPartitions); err != nil {
			m.logger.Warn("topic partitions increased without immediate assignment", "topic", name, "old_partitions", current.Partitions, "new_partitions", newPartitions, "err", err)
		}
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
	if err := checkRetentionFloor(retentionMs); err != nil {
		return topic.Topic{}, err
	}
	unlock := m.lockTopicName(name)
	defer unlock()

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
	unlock := m.lockTopicName(name)
	defer unlock()

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

// schemaPutAttempts bounds the re-read-and-retry loop in
// UpdateTopicSchema when the metastore refuses a version because
// another put landed between reading the history and proposing.
const schemaPutAttempts = 3

// UpdateTopicSchema registers a new JSON Schema version for the topic.
//
// The metastore, not this node's in-memory registry, is the source of
// truth for the topic's history: the persisted versions are read first,
// the new schema is checked for backwards compatibility against the
// persisted latest, and the result is proposed as exactly latest+1. A
// leader elected after the schema was created elsewhere, which has
// never loaded the topic locally, therefore still checks against the
// real previous version instead of treating the update as a fresh v1
// and overwriting history. The Raft state machine refuses any version
// other than latest+1 as a second line of defence; on that refusal the
// history is re-read and the check repeated.
//
// The update is idempotent: a schema that is the same JSON value as
// the current latest registers nothing and returns success, so a
// client that retries after a lost response does not grow the history.
//
// baseVersion, when positive, is a precondition: the update is applied
// only if the topic's current version is exactly baseVersion, and
// answers errs.ErrSchemaVersionConflict otherwise. Two clients that
// each read v1 and PATCH cannot both land as v2 and v3 without one of
// them noticing. Zero means no precondition.
func (m *Manager) UpdateTopicSchema(ctx context.Context, name string, rawSchema []byte, baseVersion int) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalid)
	}
	if len(rawSchema) == 0 {
		return topic.Topic{}, fmt.Errorf("%w: schema must not be empty", ErrInvalid)
	}
	if baseVersion < 0 {
		return topic.Topic{}, fmt.Errorf("%w: schema_base_version must be >= 0", ErrInvalid)
	}
	unlock := m.lockTopicName(name)
	defer unlock()

	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}
	if t.IsChild() {
		// The FSM also rejects this; checking here gives the caller a
		// named error before a Raft round-trip.
		return topic.Topic{}, fmt.Errorf("%w: schema of %q is managed by parent %q; detach to manage it independently",
			errs.ErrFanoutSchemaManaged, name, t.Parent)
	}

	if err := m.schemas.ValidateDefinition(ctx, name, rawSchema); err != nil {
		return topic.Topic{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	var version int
	for attempt := 1; ; attempt++ {
		history, err := schema.PersistedHistory(ctx, m.metastore, name)
		if err != nil {
			return topic.Topic{}, fmt.Errorf("topics: read schema history: %w", err)
		}
		version = 1
		if n := len(history); n > 0 {
			latest := history[n-1]
			if baseVersion > 0 && baseVersion != latest.Number {
				return topic.Topic{}, fmt.Errorf("%w: schema_base_version %d does not match the current version %d of %q",
					errs.ErrSchemaVersionConflict, baseVersion, latest.Number, name)
			}
			if schema.Equal(latest.Raw, rawSchema) {
				m.logger.Info("topic schema unchanged", "topic", name, "version", latest.Number)
				return t, nil
			}
			if latest.Number >= metastore.MaxSchemaVersions {
				return topic.Topic{}, fmt.Errorf("%w: %q already has %d schema versions, the maximum",
					errs.ErrSchemaHistoryFull, name, latest.Number)
			}
			version = latest.Number + 1
			if err := m.schemas.CheckCompatible(ctx, name, latest.Raw, rawSchema); err != nil {
				return topic.Topic{}, fmt.Errorf("%w: %w", ErrInvalid, err)
			}
		} else if baseVersion > 0 {
			return topic.Topic{}, fmt.Errorf("%w: schema_base_version %d given but %q has no schema yet",
				errs.ErrSchemaVersionConflict, baseVersion, name)
		}

		err = m.metastore.PutSchema(ctx, name, version, rawSchema)
		if err == nil {
			break
		}
		if errors.Is(err, errs.ErrAlreadyExists) && attempt < schemaPutAttempts {
			m.logger.Warn("schema version conflict; re-reading history",
				"topic", name, "version", version, "attempt", attempt, "err", err)
			continue
		}
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrNotFound
		}
		return topic.Topic{}, fmt.Errorf("topics: persist schema: %w", err)
	}

	// The persisted history is authoritative and the produce path
	// re-hydrates the registry whenever the metastore's schema version
	// moves, so a failure to load the local copy here is not a failure
	// of the update (and must not make the client retry, which would
	// register the same schema again as the next version).
	if err := m.schemas.Load(ctx, name, version, rawSchema); err != nil {
		m.logger.Error("schema persisted but not loaded locally; produce will reload it from the metastore",
			"topic", name, "version", version, "err", err)
	}

	m.logger.Info("topic schema updated",
		"topic", name,
		"version", version)

	return t, nil
}
