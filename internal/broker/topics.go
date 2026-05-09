package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/topic"
)

// CreateTopic registers a new topic and prepares its on-disk
// directory. Partition log files are opened lazily on first use.
//
// Zero values for partitions, replicationFactor, retention.MaxAgeMs,
// and retention.MaxBytes inherit from TopicPolicy defaults. Negative
// values are rejected; partitions exceeding TopicPolicy.MaxPartitions
// are rejected.
func (b *impl) CreateTopic(ctx context.Context, name string, partitions, replicationFactor int, retention topic.Retention) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}

	if partitions == 0 {
		partitions = b.deps.TopicPolicy.DefaultPartitions
	}
	if partitions < 0 {
		return topic.Topic{}, fmt.Errorf("%w: partitions must be >= 0 (0 = use default)", ErrInvalidArgument)
	}
	if maximum := b.deps.TopicPolicy.MaxPartitions; maximum > 0 && partitions > maximum {
		return topic.Topic{}, fmt.Errorf("%w: partitions (%d) exceeds topic.max_partitions (%d)",
			ErrInvalidArgument, partitions, maximum)
	}

	if replicationFactor == 0 {
		replicationFactor = b.deps.TopicPolicy.DefaultReplicationFactor
	}
	if replicationFactor < 2 {
		return topic.Topic{}, fmt.Errorf("%w: replication_factor must be >= 2 (0 = use default)", ErrInvalidArgument)
	}

	retention, err := b.resolveRetention(retention)
	if err != nil {
		return topic.Topic{}, err
	}

	t := topic.Topic{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		Retention:         retention,
		CreatedAt:         time.Now().UTC(),
	}

	dir := filepath.Join(b.deps.DataDir, "topics", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return topic.Topic{}, fmt.Errorf("broker: create topic dir: %w", err)
	}

	if err := b.deps.Metastore.CreateTopic(ctx, t); err != nil {
		if errors.Is(err, metastore.ErrAlreadyExists) {
			return topic.Topic{}, ErrTopicAlreadyExists
		}
		return topic.Topic{}, err
	}

	b.deps.Logger.Info("topic created",
		"topic", name,
		"partitions", partitions,
		"replication_factor", replicationFactor,
		"retention_age_ms", retention.MaxAgeMs,
		"retention_bytes", retention.MaxBytes)

	return t, nil
}

// resolveRetention applies the same zero-value-inherits-default rule
// used for partitions and replication factor. Negative values are
// rejected; positive values pass through.
func (b *impl) resolveRetention(r topic.Retention) (topic.Retention, error) {
	if r.MaxAgeMs < 0 {
		return r, fmt.Errorf("%w: retention.max_age_ms must be >= 0 (0 = use default)", ErrInvalidArgument)
	}
	if r.MaxBytes < 0 {
		return r, fmt.Errorf("%w: retention.max_bytes must be >= 0 (0 = use default)", ErrInvalidArgument)
	}
	if r.MaxAgeMs == 0 {
		r.MaxAgeMs = b.deps.TopicPolicy.DefaultRetentionMs.MaxAgeMs
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = b.deps.TopicPolicy.DefaultRetentionMs.MaxBytes
	}
	return r, nil
}

// DeleteTopic removes a topic and all of its data: closes cached
// partition logs (each does a final flush), removes the on-disk
// directory, and wipes the metastore record + offsets + schemas.
func (b *impl) DeleteTopic(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	if _, err := b.GetTopic(ctx, name); err != nil {
		return err
	}

	prefix := name + "/"
	b.mu.Lock()
	var firstErr error
	for k, l := range b.logs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(b.logs, k)
	}
	b.mu.Unlock()

	if err := b.deps.Metastore.DeleteTopic(ctx, name); err != nil {
		if !errors.Is(err, metastore.ErrNotFound) && firstErr == nil {
			firstErr = err
		}
	}

	dir := filepath.Join(b.deps.DataDir, "topics", name)
	if err := os.RemoveAll(dir); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("broker: remove topic dir: %w", err)
	}

	if firstErr == nil {
		b.deps.Logger.Info("topic deleted", "topic", name)
	}
	return firstErr
}

// GetTopicDetails returns the topic record plus per-partition runtime
// stats. Lazy-opens each partition to read its stats.
func (b *impl) GetTopicDetails(ctx context.Context, name string) (topic.Details, error) {
	t, err := b.GetTopic(ctx, name)
	if err != nil {
		return topic.Details{}, err
	}
	stats := make([]topic.PartitionStats, t.Partitions)
	for i := 0; i < t.Partitions; i++ {
		l, err := b.partitionLog(name, i)
		if err != nil {
			return topic.Details{}, err
		}
		ps := topic.PartitionStats{
			Index:        i,
			Segments:     l.SegmentCount(),
			OldestOffset: l.OldestOffset(),
			NextOffset:   l.NextOffset(),
			SizeBytes:    l.SizeBytes(),
		}
		if mt, ok := l.OldestSegmentAt(); ok {
			ps.OldestSegmentAt = mt
		}
		stats[i] = ps
	}
	return topic.Details{Topic: t, Partitions: stats}, nil
}

// IncreaseTopicPartitions raises the partition count of an existing
// topic. Increase-only — decreasing would require renumbering offsets,
// which we don't support.
//
// Caller-visible side effect: future records' partition assignment
// uses hash(key) % newPartitions, so a key that previously hashed to
// partition 3 may now hash to partition 11. Existing records stay in
// their original partitions.
func (b *impl) IncreaseTopicPartitions(ctx context.Context, name string, newPartitions int) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	if newPartitions <= 0 {
		return topic.Topic{}, fmt.Errorf("%w: partitions must be > 0", ErrInvalidArgument)
	}
	if maximum := b.deps.TopicPolicy.MaxPartitions; maximum > 0 && newPartitions > maximum {
		return topic.Topic{}, fmt.Errorf("%w: partitions (%d) exceeds topic.max_partitions (%d)",
			ErrInvalidArgument, newPartitions, maximum)
	}

	current, err := b.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}
	if newPartitions <= current.Partitions {
		return topic.Topic{}, fmt.Errorf("%w: new partition count (%d) must be greater than current (%d); decrease is not supported",
			ErrInvalidArgument, newPartitions, current.Partitions)
	}

	updated := current
	updated.Partitions = newPartitions

	if err = b.deps.Metastore.UpdateTopic(ctx, updated); err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			return topic.Topic{}, ErrTopicNotFound
		}
		return topic.Topic{}, err
	}

	b.deps.Logger.Info("topic partitions increased",
		"topic", name,
		"old_partitions", current.Partitions,
		"new_partitions", newPartitions)

	return updated, nil
}

// GetTopic maps metastore.ErrNotFound to broker.ErrTopicNotFound.
func (b *impl) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := b.deps.Metastore.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			return topic.Topic{}, ErrTopicNotFound
		}
		return topic.Topic{}, err
	}
	return t, nil
}

func (b *impl) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	return b.deps.Metastore.ListTopics(ctx, opts)
}

// UpdateTopicRetention changes the retention policy of an existing
// topic. Cached partition logs are closed so the next access reopens
// them with the new bounds (storage.Options.Retention is folded in at
// log-open time, not on every operation).
//
// Closing flushes the in-memory buffer synchronously, so no in-flight
// records are lost. The cost is a brief reopen on the next produce or
// consume — acceptable for an operator-driven change.
func (b *impl) UpdateTopicRetention(ctx context.Context, name string, retention topic.Retention) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}

	current, err := b.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}

	resolved, err := b.resolveRetention(retention)
	if err != nil {
		return topic.Topic{}, err
	}

	updated := current
	updated.Retention = resolved

	if err := b.deps.Metastore.UpdateTopic(ctx, updated); err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			return topic.Topic{}, ErrTopicNotFound
		}
		return topic.Topic{}, err
	}

	// Close cached logs so subsequent partitionLog() calls reopen with
	// the updated retention bounds. Same scoping pattern as DeleteTopic.
	prefix := name + "/"
	b.mu.Lock()
	var firstCloseErr error
	for k, l := range b.logs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if err := l.Close(); err != nil && firstCloseErr == nil {
			firstCloseErr = err
		}
		delete(b.logs, k)
	}
	b.mu.Unlock()

	if firstCloseErr != nil {
		// The metastore update succeeded; report the close error but
		// the new retention is in effect for any subsequently opened
		// log. Returning the topic record so the caller knows the
		// state landed.
		b.deps.Logger.Error("update retention: close cached logs", "topic", name, "err", firstCloseErr)
		return updated, fmt.Errorf("broker: close partition logs after retention update: %w", firstCloseErr)
	}

	b.deps.Logger.Info("topic retention updated",
		"topic", name,
		"old_age_ms", current.Retention.MaxAgeMs,
		"new_age_ms", resolved.MaxAgeMs,
		"old_bytes", current.Retention.MaxBytes,
		"new_bytes", resolved.MaxBytes)

	return updated, nil
}

// UpdateTopicSchema registers a new JSON Schema version for the topic.
// The schema registry enforces backwards compatibility: new schemas must
// be additive-only with no type changes on existing fields. The raw
// schema bytes are persisted via the metastore.
func (b *impl) UpdateTopicSchema(ctx context.Context, name string, rawSchema []byte) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	if len(rawSchema) == 0 {
		return topic.Topic{}, fmt.Errorf("%w: schema must not be empty", ErrInvalidArgument)
	}

	t, err := b.GetTopic(ctx, name)
	if err != nil {
		return topic.Topic{}, err
	}

	version, err := b.deps.Schemas.Register(ctx, name, rawSchema)
	if err != nil {
		return topic.Topic{}, fmt.Errorf("%w: %w", ErrInvalidArgument, err)
	}

	if err := b.deps.Metastore.PutSchema(ctx, name, version, rawSchema); err != nil {
		return topic.Topic{}, fmt.Errorf("broker: persist schema: %w", err)
	}

	b.deps.Logger.Info("topic schema updated",
		"topic", name,
		"version", version)

	return t, nil
}
