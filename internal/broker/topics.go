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
// partitions == 0 (or replicationFactor == 0) → use the configured
// default. Negative values are rejected; partitions exceeding
// TopicPolicy.MaxPartitions are rejected.
func (b *impl) CreateTopic(ctx context.Context, name string, partitions, replicationFactor int) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	if partitions < 0 {
		return topic.Topic{}, fmt.Errorf("%w: partitions must be >= 0 (0 = use default)", ErrInvalidArgument)
	}
	if replicationFactor < 0 {
		return topic.Topic{}, fmt.Errorf("%w: replication_factor must be >= 0 (0 = use default)", ErrInvalidArgument)
	}
	if partitions == 0 {
		partitions = b.deps.TopicPolicy.DefaultPartitions
	}
	if replicationFactor == 0 {
		replicationFactor = b.deps.TopicPolicy.DefaultReplicationFactor
	}
	if max := b.deps.TopicPolicy.MaxPartitions; max > 0 && partitions > max {
		return topic.Topic{}, fmt.Errorf("%w: partitions (%d) exceeds topic.max_partitions (%d)",
			ErrInvalidArgument, partitions, max)
	}

	t := topic.Topic{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		Retention:         b.deps.TopicPolicy.DefaultRetention,
		CreatedAt:         time.Now().UTC(),
	}

	if err := b.deps.Metastore.CreateTopic(ctx, t); err != nil {
		if errors.Is(err, metastore.ErrAlreadyExists) {
			return topic.Topic{}, ErrTopicAlreadyExists
		}
		return topic.Topic{}, err
	}

	dir := filepath.Join(b.deps.DataDir, "topics", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return topic.Topic{}, fmt.Errorf("broker: create topic dir: %w", err)
	}

	b.deps.Logger.Info("topic created",
		"topic", name,
		"partitions", partitions,
		"replication_factor", replicationFactor)
	return t, nil
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

	dir := filepath.Join(b.deps.DataDir, "topics", name)
	if err := os.RemoveAll(dir); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("broker: remove topic dir: %w", err)
	}

	if err := b.deps.Metastore.DeleteTopic(ctx, name); err != nil {
		if !errors.Is(err, metastore.ErrNotFound) && firstErr == nil {
			firstErr = err
		}
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
	if max := b.deps.TopicPolicy.MaxPartitions; max > 0 && newPartitions > max {
		return topic.Topic{}, fmt.Errorf("%w: partitions (%d) exceeds topic.max_partitions (%d)",
			ErrInvalidArgument, newPartitions, max)
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

	if err := b.deps.Metastore.UpdateTopic(ctx, updated); err != nil {
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

func (b *impl) ListTopics(ctx context.Context) ([]topic.Topic, error) {
	return b.deps.Metastore.ListTopics(ctx)
}
