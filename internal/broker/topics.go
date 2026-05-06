package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/topic"
)

// CreateTopic registers a new topic in the metastore and prepares its
// on-disk directory. Partition log files are opened lazily on first use.
func (b *impl) CreateTopic(ctx context.Context, name string, partitions, replicationFactor int) (topic.Topic, error) {
	if name == "" {
		return topic.Topic{}, fmt.Errorf("%w: name required", ErrInvalidArgument)
	}
	if partitions <= 0 {
		return topic.Topic{}, fmt.Errorf("%w: partitions must be > 0", ErrInvalidArgument)
	}
	if replicationFactor <= 0 {
		return topic.Topic{}, fmt.Errorf("%w: replication_factor must be > 0", ErrInvalidArgument)
	}

	t := topic.Topic{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
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

// GetTopic returns the topic record from the metastore, mapping
// metastore.ErrNotFound to broker.ErrTopicNotFound.
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

// ListTopics returns every topic registered in the metastore.
func (b *impl) ListTopics(ctx context.Context) ([]topic.Topic, error) {
	return b.deps.Metastore.ListTopics(ctx)
}
