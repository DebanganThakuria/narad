// Package broker is the central orchestrator. It owns per-partition
// logs, drives the produce/consume/ack flows, and stitches the
// metastore, partition manager, schema registry, replicator, and offset
// tracker together.
//
// Concurrency: a Broker is safe for concurrent use.
package broker

import (
	"context"

	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/observability/metrics"
	"github.com/debanganthakuria/narad/internal/topic"
)

// Broker is the public interface used by transports (HTTP today, more
// later). Returned errors should be discriminated with errors.Is against
// the sentinels in this package.
type Broker interface {
	// CreateTopic registers a new topic. retention zero values fall
	// back to TopicPolicy.DefaultRetention; non-zero values must be
	// non-negative.
	CreateTopic(ctx context.Context, name string, partitions, replicationFactor int, retention topic.Retention) (topic.Topic, error)
	IncreaseTopicPartitions(ctx context.Context, name string, newPartitions int) (topic.Topic, error)
	// UpdateTopicRetention changes the retention bounds of an existing
	// topic. retention zero values fall back to defaults. Cached
	// partition logs for this topic are closed so the next access
	// reopens with the new bounds.
	UpdateTopicRetention(ctx context.Context, name string, retention topic.Retention) (topic.Topic, error)
	DeleteTopic(ctx context.Context, name string) error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	GetTopicDetails(ctx context.Context, name string) (topic.Details, error)
	// ListTopics returns topics in lexicographic order. See
	// metastore.ListOptions for pagination semantics.
	ListTopics(ctx context.Context, opts metastore.ListOptions) (topics []topic.Topic, nextPageToken string, err error)

	Produce(ctx context.Context, topicName, key string, payload []byte) (offset int64, partition int, err error)
	Consume(ctx context.Context, topicName string, opts ConsumeOpts) (msg topic.Message, found bool, err error)
	Ack(ctx context.Context, topicName string, partition int, offset int64) error

	// Snapshot returns the current runtime state of every topic and
	// partition. Used by the metrics poller; safe to call frequently.
	Snapshot(ctx context.Context) ([]metrics.TopicSnapshot, error)

	Ready(ctx context.Context) error
	Close() error
}
