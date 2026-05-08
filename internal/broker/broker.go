// Package broker is the central orchestrator. It owns per-partition
// logs, drives the produce/consume/ack flows, and stitches the
// metastore, partition manager, schema registry, replicator, and offset
// tracker together.
//
// Concurrency: a Broker is safe for concurrent use.
package broker

import (
	"context"

	"github.com/debanganthakuria/narad/internal/topic"
)

// Broker is the public interface used by transports (HTTP today, more
// later). Returned errors should be discriminated with errors.Is against
// the sentinels in this package.
type Broker interface {
	CreateTopic(ctx context.Context, name string, partitions, replicationFactor int) (topic.Topic, error)
	IncreaseTopicPartitions(ctx context.Context, name string, newPartitions int) (topic.Topic, error)
	DeleteTopic(ctx context.Context, name string) error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	GetTopicDetails(ctx context.Context, name string) (topic.Details, error)
	ListTopics(ctx context.Context) ([]topic.Topic, error)

	Produce(ctx context.Context, topicName, key string, payload []byte) (offset int64, partition int, err error)
	Consume(ctx context.Context, topicName string, opts ConsumeOpts) (msg topic.Message, found bool, err error)
	Ack(ctx context.Context, topicName string, partition int, offset int64) error

	Ready(ctx context.Context) error
	Close() error
}
