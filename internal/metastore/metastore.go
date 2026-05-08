// Package metastore is the contract for Narad's persistent metadata:
// topics, schemas, and committed consumer offsets.
package metastore

import (
	"context"

	"github.com/debanganthakuria/narad/internal/topic"
)

// Metastore is the broker's view of durable metadata.
type Metastore interface {
	CreateTopic(ctx context.Context, t topic.Topic) error
	UpdateTopic(ctx context.Context, t topic.Topic) error
	DeleteTopic(ctx context.Context, name string) error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	ListTopics(ctx context.Context) ([]topic.Topic, error)

	PutSchema(ctx context.Context, topic string, version int, schema []byte) error
	GetSchema(ctx context.Context, topic string, version int) ([]byte, error)

	GetConsumerOffset(ctx context.Context, topic string, partition int) (int64, error)
	SetConsumerOffset(ctx context.Context, topic string, partition int, offset int64) error

	Close() error
}
