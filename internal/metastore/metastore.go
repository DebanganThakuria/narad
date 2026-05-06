// Package metastore is the contract for Narad's persistent metadata:
// topics, schemas, and committed consumer offsets.
//
// The PRD calls for SQLite, but Narad keeps a strict zero-third-party-dep
// policy. The default implementation (jsonfile.go) is a JSON-on-disk
// store behind this interface; a SQLite-backed implementation can drop
// in later without touching callers.
package metastore

import (
	"context"

	"github.com/debanganthakuria/narad/internal/topic"
)

// Metastore is the broker's view of durable metadata.
type Metastore interface {
	CreateTopic(ctx context.Context, t topic.Topic) error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	ListTopics(ctx context.Context) ([]topic.Topic, error)

	PutSchema(ctx context.Context, topic string, version int, schema []byte) error
	GetSchema(ctx context.Context, topic string, version int) ([]byte, error)

	GetConsumerOffset(ctx context.Context, topic string, partition int) (int64, error)
	SetConsumerOffset(ctx context.Context, topic string, partition int, offset int64) error

	Close() error
}
