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
	ListTopics(ctx context.Context, opts ListOptions) ([]topic.Topic, string, error)

	PutSchema(ctx context.Context, topic string, version int, schema []byte) error
	GetSchema(ctx context.Context, topic string, version int) ([]byte, error)

	GetConsumerOffset(ctx context.Context, topic string, partition int) (int64, error)
	SetConsumerOffset(ctx context.Context, topic string, partition int, offset int64) error

	Close() error
}

// ListOptions controls pagination for ListTopics. Pagination is keyset
// by topic name (lexicographic ascending) — robust against inserts and
// deletes between pages, unlike offset-based pagination.
//
//   - Limit == 0: return every topic in one shot. The underlying list
//     is cached, which is what the metrics poller wants.
//   - Limit > 0: return up to Limit topics. The returned next-token is
//     non-empty when more rows exist, and should be passed verbatim as
//     the next call's PageToken to fetch the next page. Cache is
//     bypassed.
//   - PageToken: empty for the first page, otherwise the token returned
//     by the previous call.
type ListOptions struct {
	Limit     int
	PageToken string
}
