// Package broker is the orchestrator facade. It composes per-domain
// managers — topics (CRUD), messaging (produce/consume/ack), runtime
// (partition logs, snapshot, lifecycle) — into a single Broker
// surface. Each manager lives in its own subpackage; the facade in
// this file embeds them so their methods are promoted onto the
// Broker interface.
//
// Files:
//
//   - broker.go: Broker interface (this file).
//   - deps.go:   Deps struct passed to broker.New.
//   - impl.go:   *impl facade (embedding) and the New constructor.
//
// Subpackages:
//
//   - errs/:      shared error sentinels (TopicNotFound, InvalidArgument, ...).
//   - runtime/:   *Logs (partition log map), *Snapshotter, *Lifecycle.
//   - topics/:    *Manager — CreateTopic / Update* / DeleteTopic / Get* / List.
//   - messaging/: *Engine  — Produce / Consume / Ack.
//
// Errors returned by broker methods alias the sentinels in errs/, so
// callers compare via errors.Is(err, broker.ErrTopicNotFound) without
// caring which subpackage produced the error.
//
// Concurrency: a Broker is safe for concurrent use.
package broker

import (
	"context"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// Broker is the public interface used by transports (HTTP only for
// now). Returned errors should be discriminated with errors.Is
// against the sentinels in this package.
type Broker interface {
	CreateTopic(ctx context.Context, opts topics.CreateOpts) (topic.Topic, error)
	IncreaseTopicPartitions(ctx context.Context, name string, newPartitions int) (topic.Topic, error)
	UpdateTopicRetention(ctx context.Context, name string, retentionMs int64) (topic.Topic, error)
	UpdateTopicCaps(ctx context.Context, name string, maxInFlightPerPartition, maxAckedAheadPerPartition int64) (topic.Topic, error)
	// UpdateTopicSchema registers a new JSON Schema version for the
	// topic, enforcing backwards compatibility.
	UpdateTopicSchema(ctx context.Context, name string, schema []byte) (topic.Topic, error)
	DeleteTopic(ctx context.Context, name string) error
	PurgeTopic(ctx context.Context, name string) error
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	GetTopicDetails(ctx context.Context, name string) (topic.Details, error)
	// ListTopics returns topics in lexicographic order. See
	// metastore.ListOptions for pagination semantics.
	ListTopics(ctx context.Context, opts metastore.ListOptions) (topics []topic.Topic, nextPageToken string, err error)

	Produce(ctx context.Context, topicName, key string, payload []byte) (offset int64, partition int, err error)
	Consume(ctx context.Context, topicName string, opts messaging.ConsumeOpts) (msg topic.Message, found bool, err error)
	// Ack accepts an opaque receipt handle returned by a prior Consume
	// call. The broker decodes, verifies HMAC, and only commits if the
	// handle still matches an active reservation.
	Ack(ctx context.Context, topicName string, receiptHandle string) error

	// Snapshot returns the current runtime state of every topic and
	// partition. Used by the metrics poller; safe to call frequently.
	Snapshot(ctx context.Context) ([]metrics.TopicSnapshot, error)

	Ready(ctx context.Context) error
	Close() error
}
