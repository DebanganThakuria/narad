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

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
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

	// AttachChild links child under parent for fan-out: every message
	// produced to parent from the attach point on is also delivered to
	// child. A positive delayMs makes it a DELAY child: records are
	// delivered only once parentCommitTime+delayMs has passed, and the
	// parent's retention must buffer delay + the minimum floor.
	// DetachChild unlinks; the child keeps what it received.
	AttachChild(ctx context.Context, parent, child string, delayMs int64) error
	DetachChild(ctx context.Context, parent, child string) error

	// ReadFanoutSlab reads committed keyed records from a locally owned
	// partition — the fan-out cursor engine's read primitive.
	ReadFanoutSlab(ctx context.Context, topicName string, partitionIdx int, opts topic.FanoutReadOpts) (topic.FanoutSlab, error)
	// FanoutCursorStats reports fan-out cursor positions for the parent
	// partitions this node owns (lag = HighWatermark - NextOffset).
	FanoutCursorStats(ctx context.Context, parent string) ([]topic.FanoutCursorStat, error)

	Produce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (offset int64, partitionIdx int, err error)
	AcceptProduce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (ingress.AcceptedProduce, error)
	CommitAcceptedProduce(ctx context.Context, record ingress.ProduceRecord) (offset int64, err error)
	CommitAcceptedProduceBatch(ctx context.Context, records []ingress.ProduceRecord) (offsets []int64, err error)
	Consume(ctx context.Context, topicName string, opts messaging.ConsumeOpts) (msg topic.Message, found bool, err error)
	// Ack accepts a decoded receipt handle returned by a prior Consume
	// call. The broker commits only if the handle still matches an
	// active reservation.
	Ack(ctx context.Context, topicName string, handle consumer.Handle) error
	// ExtendAck renews the handle's visibility window to a full fresh
	// window instead of committing, so a slow consumer keeps its lease.
	// Same validation as Ack: a lapsed handle fails with ErrHandleStale.
	ExtendAck(ctx context.Context, topicName string, handle consumer.Handle) error
	// Nack releases the handle's reservation immediately (visibility
	// zero): the message becomes redeliverable right away.
	Nack(ctx context.Context, topicName string, handle consumer.Handle) error

	// Snapshot returns the current runtime state of every topic and
	// partition. Used by the metrics poller; safe to call frequently.
	Snapshot(ctx context.Context) ([]metrics.TopicSnapshot, error)

	Ready(ctx context.Context) error
	Close() error
}

// CreateGater is the optional startup-gating surface of a Broker.
// Brokers built by New implement it (via the embedded topics.Manager):
// ArmCreateGate blocks CreateTopic on every transport (HTTP and cluster
// RPC alike) until ReleaseCreateGate opens the gate. serve.go arms the
// gate before the cluster RPC listener starts and releases it once the
// startup orphan sweep has completed, so a peer-forwarded create can
// never land a topic directory while the sweep is still walking.
//
// It is intentionally not part of Broker: transports never gate, and
// test fakes of Broker shouldn't have to implement it. The gate defaults
// to open, so brokers that never arm it behave exactly as before.
type CreateGater interface {
	ArmCreateGate()
	ReleaseCreateGate()
}
