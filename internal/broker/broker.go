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
	"time"

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
	// UpdateTopicCaps sets the per-partition caps the call names. A nil
	// cap keeps the stored value, read under the topic lock on the
	// leader, so a caller never writes back a value it read earlier.
	UpdateTopicCaps(ctx context.Context, name string, maxInFlightPerPartition, maxAckedAheadPerPartition *int64) (topic.Topic, error)
	// UpdateTopicSchema registers a new JSON Schema version for the
	// topic, enforcing backwards compatibility. Re-registering the
	// current schema is a no-op. A positive baseVersion makes the
	// update conditional on the current version being exactly that
	// (errs.ErrSchemaVersionConflict otherwise); zero is unconditional.
	UpdateTopicSchema(ctx context.Context, name string, schema []byte, baseVersion int) (topic.Topic, error)
	// TopicSchemaHistory lists every schema version of the topic in
	// ascending order (empty, version 0, for a topic without a schema).
	TopicSchemaHistory(ctx context.Context, name string) (topic.SchemaHistory, error)
	DeleteTopic(ctx context.Context, name string) error
	// PurgeTopic drops this node's local state of one incarnation of a
	// deleted topic (id is the deleted record's ID; empty purges by
	// name for senders that predate incarnation IDs).
	PurgeTopic(ctx context.Context, name, id string) error
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

	// PartitionTransferInfo and ReadPartitionSegment are the serve-side
	// of partition rebalance: a destination node copies an owned
	// partition's segments verbatim to become the new owner.
	PartitionTransferInfo(ctx context.Context, topicName string, partition int) (messaging.PartitionTransferInfo, error)
	ReadPartitionSegment(ctx context.Context, topicName string, partition int, baseOffset, at, length int64) ([]byte, error)

	// PauseProduceForHandoff / ResumeProduce briefly stop and resume
	// produce for a partition during a rebalance cutover — paused produce
	// reroutes to a live partition (AP).
	PauseProduceForHandoff(topicName string, partition int, ttl time.Duration)
	ResumeProduce(topicName string, partition int)
	// PrepareHandoff freezes an owned partition and returns its final
	// transfer info for the destination's last catch-up before the flip.
	PrepareHandoff(ctx context.Context, topicName string, partition int, freezeTTL time.Duration) (messaging.PartitionTransferInfo, error)
	// ReclaimMovedPartition deletes the stale local copy of a partition a
	// completed move relocated to another node. Refuses unless the local
	// view affirmatively shows another node owning it.
	ReclaimMovedPartition(ctx context.Context, topicName string, partition int) error
	// FanoutCursorStats reports fan-out cursor positions for the parent
	// partitions this node owns (lag = HighWatermark - NextOffset).
	FanoutCursorStats(ctx context.Context, parent string) ([]topic.FanoutCursorStat, error)

	Produce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (offset int64, partitionIdx int, err error)
	AcceptProduce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (ingress.AcceptedProduce, error)
	CommitAcceptedProduce(ctx context.Context, record ingress.ProduceRecord) (offset int64, err error)
	CommitAcceptedProduceBatch(ctx context.Context, records []ingress.ProduceRecord) (offsets []int64, err error)
	Consume(ctx context.Context, topicName string, opts messaging.ConsumeOpts) (msg topic.Message, found bool, err error)
	// ConsumeProbe and ConsumeWait split a queue-style consume so the
	// HTTP handler can ask remote owners between the local probe and the
	// local long-poll without re-scanning or losing a wake-up. See
	// messaging.Engine.ConsumeProbe.
	ConsumeProbe(ctx context.Context, topicName string, opts messaging.ConsumeOpts) (msg topic.Message, found bool, waiter *messaging.ConsumeWaiter, err error)
	// external, when non-nil, is a second wake source folded into the
	// same select as the local wait, so a consumer racing the local and
	// cross-node halves costs ONE parked goroutine rather than two.
	// wokeExternal reports that it, rather than a local record, is what
	// woke the wait.
	ConsumeWait(ctx context.Context, waiter *messaging.ConsumeWaiter, wait time.Duration, external <-chan struct{}) (msg topic.Message, found bool, wokeExternal bool, err error)
	// RegisterRemoteDemand and DropRemoteDemand let the cluster layer put
	// a peer's token in the same delivery queue as local consumers. The
	// peer is only ever TOLD that records exist and claims them itself,
	// so nothing is reserved on its behalf. See messaging.RemoteDemand.
	RegisterRemoteDemand(ctx context.Context, topicName string, rd messaging.RemoteDemand) error
	DropRemoteDemand(topicName string, rd messaging.RemoteDemand)
	// NoteRemoteClaim reports that a peer's claim (a local-only consume
	// sent in answer to a notification) has arrived for the topic, so
	// the dispatcher can release that notification's hold at once.
	NoteRemoteClaim(topicName string)
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

// TopicIDDeleter is the optional delete surface of a Broker that
// reports the topic incarnation it removed. Brokers built by New
// implement it (via the embedded topics.Manager). The HTTP delete and
// the forwarded-delete RPC assert for it so the purge fan-out names the
// incarnation the delete actually removed, read under the topic's lock;
// without it they read the incarnation before the delete, as before.
// Like CreateGater it stays out of Broker so test fakes of Broker need
// not implement it.
type TopicIDDeleter interface {
	// DeleteTopicID deletes the topic and returns the ID of the
	// incarnation it removed, also alongside a topics.PurgeError. See
	// topics.Manager.DeleteTopicID.
	DeleteTopicID(ctx context.Context, name string) (string, error)
}

// Compile-time check: the incarnation-reporting delete stays reachable
// through the facade via the embedded topics.Manager.
var _ TopicIDDeleter = (*impl)(nil)

// PartitionStatsReader is the optional one-partition describe of a
// Broker. Brokers built by New implement it (via the embedded
// topics.Manager). The cluster's per-partition stats RPC asserts for it
// and falls back to a whole GetTopicDetails without it. Like CreateGater
// it stays out of Broker so test fakes of Broker need not implement it.
type PartitionStatsReader interface {
	// LocalPartitionStats describes one partition as this node sees it
	// without reading the topic's schema; a partition outside the
	// topic's range is an invalid-argument error. See
	// topics.Manager.LocalPartitionStats.
	LocalPartitionStats(ctx context.Context, name string, partition int) (topic.PartitionStats, error)
}

// Compile-time check: the one-partition describe stays reachable through
// the facade via the embedded topics.Manager.
var _ PartitionStatsReader = (*impl)(nil)

// BatchProducer is the optional batch-produce surface of a Broker.
// Brokers built by New implement it (via the embedded messaging.Engine).
// The HTTP batch produce handler asserts for it and refuses the request
// without it: accepting a batch one AcceptProduce at a time would give
// up all or nothing. Like CreateGater it stays out of Broker so test
// fakes of Broker need not implement it.
type BatchProducer interface {
	// AcceptProduceBatch validates every message and then durably
	// accepts them all into the ingress WAL, in order, or accepts none.
	// See messaging.Engine.AcceptProduceBatch.
	AcceptProduceBatch(ctx context.Context, topicName string, msgs []messaging.ProduceMessage) ([]ingress.AcceptedProduce, error)
}

// Compile-time check: batch produce stays reachable through the facade
// via the embedded messaging.Engine.
var _ BatchProducer = (*impl)(nil)

// BatchConsumer is the optional batch-consume surface of a Broker.
// Brokers built by New implement it (via the embedded messaging.Engine).
// The HTTP consume handler asserts for it when a request asks for more
// than one record (?max=N) and serves one record at a time without it.
// Like CreateGater it stays out of Broker so test fakes of Broker need
// not implement it.
type BatchConsumer interface {
	// ConsumeBatch reserves up to max records in one non-blocking scan
	// and appends them to dst; with none reservable it returns a waiter
	// for ConsumeWait. See messaging.Engine.ConsumeBatch.
	ConsumeBatch(ctx context.Context, topicName string, opts messaging.ConsumeOpts, max int, dst []topic.Message) ([]topic.Message, *messaging.ConsumeWaiter, error)
}
