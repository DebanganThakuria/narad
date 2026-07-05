// Package messaging owns the broker's data-plane: produce, consume,
// and ack. The Engine type is embedded by the broker facade so its
// methods are promoted onto the Broker interface; HTTP handlers and
// the CLI never see the package split.
//
// Files:
//
//   - engine.go:         Engine struct, ConsumeOpts, error sentinels, constructor.
//   - routing.go:        partition ownership and produce-routing helpers.
//   - produce.go:        Produce (synchronous append + commit path).
//   - produce_accept.go: AcceptProduce (WAL-first accept path).
//   - produce_commit.go: CommitAcceptedProduce(+Batch) and the durability boundary.
//   - consume.go:        Consume (queue pull, replay, long-poll).
//   - ack.go:            Ack.
//   - schema.go:         payload validation and lazy schema loading.
//   - metadata_cache.go: versioned read-through caches over the metastore.
//
// Cross-package state: Engine holds *runtime.Logs (lazy partition log
// access), *consumer.InFlight (reservation/ack book-keeping), and
// cached metadata used by the hot path.
package messaging

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Aliases of the shared broker error sentinels for ergonomic local
// use. The broker package re-exports the underlying errs.* values
// publicly.
var (
	ErrTopicNotFound     = errs.ErrTopicNotFound
	ErrInvalid           = errs.ErrInvalidArgument
	ErrPartitionRequired = errs.ErrPartitionRequired
	ErrNotPartitionOwner = errs.ErrNotPartitionOwner
)

// ConsumeOpts is the input for Engine.Consume.
//
//   - If Partition is nil, the broker scans partitions in order looking
//     for the first one with an undelivered message (queue-style pull).
//   - If Offset is set, replay mode is engaged and Partition is required.
//   - Wait controls long-polling: if no message is available now, the
//     broker waits up to Wait for one (or for ctx to expire), whichever
//     is sooner.
type ConsumeOpts struct {
	Partition *int
	Offset    *int64
	Wait      time.Duration
}

// Engine handles produce, consume, and ack. Constructed once at
// broker startup; safe for concurrent use.
type Engine struct {
	metastore  metastore.Metastore
	schemas    schema.Registry
	partitions partition.Manager
	offsets    *consumer.InFlight
	logs       *runtime.Logs
	ingress    *ingress.Manager
	metrics    *metrics.Metrics
	logger     *slog.Logger
	selfID     string

	cacheMu         sync.RWMutex
	topicCache      map[string]cached[topic.Topic]
	assignmentCache map[string]cached[assignmentSet]
	memberCache     map[string]cached[routingMember]
	schemaLoadCache map[string]cached[bool]

	consumeCursors sync.Map // topic name -> *atomic.Uint64
}

// NewEngine wires an Engine.
func NewEngine(
	ms metastore.Metastore,
	schemas schema.Registry,
	partitions partition.Manager,
	offsets *consumer.InFlight,
	logs *runtime.Logs,
	ingressManager *ingress.Manager,
	m *metrics.Metrics,
	logger *slog.Logger,
	selfID string,
) *Engine {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if offsets != nil && logs != nil {
		// When the in-flight purger releases expired reservations the
		// messages become redeliverable, but the partition log has no
		// way to know — its NotifyC otherwise fires only on new data.
		// Wake blocked long-pollers so they retry immediately instead
		// of sleeping out their full Wait. Peek (not Get) so a purge
		// never lazily reopens a log a concurrent topic delete retired.
		offsets.SetReleaseNotifier(func(topicName string, partitionIdx int) {
			if l, ok := logs.Peek(topicName, partitionIdx); ok {
				l.Wake()
			}
		})
	}
	return &Engine{
		metastore:       ms,
		schemas:         schemas,
		partitions:      partitions,
		offsets:         offsets,
		logs:            logs,
		ingress:         ingressManager,
		metrics:         m,
		logger:          logger,
		selfID:          selfID,
		topicCache:      make(map[string]cached[topic.Topic]),
		assignmentCache: make(map[string]cached[assignmentSet]),
		memberCache:     make(map[string]cached[routingMember]),
		schemaLoadCache: make(map[string]cached[bool]),
	}
}

// nextConsumeScanStart rotates the queue-mode scan start across a
// topic's partitions so concurrent consumers spread over partitions
// instead of all draining partition 0 first.
func (e *Engine) nextConsumeScanStart(topicName string, partitions int) int {
	if partitions <= 1 {
		return 0
	}
	counter, _ := e.consumeCursors.LoadOrStore(topicName, new(atomic.Uint64))
	cursor := counter.(*atomic.Uint64).Add(1) - 1
	return int(cursor % uint64(partitions))
}

// Close releases the ingress WAL, if any. Partition logs are owned by
// runtime.Logs and closed by the broker lifecycle.
func (e *Engine) Close() error {
	if e.ingress != nil {
		return e.ingress.Close()
	}
	return nil
}
