// Package messaging owns the broker's data-plane: produce, consume,
// and ack. The Engine type is embedded by the broker facade so its
// methods are promoted onto the Broker interface; HTTP handlers and
// the CLI never see the package split.
//
// Files:
//
//   - engine.go:  Engine struct, ConsumeOpts, error sentinels, constructor.
//   - produce.go: Produce.
//   - consume.go: Consume + tryQueueRead + replayRead + waitForActivity.
//   - ack.go:     Ack.
//
// Cross-package state: Engine holds *runtime.Logs (lazy partition log
// access), *consumer.InFlight (reservation/ack book-keeping), and the
// HMAC secret for receipt handles.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/replication"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Aliases of the shared broker error sentinels for ergonomic local
// use. The broker package re-exports the underlying errs.* values
// publicly.
var (
	ErrTopicNotFound     = errs.ErrTopicNotFound
	ErrInvalid           = errs.ErrInvalidArgument
	ErrPartitionRequired = errs.ErrPartitionRequired
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
	replicator replication.Replicator
	offsets    *consumer.InFlight
	logs       *runtime.Logs
	metrics    *metrics.Metrics
	logger     *slog.Logger
}

// NewEngine wires an Engine. handleSecret must be at least 16 bytes
// (caller's responsibility to validate; broker.New does so).
func NewEngine(
	ms metastore.Metastore,
	schemas schema.Registry,
	partitions partition.Manager,
	replicator replication.Replicator,
	offsets *consumer.InFlight,
	logs *runtime.Logs,
	m *metrics.Metrics,
	logger *slog.Logger,
) *Engine {
	return &Engine{
		metastore:  ms,
		schemas:    schemas,
		partitions: partitions,
		replicator: replicator,
		offsets:    offsets,
		logs:       logs,
		metrics:    m,
		logger:     logger,
	}
}

// getTopic looks up a topic record, mapping metastore.ErrNotFound to
// the shared sentinel so callers can errors.Is against
// errs.TopicNotFound regardless of which manager surfaced the error.
func (e *Engine) getTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := e.metastore.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrTopicNotFound
		}
		return topic.Topic{}, fmt.Errorf("messaging: get topic: %w", err)
	}
	return t, nil
}
