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
	"sort"
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

type assignmentReader interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
	GetMember(podID string) (metastore.Member, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
}

func backlogForPartition(snapshot metrics.PartitionSnapshot) int64 {
	backlog := snapshot.LogEndOffset - snapshot.CommittedOffset
	if backlog < 0 {
		return 0
	}
	return backlog
}

func ownerPartitions(assignments []metastore.Assignment, ownerID string) []int {
	owned := make([]int, 0, len(assignments))
	for _, assignment := range assignments {
		if assignment.OwnerID != ownerID {
			continue
		}
		owned = append(owned, assignment.Partition)
	}
	return owned
}

func sortPartitions(partitions []int) []int {
	out := append([]int(nil), partitions...)
	sort.Ints(out)
	return out
}

func (e *Engine) localPartitions(topicName string, totalPartitions int) []int {
	if e.selfID == "" {
		return allPartitions(totalPartitions)
	}
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return allPartitions(totalPartitions)
	}
	rows, err := assignments.ListAssignments(topicName)
	if err != nil || len(rows) == 0 {
		return allPartitions(totalPartitions)
	}
	owned := ownerPartitions(rows, e.selfID)
	if len(owned) == 0 {
		return nil
	}
	return sortPartitions(owned)
}

func (e *Engine) localProbePartitions(topicName string, totalPartitions int, pinnedPartition *int) ([]int, error) {
	if pinnedPartition != nil {
		if *pinnedPartition < 0 || *pinnedPartition >= totalPartitions {
			return nil, fmt.Errorf("%w: partition out of range", ErrInvalid)
		}
		if !e.isLocalOwner(topicName, *pinnedPartition) {
			return nil, ErrNotPartitionOwner
		}
		return []int{*pinnedPartition}, nil
	}
	scan := e.localPartitions(topicName, totalPartitions)
	if len(scan) == 0 {
		return nil, ErrNotPartitionOwner
	}
	return scan, nil
}

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
	replicator replication.Replicator
	offsets    *consumer.InFlight
	logs       *runtime.Logs
	metrics    *metrics.Metrics
	logger     *slog.Logger
	selfID     string
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
	selfID string,
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
		selfID:     selfID,
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

func (e *Engine) pickProducePartition(topicName, key string, partitions int) (int, error) {
	start := e.partitions.Pick(topicName, key, partitions)
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return start, nil
	}
	for i := 0; i < partitions; i++ {
		candidate := (start + i) % partitions
		assignment, err := assignments.GetAssignment(topicName, candidate)
		if err != nil {
			continue
		}
		owner, err := assignments.GetMember(assignment.OwnerID)
		if err != nil || owner.Status != metastore.MemberAlive {
			continue
		}
		if assignment.FollowerID == "" {
			return candidate, nil
		}
		follower, err := assignments.GetMember(assignment.FollowerID)
		if err != nil || follower.Status != metastore.MemberAlive {
			continue
		}
		return candidate, nil
	}
	return 0, fmt.Errorf("messaging: no alive partition owner for topic %s", topicName)
}

func (e *Engine) isLocalOwner(topicName string, partition int) bool {
	if e.selfID == "" {
		return true
	}
	assignments, ok := e.metastore.(assignmentReader)
	if !ok {
		return true
	}
	assignment, err := assignments.GetAssignment(topicName, partition)
	if err != nil {
		return true
	}
	return assignment.OwnerID == e.selfID
}
