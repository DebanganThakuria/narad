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
// access), *consumer.InFlight (reservation/ack book-keeping), and
// cached metadata used by the hot path.
package messaging

import (
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

type assignmentReader interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
	GetMember(podID string) (metastore.Member, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
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
	if _, ok := e.metastore.(assignmentReader); !ok {
		return allPartitions(totalPartitions)
	}
	rows, err := e.listAssignments(topicName)
	if err != nil || len(rows) == 0 {
		return nil
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
	offsets    *consumer.InFlight
	logs       *runtime.Logs
	ingress    *ingress.Manager
	metrics    *metrics.Metrics
	logger     *slog.Logger
	selfID     string

	cacheMu         sync.RWMutex
	topicCache      map[string]cachedTopic
	assignmentCache map[string]cachedAssignments
	memberCache     map[string]cachedRoutingMember
	schemaLoadCache map[string]cachedSchemaLoad
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
		topicCache:      make(map[string]cachedTopic),
		assignmentCache: make(map[string]cachedAssignments),
		memberCache:     make(map[string]cachedRoutingMember),
		schemaLoadCache: make(map[string]cachedSchemaLoad),
	}
}

func (e *Engine) Close() error {
	if e.ingress != nil {
		return e.ingress.Close()
	}
	return nil
}

func (e *Engine) pickProducePartition(topicName, key string, partitions int) (int, error) {
	start := e.partitions.Pick(topicName, key, partitions)
	if _, ok := e.metastore.(assignmentReader); !ok {
		return start, nil
	}
	for i := range partitions {
		candidate := (start + i) % partitions
		assignment, err := e.getAssignment(topicName, candidate)
		if err != nil {
			continue
		}
		if !e.produceAssignmentWritable(assignment) {
			continue
		}
		return candidate, nil
	}
	return 0, fmt.Errorf("messaging: no alive partition owner for topic %s", topicName)
}

func (e *Engine) produceAssignmentWritable(assignment metastore.Assignment) bool {
	owner, err := e.getRoutingMember(assignment.OwnerID)
	return err == nil && owner.Status == metastore.MemberAlive
}

func (e *Engine) isWritableLocalProducePartition(topicName string, partition int) bool {
	if e.selfID == "" {
		return true
	}
	if _, ok := e.metastore.(assignmentReader); !ok {
		return true
	}
	assignment, err := e.getAssignment(topicName, partition)
	if err != nil {
		return false
	}
	return assignment.OwnerID == e.selfID && e.produceAssignmentWritable(assignment)
}

func (e *Engine) isLocalOwner(topicName string, partition int) bool {
	if e.selfID == "" {
		return true
	}
	if _, ok := e.metastore.(assignmentReader); !ok {
		return true
	}
	assignment, err := e.getAssignment(topicName, partition)
	if err != nil {
		return false
	}
	return assignment.OwnerID == e.selfID
}
