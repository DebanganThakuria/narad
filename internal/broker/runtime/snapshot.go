package runtime

import (
	"context"
	"log/slog"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// partitionAssignmentReader is the optional metastore capability used
// to resolve partition ownership (implemented by *metastore.Store).
type partitionAssignmentReader interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
}

// Snapshotter produces the read-only inventory of every topic and
// partition consumed by the metrics poller. Constructed once at
// broker startup and embedded into the broker facade so its Snapshot
// method satisfies the Broker interface.
type Snapshotter struct {
	metastore metastore.Metastore
	offsets   *consumer.InFlight
	logs      *Logs
	logger    *slog.Logger
	selfID    string
}

// NewSnapshotter wires a Snapshotter.
func NewSnapshotter(ms metastore.Metastore, offsets *consumer.InFlight, logs *Logs, logger *slog.Logger, selfID string) *Snapshotter {
	return &Snapshotter{
		metastore: ms,
		offsets:   offsets,
		logs:      logs,
		logger:    logger,
		selfID:    selfID,
	}
}

// Snapshot returns the current runtime state of every topic and
// partition. It is the data source for the metrics package's lag
// poller; the call is intentionally read-only and does not advance
// or mutate any state.
//
// Errors from individual partition lookups are NOT surfaced — a
// missing or transient partition is omitted from the result rather
// than failing the whole snapshot, so a single broken partition
// can't blind operators to the rest of the system.
func (s *Snapshotter) Snapshot(ctx context.Context) ([]metrics.TopicSnapshot, error) {
	// Limit=0 is the unpaginated, cached path — appropriate for the
	// poller, which always wants every topic in one shot.
	topics, _, err := s.metastore.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return nil, err
	}

	out := make([]metrics.TopicSnapshot, 0, len(topics))
	for _, t := range topics {
		ts := metrics.TopicSnapshot{
			Topic:      t.Name,
			Partitions: make([]metrics.PartitionSnapshot, 0, t.Partitions),
		}
		for i := 0; i < t.Partitions; i++ {
			ps, ok := s.partitionSnapshot(t.Name, i)
			if !ok {
				continue
			}
			ts.Partitions = append(ts.Partitions, ps)
		}
		out = append(out, ts)
	}
	return out, nil
}

// partitionSnapshot builds the snapshot for one partition, or reports
// ok=false when the partition should be omitted: not locally owned, or
// its log isn't open on this node.
func (s *Snapshotter) partitionSnapshot(topicName string, idx int) (metrics.PartitionSnapshot, bool) {
	if s.selfID != "" {
		assignments, ok := s.metastore.(partitionAssignmentReader)
		if ok {
			// Runtime metrics are intentionally local. In the WAL-first design,
			// a pod only opens and reports partition logs it currently owns.
			// Cross-node aggregation belongs in Prometheus/Grafana.
			assignment, err := assignments.GetAssignment(topicName, idx)
			if err != nil || assignment.OwnerID != s.selfID {
				return metrics.PartitionSnapshot{}, false
			}
		}
	}

	// Peek, never Get: a metrics poll must not lazily open (and mkdir) a
	// partition log. Opening here would resurrect directories for a topic
	// being deleted and report partitions this node has never served.
	log, ok := s.logs.Peek(topicName, idx)
	if !ok {
		return metrics.PartitionSnapshot{}, false
	}

	committed := s.offsets.Next(topicName, idx)

	logStart := log.OldestOffset()
	logEnd := log.NextOffset()
	inFlight, ackedAhead := s.offsets.Snapshot(topicName, idx)

	ps := metrics.PartitionSnapshot{
		Partition:       idx,
		LogStartOffset:  logStart,
		LogEndOffset:    logEnd,
		SegmentCount:    log.SegmentCount(),
		SizeBytes:       log.SizeBytes(),
		CommittedOffset: committed,
		InFlightSize:    inFlight,
		AckedAheadSize:  ackedAhead,
	}

	if committed < logStart {
		ps.Dropped = logStart - committed
	}

	if committed < logEnd && committed >= logStart {
		if mt, ok := log.SegmentMTimeForOffset(committed); ok {
			ps.OldestUnconsumedAt = mt
		}
	}

	return ps, true
}
