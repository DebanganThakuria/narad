package runtime

import (
	"context"
	"log/slog"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// partitionAssignmentLister is the preferred metastore capability for
// resolving partition ownership: one local read transaction per topic
// (implemented by *metastore.Store).
type partitionAssignmentLister interface {
	ListAssignments(topicName string) ([]metastore.Assignment, error)
}

// partitionAssignmentReader is the per-partition fallback used when the
// metastore only offers point lookups.
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
		owned := s.ownedPartitions(t.Name, t.Partitions)
		for i := 0; i < t.Partitions; i++ {
			if !owned(i) {
				continue
			}
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

// ownedPartitions returns a predicate reporting whether this node owns
// a partition of the topic. Runtime metrics are intentionally local: in
// the WAL-first design a pod only opens and reports partition logs it
// currently owns, and cross-node aggregation belongs in Prometheus.
// Without a node identity, or a metastore that cannot answer, every
// partition is considered owned.
//
// The assignment set is read in ONE local transaction per topic (a
// prefix scan under the metastore's read lock) rather than one per
// partition cluster-wide, which is what the poller's 5s tick used to
// cost; an unassigned partition, or a listing error, means "not owned"
// exactly as a failed per-partition lookup did.
func (s *Snapshotter) ownedPartitions(topicName string, partitions int) func(int) bool {
	if s.selfID == "" {
		return func(int) bool { return true }
	}
	if lister, ok := s.metastore.(partitionAssignmentLister); ok {
		assignments, err := lister.ListAssignments(topicName)
		if err != nil {
			return func(int) bool { return false }
		}
		owned := make([]bool, partitions)
		for _, a := range assignments {
			if a.OwnerID == s.selfID && a.Partition >= 0 && a.Partition < partitions {
				owned[a.Partition] = true
			}
		}
		return func(i int) bool { return owned[i] }
	}
	if reader, ok := s.metastore.(partitionAssignmentReader); ok {
		return func(i int) bool {
			assignment, err := reader.GetAssignment(topicName, i)
			return err == nil && assignment.OwnerID == s.selfID
		}
	}
	return func(int) bool { return true }
}

// partitionSnapshot builds the snapshot for one partition, or reports
// ok=false when the partition should be omitted because its log isn't
// open on this node.
func (s *Snapshotter) partitionSnapshot(topicName string, idx int) (metrics.PartitionSnapshot, bool) {
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
