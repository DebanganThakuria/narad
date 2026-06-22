package runtime

import (
	"context"
	"log/slog"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// Snapshotter produces the read-only inventory of every topic and
// partition consumed by the metrics poller. Constructed once at
// broker startup and embedded into the broker facade so its Snapshot
// method satisfies the Broker interface.
type partitionAssignmentReader interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
}

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
			ps, ok := s.partitionSnapshot(ctx, t.Name, i)
			if !ok {
				continue
			}
			ts.Partitions = append(ts.Partitions, ps)
		}
		out = append(out, ts)
	}
	return out, nil
}

func (s *Snapshotter) partitionSnapshot(ctx context.Context, topicName string, idx int) (metrics.PartitionSnapshot, bool) {
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

	log, err := s.logs.Get(topicName, idx)
	if err != nil {
		s.logger.Debug("snapshot: partition log open failed", "topic", topicName, "partition", idx, "err", err)
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
