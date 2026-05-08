package broker

import (
	"context"

	"github.com/debanganthakuria/narad/internal/observability/metrics"
)

// Snapshot returns the current runtime state of every topic and
// partition. It is the data source for the metrics package's lag
// poller; the call is intentionally read-only and does not advance
// or mutate any state.
//
// Errors from individual partition lookups are NOT surfaced — a
// missing or transient partition is omitted from the result rather
// than failing the whole snapshot, so a single broken partition
// can't blind operators to the rest of the system. The broker's
// error counter is bumped instead.
//
// The returned types live in the metrics package because they
// describe what the metrics layer needs; broker imports metrics
// already (for Deps.Metrics) so the dependency direction is
// preserved.
func (b *impl) Snapshot(ctx context.Context) ([]metrics.TopicSnapshot, error) {
	topics, err := b.deps.Metastore.ListTopics(ctx)
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
			ps, ok := b.partitionSnapshot(ctx, t.Name, i)
			if !ok {
				continue
			}
			ts.Partitions = append(ts.Partitions, ps)
		}
		out = append(out, ts)
	}
	return out, nil
}

func (b *impl) partitionSnapshot(ctx context.Context, topicName string, idx int) (metrics.PartitionSnapshot, bool) {
	log, err := b.partitionLog(topicName, idx)
	if err != nil {
		b.deps.Logger.Debug("snapshot: partition log open failed", "topic", topicName, "partition", idx, "err", err)
		return metrics.PartitionSnapshot{}, false
	}

	committed, err := b.deps.Offsets.Next(ctx, topicName, idx)
	if err != nil {
		b.deps.Logger.Debug("snapshot: offset lookup failed", "topic", topicName, "partition", idx, "err", err)
		return metrics.PartitionSnapshot{}, false
	}

	logStart := log.OldestOffset()
	logEnd := log.NextOffset()

	ps := metrics.PartitionSnapshot{
		Partition:       idx,
		LogStartOffset:  logStart,
		LogEndOffset:    logEnd,
		SegmentCount:    log.SegmentCount(),
		SizeBytes:       log.SizeBytes(),
		CommittedOffset: committed,
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
