package topics

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// partitionAssignmentReader is the optional metastore capability used
// to resolve partition ownership (implemented by *metastore.Store).
// Mirrors the same interface in broker/runtime's snapshotter.
type partitionAssignmentReader interface {
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
}

// GetTopic maps errs.ErrNotFound to ErrNotFound. Other errors
// pass through unchanged.
func (m *Manager) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := m.metastore.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, ErrNotFound
		}
		return topic.Topic{}, err
	}
	return t, nil
}

// GetTopicDetails returns the topic record plus per-partition runtime
// stats. Stats are populated only for partitions this node owns
// (lazy-opening their logs as needed); unowned partitions are read
// through the non-opening Peek accessor and otherwise report
// zero-valued stats with just Index set. Opening unowned partition
// logs here would spawn their flusher/reaper goroutines, mkdir empty
// partition dirs, and violate the owner-only invariant the metrics
// snapshotter documents (runtime/snapshot.go).
//
// The result slice always has exactly Topic.Partitions entries in
// index order: callers (the HTTP ?partition= path and the cluster
// stats RPC handler) index it positionally, and the cluster router
// merges each owner's populated entries into a complete view in
// multi-node mode. A single node owns everything, so it still reports
// full stats.
func (m *Manager) GetTopicDetails(ctx context.Context, name string) (topic.Details, error) {
	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Details{}, err
	}
	stats := make([]topic.PartitionStats, t.Partitions)
	for i := 0; i < t.Partitions; i++ {
		stats[i] = topic.PartitionStats{Index: i}
		l, err := m.partitionLogForStats(name, i)
		if err != nil {
			return topic.Details{}, err
		}
		if l == nil {
			continue
		}
		stats[i].Segments = l.SegmentCount()
		stats[i].OldestOffset = l.OldestOffset()
		stats[i].NextOffset = l.NextOffset()
		stats[i].HighWatermark = l.HighWatermark()
		stats[i].SizeBytes = l.SizeBytes()
		if mt, ok := l.OldestSegmentAt(); ok {
			stats[i].OldestSegmentAt = mt
		}
	}
	return topic.Details{Topic: t, Partitions: stats}, nil
}

// partitionLogForStats returns the log to read stats from for (name,
// idx): a lazily-opened log when this node owns the partition, an
// already-open log via Peek otherwise, or nil when the partition is
// unowned and not open locally.
func (m *Manager) partitionLogForStats(name string, idx int) (*storage.Log, error) {
	owned, err := m.ownsPartition(name, idx)
	if err != nil {
		return nil, err
	}
	if owned {
		return m.logs.Get(name, idx)
	}
	if l, ok := m.logs.Peek(name, idx); ok {
		return l, nil
	}
	return nil, nil
}

// ownsPartition reports whether this node owns (topic, idx). A manager
// without a cluster identity, or a metastore without assignment
// support, owns everything — matching the snapshotter's convention. A
// missing assignment counts as unowned so a stats query never opens a
// log for a partition nobody has claimed. Any other assignment-lookup
// failure is returned to the caller: coercing it to "unowned" would
// silently zero the stats of partitions this node actually owns.
func (m *Manager) ownsPartition(topicName string, idx int) (bool, error) {
	if m.selfID == "" {
		return true, nil
	}
	assignments, ok := m.metastore.(partitionAssignmentReader)
	if !ok {
		return true, nil
	}
	assignment, err := assignments.GetAssignment(topicName, idx)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return assignment.OwnerID == m.selfID, nil
}

// ListTopics returns topics in lexicographic order. See
// metastore.ListOptions for pagination semantics.
func (m *Manager) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	return m.metastore.ListTopics(ctx, opts)
}
