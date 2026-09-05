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
// stats. Stats are populated for partitions this node owns and for
// any partition whose log happens to be open locally; other unowned
// partitions report zero-valued stats with just Index set.
//
// A describe never opens a partition log. An open log is read through
// the non-opening Peek accessor (its live counters); a closed one is
// described from its directory (segment files plus the durable
// high-watermark file, see storage.StatPartitionDir). Opening would
// spawn flusher/reaper goroutines, mkdir empty partition dirs for
// unowned partitions, and stamp the log as active, so a monitoring
// loop that GETs topics would keep every idle log warm and idle
// eviction could never fire (runtime/evict.go, invariant 1).
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
		owned, err := m.ownsPartition(name, i)
		if err != nil {
			return topic.Details{}, err
		}
		if l, ok := m.logs.Peek(name, i); ok {
			stats[i].Segments = l.SegmentCount()
			stats[i].OldestOffset = l.OldestOffset()
			stats[i].NextOffset = l.NextOffset()
			stats[i].HighWatermark = l.HighWatermark()
			stats[i].SizeBytes = l.SizeBytes()
			if mt, ok := l.OldestSegmentAt(); ok {
				stats[i].OldestSegmentAt = mt
			}
			continue
		}
		if !owned {
			continue
		}
		dirStats, err := storage.StatPartitionDir(storage.TopicPartitionDir(m.logs.DataDir(), name, i))
		if err != nil {
			return topic.Details{}, err
		}
		stats[i].Segments = dirStats.Segments
		stats[i].OldestOffset = dirStats.OldestOffset
		// A closed log's record tail is not recorded separately from
		// its committed frontier; report the frontier for both.
		stats[i].NextOffset = dirStats.HighWatermark
		stats[i].HighWatermark = dirStats.HighWatermark
		stats[i].SizeBytes = dirStats.SizeBytes
		stats[i].OldestSegmentAt = dirStats.OldestSegmentAt
	}
	return topic.Details{Topic: t, Partitions: stats}, nil
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
