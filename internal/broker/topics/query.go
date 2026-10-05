package topics

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
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
// index order: the HTTP ?partition= path indexes it positionally, and
// the cluster router merges each owner's populated entries (which the
// owner serves through LocalPartitionStats) into a complete view in
// multi-node mode. A single node owns everything, so it still reports
// full stats.
func (m *Manager) GetTopicDetails(ctx context.Context, name string) (topic.Details, error) {
	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Details{}, err
	}
	// The current schema comes from the persisted history, the same
	// source the produce path validates against, so a describe on any
	// node reports what that node's replica enforces. Only the latest
	// version is read (audit schemas:5): walking and copying the whole
	// history cost one read and one copy per version on every GET.
	version, raw, err := m.latestSchema(ctx, name)
	if err != nil {
		return topic.Details{}, fmt.Errorf("topics: read latest schema: %w", err)
	}
	details := topic.Details{Topic: t}
	if version > 0 {
		details.SchemaVersion = version
		details.Schema = raw
	}
	// Directory stats are only this topic's if the directory is: a
	// node that missed the purge of a deleted same-named topic still
	// holds that incarnation's directory until an open quarantines it,
	// and its segments and high-watermark must not be described as the
	// live topic's.
	dirIsOurs, err := m.logs.TopicIncarnationMatches(name, t.ID)
	if err != nil {
		return topic.Details{}, err
	}
	stats := make([]topic.PartitionStats, t.Partitions)
	for i := range t.Partitions {
		if stats[i], err = m.partitionStats(name, i, dirIsOurs); err != nil {
			return topic.Details{}, err
		}
	}
	details.Partitions = stats
	return details, nil
}

// LocalPartitionStats returns one partition's runtime stats as this
// node sees them, exactly as GetTopicDetails reports that partition,
// without reading the topic's schema or describing its other
// partitions. The cluster's per-partition stats RPC serves it: the
// router asks each owner for one partition, and a whole describe there
// cost a schema read and a stat of every partition per call (audit
// schemas:5). A partition outside the topic's range is ErrInvalid.
func (m *Manager) LocalPartitionStats(ctx context.Context, name string, partition int) (topic.PartitionStats, error) {
	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.PartitionStats{}, err
	}
	if partition < 0 || partition >= t.Partitions {
		return topic.PartitionStats{}, fmt.Errorf("%w: partition %d of %q (the topic has %d)", ErrInvalid, partition, name, t.Partitions)
	}
	dirIsOurs, err := m.logs.TopicIncarnationMatches(name, t.ID)
	if err != nil {
		return topic.PartitionStats{}, err
	}
	return m.partitionStats(name, partition, dirIsOurs)
}

// partitionStats describes one partition without opening its log: an
// open log is read through the non-opening Peek accessor (its live
// counters); a closed one this node owns is described from its
// directory, and only when the directory is this incarnation's
// (dirIsOurs); any other partition reports zero stats with just Index
// set.
func (m *Manager) partitionStats(name string, i int, dirIsOurs bool) (topic.PartitionStats, error) {
	stats := topic.PartitionStats{Index: i}
	owned, err := m.ownsPartition(name, i)
	if err != nil {
		return topic.PartitionStats{}, err
	}
	if l, ok := m.logs.Peek(name, i); ok {
		stats.Segments = l.SegmentCount()
		stats.OldestOffset = l.OldestOffset()
		stats.NextOffset = l.NextOffset()
		stats.HighWatermark = l.HighWatermark()
		stats.SizeBytes = l.SizeBytes()
		if mt, ok := l.OldestSegmentAt(); ok {
			stats.OldestSegmentAt = mt
		}
		return stats, nil
	}
	if !owned || !dirIsOurs {
		return stats, nil
	}
	dirStats, err := storage.StatPartitionDir(storage.TopicPartitionDir(m.logs.DataDir(), name, i))
	if err != nil {
		return topic.PartitionStats{}, err
	}
	stats.Segments = dirStats.Segments
	stats.OldestOffset = dirStats.OldestOffset
	// A closed log's record tail is not recorded separately from its
	// committed frontier; report the frontier for both.
	stats.NextOffset = dirStats.HighWatermark
	stats.HighWatermark = dirStats.HighWatermark
	stats.SizeBytes = dirStats.SizeBytes
	stats.OldestSegmentAt = dirStats.OldestSegmentAt
	return stats, nil
}

// latestSchema reads the topic's latest persisted schema version and
// its bytes (version 0 when it has none): directly through the
// metastore's LatestSchema when it has one (the Store does), otherwise
// as the last entry of the whole history.
func (m *Manager) latestSchema(ctx context.Context, name string) (int, []byte, error) {
	if latest, ok := m.metastore.(schema.LatestSource); ok {
		return latest.LatestSchema(ctx, name)
	}
	history, err := schema.PersistedHistory(ctx, m.metastore, name)
	if err != nil {
		return 0, nil, err
	}
	if n := len(history); n > 0 {
		return history[n-1].Number, history[n-1].Raw, nil
	}
	return 0, nil, nil
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
