package topics

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// GetTopic maps metastore.ErrNotFound to ErrNotFound. Other errors
// pass through unchanged.
func (m *Manager) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := m.metastore.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, metastore.ErrNotFound) {
			return topic.Topic{}, ErrNotFound
		}
		return topic.Topic{}, err
	}
	return t, nil
}

// GetTopicDetails returns the topic record plus per-partition runtime
// stats. Lazy-opens each partition to read its stats.
func (m *Manager) GetTopicDetails(ctx context.Context, name string) (topic.Details, error) {
	t, err := m.GetTopic(ctx, name)
	if err != nil {
		return topic.Details{}, err
	}
	stats := make([]topic.PartitionStats, t.Partitions)
	for i := 0; i < t.Partitions; i++ {
		l, err := m.logs.Get(name, i)
		if err != nil {
			return topic.Details{}, err
		}
		ps := topic.PartitionStats{
			Index:        i,
			Segments:     l.SegmentCount(),
			OldestOffset: l.OldestOffset(),
			NextOffset:   l.NextOffset(),
			SizeBytes:    l.SizeBytes(),
		}
		if mt, ok := l.OldestSegmentAt(); ok {
			ps.OldestSegmentAt = mt
		}
		stats[i] = ps
	}
	return topic.Details{Topic: t, Partitions: stats}, nil
}

// ListTopics returns topics in lexicographic order. See
// metastore.ListOptions for pagination semantics.
func (m *Manager) ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error) {
	return m.metastore.ListTopics(ctx, opts)
}
