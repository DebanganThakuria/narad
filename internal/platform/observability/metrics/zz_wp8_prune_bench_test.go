package metrics

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkWP8StorageRecorderObserve is the per-flush, per-fsync cost
// of the storage recorder: the path every commit batch takes.
func BenchmarkWP8StorageRecorderObserve(b *testing.B) {
	m := New(prometheus.NewRegistry())
	r := m.StorageRecorder("orders", 3)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		r.ObserveFlush(time.Millisecond, 4096)
		r.ObserveFsync(time.Millisecond)
	}
}

// BenchmarkWP8PartitionCounters is the per-message counter lookup on
// the produce and consume paths.
func BenchmarkWP8PartitionCounters(b *testing.B) {
	m := New(prometheus.NewRegistry())
	m.PartitionCounters("orders", 3)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		pc := m.PartitionCounters("orders", 3)
		pc.MessagesConsumed.Inc()
	}
}

// BenchmarkWP8PruneOrphanedTopics is the per-tick cost the poller now
// pays when nothing needs re-pruning: one pass over a node's counter
// cache (100 topics x 100 partitions) and its recorder topics.
func BenchmarkWP8PruneOrphanedTopics(b *testing.B) {
	m := New(prometheus.NewRegistry())
	current := make(map[string]struct{})
	for tp := range 100 {
		name := "topic-" + strconv.Itoa(tp)
		current[name] = struct{}{}
		for p := range 100 {
			m.PartitionCounters(name, p)
			m.StorageRecorder(name, p)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		m.pruneOrphanedTopics(current, m.snapshotEpoch())
	}
}
