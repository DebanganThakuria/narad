package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// partitionKey identifies one cached PartitionCounters set.
type partitionKey struct {
	topic     string
	partition int
}

// PartitionCounters is the set of per-(topic, partition) counters the
// messaging engine increments on the produce and consume hot paths,
// resolved once and cached so an append or a delivery does one map
// lookup instead of a label-hash resolution (plus an Itoa) per counter.
//
// Resolve it per operation with Metrics.PartitionCounters rather than
// holding it across a topic's lifetime: when the poller prunes a
// deleted topic's series, the cached entry is dropped with them, and the
// next call re-binds live children. A pointer held across that prune
// would keep incrementing detached children that no longer appear in
// the exposition.
type PartitionCounters struct {
	MessagesProduced prometheus.Counter
	BytesProduced    prometheus.Counter
	MessagesConsumed prometheus.Counter
	BytesConsumed    prometheus.Counter
	CorruptSkipped   prometheus.Counter
}

// PartitionCounters returns the cached counter set for the pair,
// resolving and caching it on first sight (and after a prune). Returns
// nil when m is nil.
func (m *Metrics) PartitionCounters(topic string, partition int) *PartitionCounters {
	if m == nil {
		return nil
	}
	key := partitionKey{topic: topic, partition: partition}
	if v, ok := m.partitionCounters.Load(key); ok {
		return v.(*PartitionCounters)
	}
	return m.resolvePartitionCounters(key)
}

// resolvePartitionCounters binds the children under the prune lock so
// the set is resolved entirely before or entirely after any concurrent
// pruneTopicSeries, never straddling it (which would cache children the
// prune had already detached).
func (m *Metrics) resolvePartitionCounters(key partitionKey) *PartitionCounters {
	m.pruneMu.Lock()
	defer m.pruneMu.Unlock()
	if v, ok := m.partitionCounters.Load(key); ok {
		return v.(*PartitionCounters)
	}
	p := strconv.Itoa(key.partition)
	pc := &PartitionCounters{
		MessagesProduced: m.MessagesProducedTotal.WithLabelValues(key.topic, p),
		BytesProduced:    m.BytesProducedTotal.WithLabelValues(key.topic, p),
		MessagesConsumed: m.MessagesConsumedTotal.WithLabelValues(key.topic, p),
		BytesConsumed:    m.BytesConsumedTotal.WithLabelValues(key.topic, p),
		CorruptSkipped:   m.CorruptSkippedTotal.WithLabelValues(key.topic, p),
	}
	m.partitionCounters.Store(key, pc)
	return pc
}

// dropPartitionCounters removes every cached set for topic. Caller
// holds pruneMu.
func (m *Metrics) dropPartitionCounters(topic string) {
	m.partitionCounters.Range(func(k, _ any) bool {
		if k.(partitionKey).topic == topic {
			m.partitionCounters.Delete(k)
		}
		return true
	})
}
