package metrics

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// StorageRecorder returns a storage.MetricsRecorder bound to one
// (topic, partition) pair. Construct one per partition log at
// open-time and pass it via storage.Options.Metrics.
//
// Returns nil if m is nil so callers can pass the result through to
// storage without an outer nil check — storage.Options.Metrics
// already handles nil as "no instrumentation".
//
// The labels are fixed for the recorder's lifetime, so every child it
// observes into is resolved once here rather than on every flush and
// fsync. If the topic's series are pruned while the recorder is alive
// (topic deleted and recreated within one poller tick), the children
// are re-resolved on the next observation so no increment lands on a
// detached child.
func (m *Metrics) StorageRecorder(topic string, partition int) storage.MetricsRecorder {
	if m == nil {
		return nil
	}
	r := &storageRecorder{
		m:         m,
		topic:     topic,
		partition: strconv.Itoa(partition),
	}
	r.resolve()
	return r
}

type storageRecorder struct {
	m         *Metrics
	topic     string
	partition string
	children  atomic.Pointer[storageChildren]
}

// storageChildren is one resolved set of children, tagged with the
// prune generation it was resolved under.
type storageChildren struct {
	gen uint64

	flushDuration prometheus.Observer
	flushBytes    prometheus.Counter
	fsyncDuration prometheus.Observer
	hwmPersistOK  prometheus.Observer
	hwmPersistErr prometheus.Observer
	retentionRun  prometheus.Observer

	retentionAgeBytes      prometheus.Counter
	retentionAgeMessages   prometheus.Counter
	retentionBytesBytes    prometheus.Counter
	retentionBytesMessages prometheus.Counter
}

// live returns the current children, re-resolving them if a prune has
// happened since they were resolved. The common path is one atomic
// load and one compare.
func (r *storageRecorder) live() *storageChildren {
	if c := r.children.Load(); c != nil && c.gen == r.m.pruneGeneration.Load() {
		return c
	}
	return r.resolve()
}

// resolve binds every child under the prune lock, so the set is either
// resolved entirely before a prune (and tagged with the older
// generation, forcing a re-resolve next time) or entirely after it.
func (r *storageRecorder) resolve() *storageChildren {
	m := r.m
	m.pruneMu.Lock()
	defer m.pruneMu.Unlock()
	c := &storageChildren{
		gen:           m.pruneGeneration.Load(),
		flushDuration: m.FlushDurationSeconds.WithLabelValues(r.topic, r.partition),
		flushBytes:    m.FlushBytesTotal.WithLabelValues(r.topic, r.partition),
		fsyncDuration: m.FsyncDurationSeconds.WithLabelValues(r.topic, r.partition),
		hwmPersistOK:  m.HighWatermarkPersistSeconds.WithLabelValues(r.topic, r.partition, "ok"),
		hwmPersistErr: m.HighWatermarkPersistSeconds.WithLabelValues(r.topic, r.partition, "error"),
		retentionRun:  m.RetentionRunSeconds.WithLabelValues(r.topic, r.partition),

		retentionAgeBytes:      m.RetentionBytesDeleted.WithLabelValues(r.topic, r.partition, "age"),
		retentionAgeMessages:   m.RetentionMessagesDeleted.WithLabelValues(r.topic, r.partition, "age"),
		retentionBytesBytes:    m.RetentionBytesDeleted.WithLabelValues(r.topic, r.partition, "bytes"),
		retentionBytesMessages: m.RetentionMessagesDeleted.WithLabelValues(r.topic, r.partition, "bytes"),
	}
	r.children.Store(c)
	return c
}

func (r *storageRecorder) ObserveFlush(duration time.Duration, frameBytes int64) {
	c := r.live()
	c.flushDuration.Observe(duration.Seconds())
	c.flushBytes.Add(float64(frameBytes))
}

func (r *storageRecorder) ObserveFsync(duration time.Duration) {
	r.live().fsyncDuration.Observe(duration.Seconds())
}

func (r *storageRecorder) ObserveHighWatermarkPersist(duration time.Duration, outcome string) {
	c := r.live()
	switch outcome {
	case "ok":
		c.hwmPersistOK.Observe(duration.Seconds())
	case "error":
		c.hwmPersistErr.Observe(duration.Seconds())
	default:
		r.m.HighWatermarkPersistSeconds.WithLabelValues(r.topic, r.partition, outcome).Observe(duration.Seconds())
	}
}

func (r *storageRecorder) IncRetentionDeletion(reason string, bytesDeleted, messagesDeleted int64) {
	c := r.live()
	switch reason {
	case "age":
		c.retentionAgeBytes.Add(float64(bytesDeleted))
		c.retentionAgeMessages.Add(float64(messagesDeleted))
	case "bytes":
		c.retentionBytesBytes.Add(float64(bytesDeleted))
		c.retentionBytesMessages.Add(float64(messagesDeleted))
	default:
		r.m.RetentionBytesDeleted.WithLabelValues(r.topic, r.partition, reason).Add(float64(bytesDeleted))
		r.m.RetentionMessagesDeleted.WithLabelValues(r.topic, r.partition, reason).Add(float64(messagesDeleted))
	}
}

func (r *storageRecorder) ObserveRetentionRun(duration time.Duration) {
	r.live().retentionRun.Observe(duration.Seconds())
}
