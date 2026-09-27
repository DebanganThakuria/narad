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
// fsync. When any topic's series are pruned the children are
// re-resolved on the next observation, so no increment lands on a
// detached child. When the recorder's own topic is pruned (the topic
// was deleted while its log stayed open), the recorder goes inert for
// good instead: re-resolving would re-create series the poller never
// prunes again.
func (m *Metrics) StorageRecorder(topic string, partition int) storage.MetricsRecorder {
	if m == nil {
		return nil
	}
	r := &storageRecorder{
		m:         m,
		topic:     topic,
		partition: strconv.Itoa(partition),
		born:      m.epoch.Load(),
	}
	r.resolve()
	return r
}

type storageRecorder struct {
	m         *Metrics
	topic     string
	partition string
	// born is the epoch the recorder was created at, and life the topic
	// life it belongs to (guarded by m.pruneMu). A prune that retires
	// life for recorders born before its epoch makes this one inert.
	born     uint64
	life     *topicLife
	children atomic.Pointer[storageChildren]
}

// topicLife is shared by the storage recorders of one topic name from
// the first one resolved until the topic is pruned. Guarded by
// Metrics.pruneMu.
type topicLife struct {
	// born is the epoch the life began at; pruneOrphanedTopics uses it
	// to find a life that began before a listing without its topic.
	born uint64
	// deadBefore retires every recorder born before it. A recorder
	// born at or after it (a same-named successor whose log opened
	// while the poller was between its listing and the prune) moves to
	// the topic's next life on its next resolve.
	deadBefore uint64
}

// adder is the part of prometheus.Counter the recorder uses.
type adder interface{ Add(float64) }

// discard is the child every observation of an inert recorder lands on.
type discard struct{}

func (discard) Observe(float64) {}
func (discard) Add(float64)     {}

// storageChildren is one resolved set of children, tagged with the
// prune generation it was resolved under. dead marks the inert set of
// a retired recorder.
type storageChildren struct {
	gen  uint64
	dead bool

	flushDuration prometheus.Observer
	flushBytes    adder
	fsyncDuration prometheus.Observer
	hwmPersistOK  prometheus.Observer
	hwmPersistErr prometheus.Observer
	retentionRun  prometheus.Observer

	retentionAgeBytes      adder
	retentionAgeMessages   adder
	retentionBytesBytes    adder
	retentionBytesMessages adder
}

// deadChildren returns the inert set, tagged with gen.
func deadChildren(gen uint64) *storageChildren {
	var d discard
	return &storageChildren{
		gen: gen, dead: true,
		flushDuration: d, flushBytes: d, fsyncDuration: d,
		hwmPersistOK: d, hwmPersistErr: d, retentionRun: d,
		retentionAgeBytes: d, retentionAgeMessages: d,
		retentionBytesBytes: d, retentionBytesMessages: d,
	}
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
// generation, forcing a re-resolve next time) or entirely after it. A
// recorder its topic's prune retired gets the inert set instead.
func (r *storageRecorder) resolve() *storageChildren {
	m := r.m
	m.pruneMu.Lock()
	defer m.pruneMu.Unlock()
	gen := m.pruneGeneration.Load()
	if r.life != nil && r.born < r.life.deadBefore {
		c := deadChildren(gen)
		r.children.Store(c)
		return c
	}
	// A live recorder belongs to the topic's current life, which a
	// recorder born inside a pruning tick reaches only here.
	life := m.topicLives[r.topic]
	if life == nil {
		life = &topicLife{born: m.epoch.Load()}
		m.topicLives[r.topic] = life
	}
	r.life = life
	c := &storageChildren{
		gen:           gen,
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
		if !c.dead {
			r.m.HighWatermarkPersistSeconds.WithLabelValues(r.topic, r.partition, outcome).Observe(duration.Seconds())
		}
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
		if !c.dead {
			r.m.RetentionBytesDeleted.WithLabelValues(r.topic, r.partition, reason).Add(float64(bytesDeleted))
			r.m.RetentionMessagesDeleted.WithLabelValues(r.topic, r.partition, reason).Add(float64(messagesDeleted))
		}
	}
}

func (r *storageRecorder) ObserveRetentionRun(duration time.Duration) {
	r.live().retentionRun.Observe(duration.Seconds())
}

// IncStorageError implements storage.ErrorRecorder: a poisoned log, a
// discarded uncommitted tail, or a segment the reaper could not unlink
// land in errors_total{component="storage"}.
func (r *storageRecorder) IncStorageError(kind string) {
	r.m.IncError("storage", kind)
}
