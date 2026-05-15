package metrics

import (
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// StorageRecorder returns a storage.MetricsRecorder bound to one
// (topic, partition) pair. Construct one per partition log at
// open-time and pass it via storage.Options.Metrics.
//
// Returns nil if m is nil so callers can pass the result through to
// storage without an outer nil check — storage.Options.Metrics
// already handles nil as "no instrumentation".
func (m *Metrics) StorageRecorder(topic string, partition int) storage.MetricsRecorder {
	if m == nil {
		return nil
	}
	return &storageRecorder{
		m:         m,
		topic:     topic,
		partition: strconv.Itoa(partition),
	}
}

type storageRecorder struct {
	m         *Metrics
	topic     string
	partition string
}

func (r *storageRecorder) ObserveFlush(duration time.Duration, bytesWritten int64) {
	r.m.FlushDurationSeconds.WithLabelValues(r.topic, r.partition).Observe(duration.Seconds())
	r.m.FlushBytesTotal.WithLabelValues(r.topic, r.partition).Add(float64(bytesWritten))
}

func (r *storageRecorder) ObserveFsync(duration time.Duration) {
	r.m.FsyncDurationSeconds.WithLabelValues(r.topic, r.partition).Observe(duration.Seconds())
}

func (r *storageRecorder) IncSegmentRolled() {
	r.m.SegmentsRolledTotal.WithLabelValues(r.topic, r.partition).Inc()
}

func (r *storageRecorder) IncRetentionDeletion(reason string, bytesDeleted, messagesDeleted int64) {
	r.m.RetentionDeletionsTotal.WithLabelValues(r.topic, r.partition, reason).Inc()
	r.m.RetentionBytesDeleted.WithLabelValues(r.topic, r.partition, reason).Add(float64(bytesDeleted))
	r.m.RetentionMessagesDeleted.WithLabelValues(r.topic, r.partition, reason).Add(float64(messagesDeleted))
}

func (r *storageRecorder) ObserveRetentionRun(duration time.Duration) {
	r.m.RetentionRunSeconds.WithLabelValues(r.topic, r.partition).Observe(duration.Seconds())
}

func (r *storageRecorder) IncSegmentScanned() {
	r.m.SegmentsScannedAtBoot.WithLabelValues(r.topic, r.partition).Inc()
}
