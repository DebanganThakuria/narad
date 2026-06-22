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

func (r *storageRecorder) ObserveAppend(operation string, duration time.Duration, outcome string, records int, bytes int64) {
	r.m.ObserveHotPathStage("storage", operation, "append", outcome, duration)
}

func (r *storageRecorder) ObserveRead(duration time.Duration, source string, outcome string) {
	r.m.ObserveHotPathStage("storage", "read", source, outcome, duration)
}

func (r *storageRecorder) ObserveFlush(duration time.Duration, records int, payloadBytes int64, frameBytes int64) {
	r.m.FlushDurationSeconds.WithLabelValues(r.topic, r.partition).Observe(duration.Seconds())
	r.m.FlushBytesTotal.WithLabelValues(r.topic, r.partition).Add(float64(frameBytes))
	r.m.FlushRecordsPerFrame.WithLabelValues(r.topic, r.partition).Observe(float64(records))
	r.m.FlushPayloadBytes.WithLabelValues(r.topic, r.partition).Observe(float64(payloadBytes))
	r.m.FlushFrameBytes.WithLabelValues(r.topic, r.partition).Observe(float64(frameBytes))
}

func (r *storageRecorder) SetBufferStats(records int, bytes int64) {
	r.m.BufferRecords.WithLabelValues(r.topic, r.partition).Set(float64(records))
	r.m.BufferBytes.WithLabelValues(r.topic, r.partition).Set(float64(bytes))
}

func (r *storageRecorder) SetSegmentIndexStats(entries int) {
	r.m.SegmentIndexEntries.WithLabelValues(r.topic, r.partition).Set(float64(entries))
}

func (r *storageRecorder) ObserveFsync(duration time.Duration) {
	r.m.FsyncDurationSeconds.WithLabelValues(r.topic, r.partition).Observe(duration.Seconds())
}

func (r *storageRecorder) ObserveHighWatermarkPersist(duration time.Duration, outcome string) {
	r.m.HighWatermarkPersistSeconds.WithLabelValues(r.topic, r.partition, outcome).Observe(duration.Seconds())
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
