package storage

import "time"

// MetricsRecorder is the storage layer's plug for observability. It is
// intentionally tiny and defined here (rather than in an observability
// package) so storage stays decoupled from any specific metrics
// implementation. Pass nil to disable.
//
// Implementations are expected to bake in topic/partition labels at
// construction time — storage only knows it has "a recorder for this
// Log" and never deals with labels itself.
type MetricsRecorder interface {
	// ObserveFlush records one drainOnce → segment write.
	ObserveFlush(duration time.Duration, frameBytes int64)

	// ObserveFsync records one segment.Sync() call.
	ObserveFsync(duration time.Duration)

	// ObserveHighWatermarkPersist records one attempt to persist the
	// durable high-watermark metadata file.
	ObserveHighWatermarkPersist(duration time.Duration, outcome string)

	// IncRetentionDeletion fires once per segment removed by retention.
	// reason is "age" or "bytes". messagesDeleted = nextOffset -
	// baseOffset for that segment.
	IncRetentionDeletion(reason string, bytesDeleted, messagesDeleted int64)

	// ObserveRetentionRun records one sweep, whether or not anything
	// was deleted.
	ObserveRetentionRun(duration time.Duration)
}
