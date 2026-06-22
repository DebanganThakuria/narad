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
	// ObserveAppend records a single Append or AppendBatch call into
	// the in-memory buffer.
	ObserveAppend(operation string, duration time.Duration, outcome string, records int, bytes int64)

	// ObserveRead records one Read call and where the payload was found.
	ObserveRead(duration time.Duration, source string, outcome string)

	// ObserveFlush records one drainOnce → segment write.
	// payloadBytes is the logical payload size; frameBytes is the
	// on-disk frame size after codec/framing.
	ObserveFlush(duration time.Duration, records int, payloadBytes, frameBytes int64)

	// SetBufferStats records the current in-memory buffer size for
	// this log.
	SetBufferStats(records int, bytes int64)

	// SetSegmentIndexStats records the current number of hot in-memory
	// index entries retained for this log.
	SetSegmentIndexStats(entries int)

	// ObserveFsync records one segment.Sync() call.
	ObserveFsync(duration time.Duration)

	// ObserveHighWatermarkPersist records one attempt to persist the
	// durable high-watermark metadata file.
	ObserveHighWatermarkPersist(duration time.Duration, outcome string)

	// IncSegmentRolled fires when the active segment is sealed and a
	// fresh one is opened.
	IncSegmentRolled()

	// IncRetentionDeletion fires once per segment removed by retention.
	// reason is "age" or "bytes". messagesDeleted = nextOffset -
	// baseOffset for that segment.
	IncRetentionDeletion(reason string, bytesDeleted, messagesDeleted int64)

	// ObserveRetentionRun records one sweep, whether or not anything
	// was deleted.
	ObserveRetentionRun(duration time.Duration)

	// IncSegmentScanned fires once per segment file walked during
	// startup recovery.
	IncSegmentScanned()
}
