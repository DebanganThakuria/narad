package metrics

import (
	"context"
)

// TopicSnapshot is one (topic, []partition) row returned by a
// SnapshotProvider. Defined in this package so the poller can iterate
// it without importing broker (which would cycle: broker already
// imports metrics for Deps.Metrics).
type TopicSnapshot struct {
	Topic      string
	Partitions []PartitionSnapshot
}

// PartitionSnapshot captures the runtime state of one partition that
// matters to operators: where the log starts/ends, where the consumer
// is, how many messages and segments are sitting on disk, and whether
// retention has eaten messages out from under the consumer.
//
// OldestUnconsumedAt is the file mtime of the segment containing the
// committed offset — an upper bound on "when the consumer's next
// message was last touched", not an exact produce timestamp (Narad's
// on-disk frame doesn't carry per-message timestamps). Zero value
// means the consumer is caught up, the message has been
// retention-deleted (Dropped > 0), or the segment file was
// unreadable.
type PartitionSnapshot struct {
	Partition          int
	LogStartOffset     int64
	LogEndOffset       int64
	SegmentCount       int
	SizeBytes          int64
	CommittedOffset    int64
	OldestUnconsumedAt int64
	Dropped            int64
	// InFlightSize is the count of currently-reserved offsets for this
	// partition (consumer.InFlight.entries). AckedAheadSize is the
	// count of acked-but-not-yet-contiguous offsets. Both feed gauge
	// metrics that operators watch to spot stuck heads or runaway
	// consumers.
	InFlightSize   int
	AckedAheadSize int
}

// SnapshotProvider is the slice of broker.Broker the poller needs.
// Defining it here (rather than importing broker) avoids a cycle —
// broker already imports metrics for Deps.Metrics, so we can't
// import the other way.
//
// broker.Broker satisfies this interface implicitly (Go's structural
// typing — no import needed in broker to "implement" it).
type SnapshotProvider interface {
	Snapshot(ctx context.Context) ([]TopicSnapshot, error)
}
