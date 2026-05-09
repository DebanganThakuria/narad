// Package consumer manages committed (acknowledged) offsets for
// queue-style consumption.
//
// The OffsetTracker is the broker's view of "what has the consumer
// already processed?" — the next message to deliver is at Next + 1.
package consumer

import "context"

// OffsetTracker persists per-partition consumer offsets.
// Implementations must be safe for concurrent use.
type OffsetTracker interface {
	// Next returns the offset of the next message that should be
	// delivered. For a fresh partition (no commits yet) it returns
	// 0.
	Next(ctx context.Context, topic string, partition int) (int64, error)

	// Commit advances the tracker so messages up to and including
	// `offset` are considered processed. Idempotent; commits at or
	// below the current position are a no-op.
	Commit(ctx context.Context, topic string, partition int, offset int64) error
}
