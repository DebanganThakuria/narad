package consumer

import (
	"context"
	"sync"
	"time"
)

// InFlight wraps an OffsetTracker, adding message invisibility: once
// ReserveNext delivers an offset, it remains invisible until either the
// visibility timeout expires or Commit removes it.
//
// Per-partition ordering is preserved — if the next uncommitted offset is
// in-flight, ReserveNext returns not-found so callers can try other
// partitions.
type InFlight struct {
	inner   OffsetTracker
	mu      sync.RWMutex
	entries map[string]map[int]map[int64]time.Time // topic -> partition -> offset -> expiresAt
}

func NewInFlight(inner OffsetTracker) *InFlight {
	return &InFlight{
		inner:   inner,
		entries: make(map[string]map[int]map[int64]time.Time),
	}
}

// Next delegates to the inner tracker. It returns committed+1 regardless
// of in-flight state — callers should use ReserveNext for consumption.
func (f *InFlight) Next(ctx context.Context, topic string, partition int) (int64, error) {
	return f.inner.Next(ctx, topic, partition)
}

// Commit removes the offset from the in-flight set and delegates to the
// inner tracker. Idempotent; removing a non-existent entry is a no-op.
func (f *InFlight) Commit(ctx context.Context, topic string, partition int, offset int64) error {
	f.mu.Lock()
	f.removeLocked(topic, partition, offset)
	f.mu.Unlock()
	return f.inner.Commit(ctx, topic, partition, offset)
}

// ReserveNext atomically checks the next uncommitted offset for
// deliverability and marks it in-flight. Returns -1, false if:
//   - The log is empty (committed == logTail), or
//   - The next offset is already in-flight with an unexpired timeout
//
// Callers should skip the partition on false and try the next one.
func (f *InFlight) ReserveNext(ctx context.Context, topic string, partition int, visibilityTimeout time.Duration, logTail int64) (int64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	next, err := f.inner.Next(ctx, topic, partition)
	if err != nil {
		return -1, false, err
	}
	if next >= logTail {
		return -1, false, nil // partition empty / fully consumed
	}

	expiresAt, inFlight := f.getLocked(topic, partition, next)
	if inFlight && time.Now().Before(expiresAt) {
		return -1, false, nil // still invisible
	}

	expiresAt = time.Now().Add(visibilityTimeout)
	f.setLocked(topic, partition, next, expiresAt)
	return next, true, nil
}

func (f *InFlight) getLocked(topic string, partition int, offset int64) (time.Time, bool) {
	pm, ok := f.entries[topic]
	if !ok {
		return time.Time{}, false
	}
	om, ok := pm[partition]
	if !ok {
		return time.Time{}, false
	}
	exp, ok := om[offset]
	return exp, ok
}

func (f *InFlight) setLocked(topic string, partition int, offset int64, expiresAt time.Time) {
	pm, ok := f.entries[topic]
	if !ok {
		pm = make(map[int]map[int64]time.Time)
		f.entries[topic] = pm
	}
	om, ok := pm[partition]
	if !ok {
		om = make(map[int64]time.Time)
		pm[partition] = om
	}
	om[offset] = expiresAt
}

func (f *InFlight) removeLocked(topic string, partition int, offset int64) {
	pm, ok := f.entries[topic]
	if !ok {
		return
	}
	om, ok := pm[partition]
	if !ok {
		return
	}
	delete(om, offset)
}