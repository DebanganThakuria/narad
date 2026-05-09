package consumer

import (
	"context"
	"sync"
	"time"
)

// InFlight wraps an OffsetTracker with SQS-style message invisibility.
// Once ReserveNext hands an offset to a consumer, that offset is
// invisible to subsequent ReserveNext calls until either Commit clears
// it or the visibility timeout expires.
//
// State is sharded per (topic, partition): each shard has its own
// mutex and its own offset → expiresAt map, so reservations on
// independent partitions never serialize against each other. Within a
// partition, the lock window covers only the in-flight map check —
// the inner tracker's Next call runs outside it.
//
// Expiries are stored as Unix-milliseconds (int64) rather than
// time.Time so the in-flight set is timezone-independent and trivially
// serializable if we ever persist it.
type InFlight struct {
	inner   OffsetTracker
	shards  sync.Map // shardKey -> *partitionShard
	timeNow func() int64
}

type partitionShard struct {
	mu      sync.Mutex
	entries map[int64]int64 // offset -> expiresAtUnixMs
}

func NewInFlight(inner OffsetTracker) *InFlight {
	return &InFlight{
		inner:   inner,
		timeNow: nowUnixMs,
	}
}

// Next returns the inner tracker's next-to-deliver offset. It does
// NOT consult the in-flight set — callers driving consumption should
// use ReserveNext. Next exists for read-only callers (metrics
// snapshot, lag reporting) that want committed+1 regardless of
// reservation state.
func (f *InFlight) Next(ctx context.Context, topic string, partition int) (int64, error) {
	return f.inner.Next(ctx, topic, partition)
}

// Commit clears the (topic, partition, offset) reservation and
// delegates to the inner tracker. Idempotent: clearing an unreserved
// offset is a no-op.
func (f *InFlight) Commit(ctx context.Context, topic string, partition int, offset int64) error {
	sh := f.shard(topic, partition)
	sh.mu.Lock()
	delete(sh.entries, offset)
	sh.mu.Unlock()
	return f.inner.Commit(ctx, topic, partition, offset)
}

// ReserveNext atomically claims the partition's next uncommitted
// offset for the caller. Returns (offset, true, nil) on success;
// (-1, false, nil) when the partition is empty or the next offset is
// already reserved with an unexpired timeout — caller should skip
// this partition.
//
// inner.Next runs outside the shard lock; we pay the metastore-cache
// hit unlocked, then take the shard lock only for the reserve check
// and write.
func (f *InFlight) ReserveNext(ctx context.Context, topic string, partition int, visibilityTimeout time.Duration, logTail int64) (int64, bool, error) {
	next, err := f.inner.Next(ctx, topic, partition)
	if err != nil {
		return -1, false, err
	}
	if next >= logTail {
		return -1, false, nil
	}

	sh := f.shard(topic, partition)
	now := f.timeNow()

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if exp, ok := sh.entries[next]; ok && exp > now {
		return -1, false, nil
	}
	sh.entries[next] = now + visibilityTimeout.Milliseconds()
	return next, true, nil
}

func (f *InFlight) shard(topic string, partition int) *partitionShard {
	key := shardKey{topic: topic, partition: partition}
	if v, ok := f.shards.Load(key); ok {
		return v.(*partitionShard)
	}
	fresh := &partitionShard{entries: make(map[int64]int64)}
	actual, _ := f.shards.LoadOrStore(key, fresh)
	return actual.(*partitionShard)
}

type shardKey struct {
	topic     string
	partition int
}

func nowUnixMs() int64 {
	return time.Now().UnixMilli()
}
