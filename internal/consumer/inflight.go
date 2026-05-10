package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// InFlight wraps an OffsetTracker with SQS-style message invisibility,
// gap-skipping reservation, and out-of-order acknowledgement support.
//
// Per-partition state is sharded so that reservations on independent
// partitions never serialize against each other. Within a shard, the
// lock window covers only the reservation/ack book-keeping; the inner
// tracker's metastore round-trip happens outside the lock for Commit
// (idempotent persistence is order-independent) but inside the lock
// for ReserveNext (gap-scan needs a consistent committed-offset view).
//
// State is in-memory only. A broker restart effectively expires every
// reservation simultaneously and forgets the ackedAhead set, both of
// which are recoverable: outstanding reservations re-deliver via
// at-least-once; ackedAhead acks re-deliver once and the consumer's
// idempotent handler swallows the duplicate.
type InFlight struct {
	inner   OffsetTracker
	resolve CapsResolver
	shards  sync.Map // shardKey -> *partitionShard
	timeNow func() int64
}

// CapsResolver returns the per-topic in-flight caps. Called once per
// shard creation; broker triggers re-resolution via RefreshCaps when
// an operator alters a topic's caps.
type CapsResolver func(ctx context.Context, topic string) (Caps, error)

// Caps are the per-partition limits that govern an InFlight shard.
// MaxInFlight bounds simultaneously-reserved offsets; MaxAckedAhead
// bounds the sparse out-of-order ack set. Both must be > 0.
type Caps struct {
	MaxInFlight   int
	MaxAckedAhead int
}

type partitionShard struct {
	mu            sync.Mutex
	entries       map[int64]reservation
	ackedAhead    map[int64]struct{}
	nonceSeq      atomic.Int64
	maxInFlight   int
	maxAckedAhead int
}

type reservation struct {
	expiresAtUnixMs int64
	nonce           int64
}

// ReserveResult carries everything the caller needs to build a
// receipt handle plus a metric label when nothing was reserved.
type ReserveResult struct {
	Reserved        bool
	Offset          int64
	Nonce           int64
	ExpiresAtUnixMs int64
	// SkipReason is set when Reserved is false:
	//   "cap"            — partition's MaxInFlight reached
	//   "empty"          — partition has no offsets past committed
	//   "all_reserved"   — every reachable offset is currently in-flight
	SkipReason string
}

// ErrAckedAheadFull means a Commit at offset > committed+1 was
// rejected because the partition's MaxAckedAhead is already full.
// Surfaced to the HTTP layer as 503 — the head of the queue is stuck
// and the client should back off.
var ErrAckedAheadFull = errors.New("consumer: acked-ahead set is full; head of queue may be stuck")

func NewInFlight(inner OffsetTracker, resolve CapsResolver) *InFlight {
	return &InFlight{
		inner:   inner,
		resolve: resolve,
		timeNow: nowUnixMs,
	}
}

// Next returns the inner tracker's next-to-deliver offset without
// consulting the in-flight set. Read-only callers (lag metrics,
// snapshot) use this; consumption goes through ReserveNext.
func (f *InFlight) Next(ctx context.Context, topic string, partition int) (int64, error) {
	return f.inner.Next(ctx, topic, partition)
}

// ReserveNext atomically scans the partition for the lowest offset
// that is neither currently reserved-with-unexpired-exp nor sitting
// in ackedAhead, marks it reserved with a fresh nonce, and returns
// it. The reservation is invisible to other ReserveNext calls until
// either Commit clears it or the visibility timeout expires.
//
// inner.Next runs inside the shard lock so that a concurrent Commit
// cannot advance committed mid-scan and leave us re-reserving an
// offset we should have skipped.
func (f *InFlight) ReserveNext(ctx context.Context, topic string, partition int, visibilityTimeout time.Duration, logTail int64) (ReserveResult, error) {
	sh, err := f.shard(ctx, topic, partition)
	if err != nil {
		return ReserveResult{}, err
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if len(sh.entries) >= sh.maxInFlight {
		return ReserveResult{SkipReason: "cap"}, nil
	}

	next, err := f.inner.Next(ctx, topic, partition)
	if err != nil {
		return ReserveResult{}, err
	}
	if next >= logTail {
		return ReserveResult{SkipReason: "empty"}, nil
	}

	now := f.timeNow()
	for off := next; off < logTail; off++ {
		if rsv, ok := sh.entries[off]; ok && rsv.expiresAtUnixMs > now {
			continue
		}
		if _, ok := sh.ackedAhead[off]; ok {
			continue
		}
		nonce := sh.nonceSeq.Add(1)
		exp := now + visibilityTimeout.Milliseconds()
		sh.entries[off] = reservation{expiresAtUnixMs: exp, nonce: nonce}
		return ReserveResult{
			Reserved:        true,
			Offset:          off,
			Nonce:           nonce,
			ExpiresAtUnixMs: exp,
		}, nil
	}
	return ReserveResult{SkipReason: "all_reserved"}, nil
}

// Commit acknowledges (topic, partition, offset). Three branches:
//
//   - offset <= committed:    drop (idempotent re-ack).
//   - offset == committed+1:  advance committed, then walk forward
//     through ackedAhead/entries collapsing any contiguous run.
//     Persist the new high-water via inner.Commit.
//   - offset > committed+1:   record in ackedAhead. Reject with
//     ErrAckedAheadFull if the cap is already reached. inner.Commit
//     is NOT called — a sparse ack does not advance persistent state.
//
// Persistence happens outside the shard lock; MetastoreBacked.Commit
// is order-independent (offset > current is the only write condition)
// so a stale-out-of-order persist is safely dropped.
func (f *InFlight) Commit(ctx context.Context, topic string, partition int, offset int64) error {
	committedSnapshot, err := f.inner.Next(ctx, topic, partition)
	if err != nil {
		return err
	}
	committed := committedSnapshot - 1 // committed = last persisted offset; Next is committed+1

	if offset <= committed {
		// Idempotent: nothing to do, but clear any stale reservation.
		sh, _ := f.shard(ctx, topic, partition)
		if sh != nil {
			sh.mu.Lock()
			delete(sh.entries, offset)
			sh.mu.Unlock()
		}
		return nil
	}

	sh, err := f.shard(ctx, topic, partition)
	if err != nil {
		return err
	}

	sh.mu.Lock()
	if offset == committed+1 {
		advance := offset
		delete(sh.entries, advance)
		for {
			next := advance + 1
			if _, ok := sh.ackedAhead[next]; !ok {
				break
			}
			delete(sh.ackedAhead, next)
			delete(sh.entries, next)
			advance = next
		}
		sh.mu.Unlock()
		return f.inner.Commit(ctx, topic, partition, advance)
	}

	// offset > committed+1: ahead path.
	if _, already := sh.ackedAhead[offset]; !already {
		if len(sh.ackedAhead) >= sh.maxAckedAhead {
			sh.mu.Unlock()
			return ErrAckedAheadFull
		}
		sh.ackedAhead[offset] = struct{}{}
	}
	delete(sh.entries, offset)
	sh.mu.Unlock()
	return nil
}

// CheckHandle verifies that the encoded reservation in h is still
// active for (topic, partition, offset). Returns nil if the caller
// may proceed to Commit; ErrHandleHMACMismatch-class errors otherwise.
//
// Verification logic:
//   - shard lookup of entries[offset] under lock
//   - if absent: handle is for a long-since committed/expired offset
//   - if present but nonce differs: re-reserved by another consumer
//
// In both negative cases the caller should map to HTTP 410 (Gone).
func (f *InFlight) CheckHandle(ctx context.Context, topic string, partition int, offset, nonce int64) error {
	sh, err := f.shard(ctx, topic, partition)
	if err != nil {
		return err
	}
	sh.mu.Lock()
	rsv, ok := sh.entries[offset]
	sh.mu.Unlock()
	if !ok {
		return ErrHandleStale
	}
	if rsv.nonce != nonce {
		return ErrHandleStale
	}
	return nil
}

// ErrHandleStale means the handle's offset is no longer reserved with
// the same nonce — either the message was already committed, the
// visibility timeout expired and another consumer re-reserved it, or
// the broker restarted. HTTP layer maps to 410.
var ErrHandleStale = errors.New("consumer: receipt handle no longer matches an active reservation")

// Snapshot returns the current shard sizes for (topic, partition).
// Used by metrics. Returns (0, 0) if no shard exists yet — equivalent
// to "no activity".
func (f *InFlight) Snapshot(topic string, partition int) (inFlight, ackedAhead int) {
	v, ok := f.shards.Load(shardKey{topic: topic, partition: partition})
	if !ok {
		return 0, 0
	}
	sh := v.(*partitionShard)
	sh.mu.Lock()
	inFlight = len(sh.entries)
	ackedAhead = len(sh.ackedAhead)
	sh.mu.Unlock()
	return
}

// RefreshCaps re-resolves and applies the topic's caps to every
// existing shard for that topic. Called by the broker when an
// operator alters a topic's caps via the alter endpoint. Idempotent.
func (f *InFlight) RefreshCaps(ctx context.Context, topic string) error {
	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return err
	}
	if caps.MaxInFlight <= 0 || caps.MaxAckedAhead <= 0 {
		return errors.New("consumer: caps must be positive")
	}
	f.shards.Range(func(k, v any) bool {
		key := k.(shardKey)
		if key.topic != topic {
			return true
		}
		sh := v.(*partitionShard)
		sh.mu.Lock()
		sh.maxInFlight = caps.MaxInFlight
		sh.maxAckedAhead = caps.MaxAckedAhead
		sh.mu.Unlock()
		return true
	})
	return nil
}

// DropTopic removes all shards for a topic. Called by the broker on
// DeleteTopic so series and book-keeping don't leak.
func (f *InFlight) DropTopic(topic string) {
	f.shards.Range(func(k, _ any) bool {
		if k.(shardKey).topic == topic {
			f.shards.Delete(k)
		}
		return true
	})
}

// shard returns the (lazily-created) shard for (topic, partition).
// Cap resolution happens once on first creation; concurrent creators
// race through LoadOrStore and only one resolve call wins.
func (f *InFlight) shard(ctx context.Context, topic string, partition int) (*partitionShard, error) {
	key := shardKey{topic: topic, partition: partition}
	if v, ok := f.shards.Load(key); ok {
		return v.(*partitionShard), nil
	}
	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return nil, err
	}
	if caps.MaxInFlight <= 0 || caps.MaxAckedAhead <= 0 {
		return nil, errors.New("consumer: caps must be positive")
	}
	fresh := &partitionShard{
		entries:       make(map[int64]reservation),
		ackedAhead:    make(map[int64]struct{}),
		maxInFlight:   caps.MaxInFlight,
		maxAckedAhead: caps.MaxAckedAhead,
	}
	actual, _ := f.shards.LoadOrStore(key, fresh)
	return actual.(*partitionShard), nil
}

type shardKey struct {
	topic     string
	partition int
}

func nowUnixMs() int64 {
	return time.Now().UnixMilli()
}
