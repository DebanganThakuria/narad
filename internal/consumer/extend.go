package consumer

import (
	"container/heap"
	"time"
)

// ExtendHandle renews a live reservation's visibility window to
// now + visibilityTimeout and returns the new expiry (Unix ms). The
// nonce is unchanged — the consumer keeps acking with the same receipt
// handle. Validation matches CommitHandle exactly: an expired or
// superseded handle fails with ErrHandleStale, so a consumer whose
// lease already lapsed (and whose message may have been redelivered)
// can never revive it.
//
// The renewed deadline is pushed as a new expiry-heap entry; the old
// entry for the same reservation is skipped when popped because its
// expiry no longer matches the live reservation (see
// purgeExpiredLocked).
func (f *InFlight) ExtendHandle(topic string, partition int, offset, nonce int64, visibilityTimeout time.Duration) (int64, error) {
	sh := f.shard(topic, partition)
	if sh == nil {
		return 0, ErrHandleStale
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := f.now()
	sh.purgeExpiredLocked(now)

	rsv, ok := sh.entries[offset]
	if !ok || rsv.nonce != nonce {
		return 0, ErrHandleStale
	}

	exp := now + visibilityTimeout.Milliseconds()
	sh.entries[offset] = reservation{expiresAtUnixMs: exp, nonce: nonce}
	heap.Push(&sh.expiry, expiryEntry{offset: offset, expiresAtUnixMs: exp, nonce: nonce})
	return exp, nil
}

// ReleaseHandle relinquishes a live reservation immediately (negative
// ack): the offset becomes redeliverable right away instead of waiting
// out the visibility timeout, and blocked long-pollers are woken.
// Nothing is committed — the message is simply back in the queue.
// Validation matches CommitHandle: an expired or superseded handle
// fails with ErrHandleStale.
func (f *InFlight) ReleaseHandle(topic string, partition int, offset, nonce int64) error {
	sh := f.shard(topic, partition)
	if sh == nil {
		return ErrHandleStale
	}

	sh.mu.Lock()
	sh.purgeExpiredLocked(f.now())
	rsv, ok := sh.entries[offset]
	if !ok || rsv.nonce != nonce {
		sh.mu.Unlock()
		return ErrHandleStale
	}
	// The stale heap entry left behind is skipped when popped: the
	// offset is either unreserved (no entries hit) or re-reserved
	// under a new nonce by then.
	delete(sh.entries, offset)
	sh.mu.Unlock()

	f.notifyRelease(topic, partition)
	return nil
}
