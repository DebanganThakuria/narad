package consumer

import (
	"container/heap"
	"context"
	"time"
)

// ReserveResult is returned by ReserveNext.
type ReserveResult struct {
	Reserved        bool
	Offset          int64
	Nonce           int64
	ExpiresAtUnixMs int64
	// SkipReason is a diagnostic label explaining why Reserved is false:
	// "cap" (MaxInFlight reached), "empty" (no new offsets past
	// committed), or "all_reserved" (all reachable offsets currently
	// in-flight). Production callers only branch on Reserved; the reason
	// exists for tests and debugging.
	SkipReason string
}

// ReserveNext finds the lowest unreserved offset past the committed
// frontier, marks it in-flight, and returns it with a nonce.
func (f *InFlight) ReserveNext(ctx context.Context, topic string, partition int, visibilityTimeout time.Duration, logTail int64) (ReserveResult, error) {
	sh, err := f.shardOrCreate(ctx, topic, partition)
	if err != nil {
		return ReserveResult{}, err
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := f.now()
	sh.purgeExpiredLocked(now)

	if len(sh.entries) >= sh.maxInFlight {
		return ReserveResult{SkipReason: "cap"}, nil
	}

	next := sh.committed + 1
	if next >= logTail {
		return ReserveResult{SkipReason: "empty"}, nil
	}

	// When the ahead-of-frontier state is at capacity, every ack except
	// the frontier hole's is doomed to ErrAckedAheadFull — handing out
	// NEW offsets from here only manufactures deliveries whose acks
	// bounce, expire, and redeliver (the ack-503 spiral observed under
	// soak: hundreds of thousands of duplicates in hours). Serve ONLY
	// the offset the frontier is waiting on; one successful ack there
	// collapses the whole acked-ahead run and normal service resumes.
	if sh.aheadFullLocked() {
		if sh.resolvedOrReservedLocked(next) {
			return ReserveResult{SkipReason: "ahead_full"}, nil
		}
		return sh.reserveLocked(next, now, visibilityTimeout), nil
	}

	for off := next; off < logTail; off++ {
		if sh.resolvedOrReservedLocked(off) {
			continue
		}
		return sh.reserveLocked(off, now, visibilityTimeout), nil
	}
	return ReserveResult{SkipReason: "all_reserved"}, nil
}

// resolvedOrReservedLocked reports whether off cannot be handed out:
// currently in-flight, already acked ahead of the frontier, or skipped
// as corrupt (a poison offset is never re-reserved). Must hold sh.mu.
func (sh *partitionShard) resolvedOrReservedLocked(off int64) bool {
	if _, ok := sh.entries[off]; ok {
		return true
	}
	if _, ok := sh.ackedAhead[off]; ok {
		return true
	}
	if _, ok := sh.corrupt[off]; ok {
		return true
	}
	return false
}

// reserveLocked records a fresh reservation for off and returns it.
// Must hold sh.mu.
func (sh *partitionShard) reserveLocked(off, now int64, visibilityTimeout time.Duration) ReserveResult {
	nonce := sh.nonceSeq.Add(1)
	exp := now + visibilityTimeout.Milliseconds()
	sh.entries[off] = reservation{expiresAtUnixMs: exp, nonce: nonce}
	heap.Push(&sh.expiry, expiryEntry{offset: off, expiresAtUnixMs: exp, nonce: nonce})
	return ReserveResult{
		Reserved:        true,
		Offset:          off,
		Nonce:           nonce,
		ExpiresAtUnixMs: exp,
	}
}

// CommitHandle verifies the nonce and commits the offset atomically.
// When the frontier advances, onCommit is called to persist the new
// committed offset to the .offsets log.
//
// Removing a live reservation frees a MaxInFlight cap slot, so on
// success the release notifier fires too: a long-poller parked because
// the partition was at cap must be woken by the ack, not left sleeping
// out its full Wait. Like the purger, the notifier is invoked only
// after all shard locks are released (see ReleaseFunc).
func (f *InFlight) CommitHandle(topic string, partition int, offset, nonce int64) error {
	return f.resolveReserved(topic, partition, offset, nonce, ackedAheadSet)
}

// SkipCorrupt advances the committed frontier past an offset whose on-disk
// frame is permanently unreadable (corruption), recording it as skipped data
// rather than delivered. It is the consumer-side escape from a poison record
// that would otherwise head-of-line-block the partition forever: the broker
// calls it only after its OWN read of the reserved offset returned a corruption
// error (storage.IsCorrupt) — the reservation nonce is required, so a client
// cannot force a skip.
//
// Skipping is irreversible data loss (narad keeps a single copy; there is no
// replica to heal from). Callers MUST record the loss observably (metric + log)
// so it is alertable, never silent. The frontier advance persists via onCommit,
// so the skip survives restart and the offset is not re-attempted.
func (f *InFlight) SkipCorrupt(topic string, partition int, offset, nonce int64) error {
	return f.resolveReserved(topic, partition, offset, nonce, corruptSet)
}

func ackedAheadSet(sh *partitionShard) map[int64]struct{} { return sh.ackedAhead }
func corruptSet(sh *partitionShard) map[int64]struct{}    { return sh.corrupt }

// resolveReserved is the shared body of CommitHandle and SkipCorrupt:
// verify the live reservation's nonce, then resolve the offset. At the
// frontier the committed offset advances (collapsing over any contiguous
// resolved run) and is persisted via onCommit; ahead of the frontier the
// offset is parked in the set selected by aheadOf (ackedAhead for acks,
// corrupt for skips), subject to the shared ahead cap. Either way the
// reservation is removed, freeing a MaxInFlight slot, so the release
// notifier fires after all shard locks are dropped.
func (f *InFlight) resolveReserved(topic string, partition int, offset, nonce int64, aheadOf func(*partitionShard) map[int64]struct{}) error {
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

	if offset == sh.committed+1 {
		delete(sh.entries, offset)
		sh.committed = offset
		advance := sh.advanceCommittedLocked()
		sh.mu.Unlock()
		if f.onCommit != nil {
			f.onCommit(topic, partition, advance)
		}
		f.notifyRelease(topic, partition)
		return nil
	}

	ahead := aheadOf(sh)
	if _, already := ahead[offset]; !already {
		if sh.aheadFullLocked() {
			sh.mu.Unlock()
			return ErrAckedAheadFull
		}
		ahead[offset] = struct{}{}
	}
	delete(sh.entries, offset)
	sh.mu.Unlock()
	f.notifyRelease(topic, partition)
	return nil
}
