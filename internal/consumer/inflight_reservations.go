package consumer

import (
	"container/heap"
	"context"
	"time"
)

// ReserveNext finds the lowest unreserved offset past the committed
// frontier, marks it in-flight, and returns it with a nonce.
func (f *InFlight) ReserveNext(ctx context.Context, topic string, partition int, visibilityTimeout time.Duration, logTail int64) (ReserveResult, error) {
	sh, err := f.getOrCreate(ctx, topic, partition)
	if err != nil {
		return ReserveResult{}, err
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := f.now()
	sh.purgeExpired(now)

	if len(sh.entries) >= sh.maxInFlight {
		return ReserveResult{SkipReason: "cap"}, nil
	}

	next := sh.committed + 1
	if next >= logTail {
		return ReserveResult{SkipReason: "empty"}, nil
	}

	for off := next; off < logTail; off++ {
		if _, ok := sh.entries[off]; ok {
			continue
		}
		if _, ok := sh.ackedAhead[off]; ok {
			continue
		}
		if _, ok := sh.corrupt[off]; ok {
			continue // permanently unreadable; never re-reserve a poison offset
		}
		nonce := sh.nonceSeq.Add(1)
		exp := now + visibilityTimeout.Milliseconds()
		sh.entries[off] = reservation{expiresAtUnixMs: exp, nonce: nonce}
		heap.Push(&sh.expiry, expiryEntry{offset: off, expiresAtUnixMs: exp, nonce: nonce})
		return ReserveResult{
			Reserved:        true,
			Offset:          off,
			Nonce:           nonce,
			ExpiresAtUnixMs: exp,
		}, nil
	}

	return ReserveResult{SkipReason: "all_reserved"}, nil
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
	sh := f.getShard(topic, partition)
	if sh == nil {
		return ErrHandleStale
	}

	sh.mu.Lock()
	sh.purgeExpired(f.now())
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

	if _, already := sh.ackedAhead[offset]; !already {
		if sh.aheadFullLocked() {
			sh.mu.Unlock()
			return ErrAckedAheadFull
		}
		sh.ackedAhead[offset] = struct{}{}
	}
	delete(sh.entries, offset)
	sh.mu.Unlock()
	f.notifyRelease(topic, partition)
	return nil
}

// notifyRelease invokes the release notifier for (topic, partition), if
// one is registered. Callers must NOT hold any shard lock — the
// notifier may call back into log wake-up paths (deadlock discipline
// documented on SetReleaseNotifier and ReleaseFunc).
func (f *InFlight) notifyRelease(topic string, partition int) {
	if notify := f.releaseNotifier(); notify != nil {
		notify(topic, partition)
	}
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
	sh := f.getShard(topic, partition)
	if sh == nil {
		return ErrHandleStale
	}

	sh.mu.Lock()
	sh.purgeExpired(f.now())
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

	// Ahead of the frontier: record as corrupt so ReserveNext skips it and the
	// frontier collapses past it once it is reached.
	if _, already := sh.corrupt[offset]; !already {
		if sh.aheadFullLocked() {
			sh.mu.Unlock()
			return ErrAckedAheadFull
		}
		sh.corrupt[offset] = struct{}{}
	}
	delete(sh.entries, offset)
	sh.mu.Unlock()
	f.notifyRelease(topic, partition)
	return nil
}

// advanceCommittedLocked moves the committed frontier forward across the
// contiguous run of already-resolved offsets immediately after it — offsets
// acked out of order (ackedAhead) and offsets skipped as corrupt (corrupt) —
// and returns the new committed offset. Must hold sh.mu.
func (sh *partitionShard) advanceCommittedLocked() int64 {
	for {
		next := sh.committed + 1
		if _, ok := sh.ackedAhead[next]; ok {
			delete(sh.ackedAhead, next)
			sh.committed = next
			continue
		}
		if _, ok := sh.corrupt[next]; ok {
			delete(sh.corrupt, next)
			sh.committed = next
			continue
		}
		break
	}
	return sh.committed
}

// aheadFullLocked reports whether the bounded "ahead of frontier" state
// (acked-ahead + corrupt-skipped) has hit its cap. Must hold sh.mu.
func (sh *partitionShard) aheadFullLocked() bool {
	return len(sh.ackedAhead)+len(sh.corrupt) >= sh.maxAckedAhead
}
