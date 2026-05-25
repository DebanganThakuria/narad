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
func (f *InFlight) CommitHandle(topic string, partition int, offset, nonce int64) error {
	sh := f.getShard(topic, partition)
	if sh == nil {
		return ErrHandleStale
	}

	sh.mu.Lock()
	rsv, ok := sh.entries[offset]
	if !ok || rsv.nonce != nonce {
		sh.mu.Unlock()
		return ErrHandleStale
	}

	if offset == sh.committed+1 {
		advance := offset
		delete(sh.entries, advance)
		for {
			next := advance + 1
			if _, ok := sh.ackedAhead[next]; !ok {
				break
			}
			delete(sh.ackedAhead, next)
			advance = next
		}
		sh.committed = advance
		sh.mu.Unlock()
		if f.onCommit != nil {
			f.onCommit(topic, partition, advance)
		}
		return nil
	}

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
