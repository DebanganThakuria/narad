package consumer

import (
	"container/heap"
	"sync"
	"sync/atomic"
)

const (
	// initialExpiryHeapCap bounds the expiry heap's initial allocation
	// so shards with huge MaxInFlight don't preallocate it all up front.
	initialExpiryHeapCap = 64

	// heapCompactSlack keeps small heaps from churning through rebuilds.
	heapCompactSlack = 16
)

// partitionShard holds all reservation state for one (topic, partition).
// Every field is guarded by mu except nonceSeq, which is atomic.
type partitionShard struct {
	mu        sync.Mutex
	committed int64 // last committed offset; -1 = none committed yet
	entries   map[int64]reservation
	expiry    expiryHeap // min-heap by expiresAtUnixMs for proactive eviction

	// ackedAhead holds offsets acked out of order, past the committed
	// frontier. The frontier collapses over them once the gap closes.
	ackedAhead map[int64]struct{}

	// corrupt holds offsets skipped because their on-disk frame is permanently
	// unreadable (corruption). Like ackedAhead they are "resolved" offsets the
	// committed frontier may advance over, but they are skipped data (lost),
	// not delivered. ReserveNext never re-reserves them.
	corrupt map[int64]struct{}

	nonceSeq      atomic.Int64
	maxInFlight   int
	maxAckedAhead int
}

type reservation struct {
	expiresAtUnixMs int64
	nonce           int64
}

type shardKey struct {
	topic     string
	partition int
}

func newPartitionShard(committed int64, caps Caps) *partitionShard {
	return &partitionShard{
		committed:     committed,
		entries:       make(map[int64]reservation),
		expiry:        make(expiryHeap, 0, min(caps.MaxInFlight, initialExpiryHeapCap)),
		ackedAhead:    make(map[int64]struct{}),
		corrupt:       make(map[int64]struct{}),
		maxInFlight:   caps.MaxInFlight,
		maxAckedAhead: caps.MaxAckedAhead,
	}
}

// purgeExpiredLocked removes expired reservations from entries and the
// heap, returning how many live reservations were released (i.e. offsets
// that became redeliverable). Must be called with sh.mu held. Entries
// whose nonce no longer matches (re-reserved since the heap entry was
// pushed) are silently discarded and not counted.
func (sh *partitionShard) purgeExpiredLocked(now int64) int {
	released := 0
	for sh.expiry.Len() > 0 && sh.expiry[0].expiresAtUnixMs <= now {
		e := heap.Pop(&sh.expiry).(expiryEntry)
		if rsv, ok := sh.entries[e.offset]; ok && rsv.nonce == e.nonce {
			delete(sh.entries, e.offset)
			released++
		}
	}
	sh.compactExpiryLocked()
	return released
}

// compactExpiryLocked rebuilds the expiry heap from the live reservation
// set when it has accumulated many dead slots. Committed and acked-ahead
// reservations are deleted from entries but their heap slot lingers until
// its expiry timestamp passes (it is skipped on pop via the nonce check).
// Under a long visibility timeout with high throughput that backlog is
// unbounded, so periodically rebuild from entries (the authoritative live
// set) to keep heap memory proportional to live reservations.
// Must be called with sh.mu held.
func (sh *partitionShard) compactExpiryLocked() {
	if len(sh.expiry) <= 2*len(sh.entries)+heapCompactSlack {
		return
	}
	rebuilt := sh.expiry[:0]
	for offset, rsv := range sh.entries {
		rebuilt = append(rebuilt, expiryEntry{
			offset:          offset,
			expiresAtUnixMs: rsv.expiresAtUnixMs,
			nonce:           rsv.nonce,
		})
	}
	sh.expiry = rebuilt
	heap.Init(&sh.expiry)
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

// expiryEntry is one slot in the per-shard min-heap.
// nonce ties the heap entry to the specific reservation so that
// stale entries (offset re-reserved since the entry was pushed)
// are silently skipped when popped.
type expiryEntry struct {
	offset          int64
	expiresAtUnixMs int64
	nonce           int64
}

// expiryHeap is a min-heap ordered by expiresAtUnixMs.
type expiryHeap []expiryEntry

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].expiresAtUnixMs < h[j].expiresAtUnixMs }
func (h expiryHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *expiryHeap) Push(x any)        { *h = append(*h, x.(expiryEntry)) }
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
