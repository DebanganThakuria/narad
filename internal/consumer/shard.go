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

	// scanCursor is the lowest offset above the committed frontier that has
	// never been handed out. Every offset in [committed+1, scanCursor) has
	// been reserved at least once, so it is currently reserved, resolved
	// (ackedAhead/corrupt), or released back into freeSet.
	//
	// scanCursor plus freeSet replace what used to be a linear rescan from
	// committed+1 on every ReserveNext. That scan was O(outstanding
	// reservations) — with three map lookups per offset — and it ran while
	// holding sh.mu, so every consumer on a partition serialised behind a
	// lock whose hold time grew with the in-flight depth.
	scanCursor int64

	// freeSet holds offsets below scanCursor whose reservation was dropped
	// without being resolved (visibility-timeout expiry or an explicit
	// release), so they are deliverable again. freeSet is authoritative.
	//
	// freeHeap orders them so the lowest is found in O(1). It may contain
	// stale offsets — re-reserving one leaves its heap slot behind rather
	// than paying an O(n) removal — which are discarded on pop by checking
	// freeSet, and compacted away once they outnumber the live set.
	freeSet  map[int64]struct{}
	freeHeap offsetHeap

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
		scanCursor:    committed + 1,
		freeSet:       make(map[int64]struct{}),
		maxInFlight:   caps.MaxInFlight,
		maxAckedAhead: caps.MaxAckedAhead,
	}
}

// nextDeliverableLocked returns the lowest offset above the committed
// frontier that is neither reserved nor resolved: the lowest live entry
// in freeSet, or scanCursor when nothing has been released. Stale heap
// slots (offsets re-reserved since being pushed) are discarded as they
// surface. Must hold sh.mu.
func (sh *partitionShard) nextDeliverableLocked() int64 {
	for sh.freeHeap.Len() > 0 {
		off := sh.freeHeap[0]
		if _, live := sh.freeSet[off]; live {
			return off
		}
		heap.Pop(&sh.freeHeap)
	}
	return sh.scanCursor
}

// takeOffsetLocked records that off has been handed out, so the next
// lookup does not return it again. Must hold sh.mu.
func (sh *partitionShard) takeOffsetLocked(off int64) {
	if _, free := sh.freeSet[off]; free {
		// The heap slot is left behind deliberately; nextDeliverableLocked
		// discards it and compactFreeLocked reclaims the space.
		delete(sh.freeSet, off)
		return
	}
	if off == sh.scanCursor {
		sh.scanCursor++
	}
}

// releaseOffsetLocked makes off deliverable again after its reservation
// was dropped WITHOUT being resolved — an expired visibility window or an
// explicit release. Resolved offsets (acked-ahead, corrupt-skipped) must
// not come through here: they are permanently undeliverable.
//
// Offsets at or below the frontier are ignored. That case cannot arise —
// the frontier only advances over resolved offsets, so it can never pass
// a free one — but the guard keeps the invariant local and cheap.
// Must hold sh.mu.
func (sh *partitionShard) releaseOffsetLocked(off int64) {
	if off <= sh.committed || off >= sh.scanCursor {
		return
	}
	if _, dup := sh.freeSet[off]; dup {
		return
	}
	sh.freeSet[off] = struct{}{}
	heap.Push(&sh.freeHeap, off)
}

// compactFreeLocked rebuilds the free heap from freeSet once stale slots
// dominate it, mirroring compactExpiryLocked. Without this, a partition
// that repeatedly releases and re-reserves the same offsets would grow
// the heap without bound even though the live free set stays small.
// Must hold sh.mu.
func (sh *partitionShard) compactFreeLocked() {
	if len(sh.freeHeap) <= 2*len(sh.freeSet)+heapCompactSlack {
		return
	}
	rebuilt := sh.freeHeap[:0]
	for off := range sh.freeSet {
		rebuilt = append(rebuilt, off)
	}
	sh.freeHeap = rebuilt
	heap.Init(&sh.freeHeap)
}

// purgeExpiredLocked removes expired reservations from entries and the
// heap, returning how many live reservations were released (i.e. offsets
// that became redeliverable). Must be called with sh.mu held. Entries
// that no longer describe the live reservation are silently discarded
// and not counted: a nonce mismatch means the offset was re-reserved
// since the heap entry was pushed, and an expiry mismatch means the
// reservation was extended (ExtendHandle keeps the nonce but supersedes
// the old deadline with a fresh heap entry).
func (sh *partitionShard) purgeExpiredLocked(now int64) int {
	released := 0
	for sh.expiry.Len() > 0 && sh.expiry[0].expiresAtUnixMs <= now {
		e := heap.Pop(&sh.expiry).(expiryEntry)
		if rsv, ok := sh.entries[e.offset]; ok && rsv.nonce == e.nonce && rsv.expiresAtUnixMs == e.expiresAtUnixMs {
			delete(sh.entries, e.offset)
			// The window lapsed without an ack, so the offset goes back into
			// the deliverable set rather than being resolved.
			sh.releaseOffsetLocked(e.offset)
			released++
		}
	}
	sh.compactExpiryLocked()
	sh.compactFreeLocked()
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

// offsetHeap is a min-heap of offsets, used to find the lowest released
// offset without rescanning the reservation table.
type offsetHeap []int64

func (h offsetHeap) Len() int           { return len(h) }
func (h offsetHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h offsetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *offsetHeap) Push(x any)        { *h = append(*h, x.(int64)) }
func (h *offsetHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
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
