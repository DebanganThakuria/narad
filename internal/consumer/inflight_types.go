package consumer

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"

	"github.com/debanganthakuria/narad/internal/errs"
)

var (
	ErrHandleStale    = errs.ErrHandleStale
	ErrAckedAheadFull = errs.ErrAckedAheadFull
)

// CommitFunc is called after the committed-offset frontier advances for
// a partition. Implementations write the new offset to the per-partition
// .offsets log for crash-recovery durability. Errors are handled inside
// the implementation; the caller does not fail if the write fails.
type CommitFunc func(topic string, partition int, offset int64)

// CapsResolver returns per-topic in-flight limits. Called once at shard
// creation; update live shards via RefreshCaps when caps change.
type CapsResolver func(ctx context.Context, topic string) (Caps, error)

// Caps bounds per-partition in-flight state.
type Caps struct {
	MaxInFlight   int
	MaxAckedAhead int
}

// InFlight tracks in-flight message reservations per partition.
// All state is in-memory — a restart clears reservations, causing
// at-most one redelivery per message (visibility timeout).
type InFlight struct {
	mu       sync.RWMutex
	shards   map[shardKey]*partitionShard
	onCommit CommitFunc
	resolve  CapsResolver
	clockMu  sync.RWMutex
	timeNow  func() int64 // replaced in tests
}

type partitionShard struct {
	mu            sync.Mutex
	committed     int64 // last committed offset; -1 = none committed yet
	entries       map[int64]reservation
	expiry        expiryHeap // min-heap by expiresAtUnixMs for proactive eviction
	ackedAhead    map[int64]struct{}
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

// ReserveResult is returned by ReserveNext.
type ReserveResult struct {
	Reserved        bool
	Offset          int64
	Nonce           int64
	ExpiresAtUnixMs int64
	// SkipReason explains why Reserved is false:
	// "cap" — MaxInFlight reached
	// "empty" — no new offsets past committed
	// "all_reserved" — all reachable offsets currently in-flight
	SkipReason string
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

// purgeExpired removes expired reservations from entries and the heap.
// Must be called with sh.mu held. Entries whose nonce no longer matches
// (re-reserved since the heap entry was pushed) are silently discarded.
func (sh *partitionShard) purgeExpired(now int64) {
	for sh.expiry.Len() > 0 && sh.expiry[0].expiresAtUnixMs <= now {
		e := heap.Pop(&sh.expiry).(expiryEntry)
		if rsv, ok := sh.entries[e.offset]; ok && rsv.nonce == e.nonce {
			delete(sh.entries, e.offset)
		}
	}
}
