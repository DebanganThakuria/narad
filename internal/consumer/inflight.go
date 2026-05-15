package consumer

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
)

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

// NewInFlight creates an InFlight tracker. onCommit may be nil (no
// persistence, useful for tests or early development).
func NewInFlight(resolve CapsResolver, onCommit CommitFunc) *InFlight {
	return &InFlight{
		shards:   make(map[shardKey]*partitionShard),
		onCommit: onCommit,
		resolve:  resolve,
		timeNow:  nowUnixMs,
	}
}

// Init seeds a partition shard with the committed offset recovered from
// the .offsets file on startup. Call before the first ReserveNext on any
// partition that has prior commit history.
func (f *InFlight) Init(ctx context.Context, topic string, partition int, committed int64) error {
	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return err
	}
	if caps.MaxInFlight <= 0 || caps.MaxAckedAhead <= 0 {
		return errors.New("consumer: caps must be positive")
	}
	f.mu.Lock()
	f.shards[shardKey{topic, partition}] = &partitionShard{
		committed:     committed,
		entries:       make(map[int64]reservation),
		expiry:        make(expiryHeap, 0, caps.MaxInFlight),
		ackedAhead:    make(map[int64]struct{}),
		maxInFlight:   caps.MaxInFlight,
		maxAckedAhead: caps.MaxAckedAhead,
	}
	f.mu.Unlock()
	return nil
}

// Next returns the next offset to deliver (committed+1). Used by the
// metrics poller to compute consumer lag.
func (f *InFlight) Next(topic string, partition int) int64 {
	f.mu.RLock()
	sh := f.shards[shardKey{topic, partition}]
	f.mu.RUnlock()
	if sh == nil {
		return 0
	}
	sh.mu.Lock()
	n := sh.committed + 1
	sh.mu.Unlock()
	return n
}

// ReserveNext finds the lowest unreserved offset past the committed
// frontier, marks it in-flight, and returns it with a nonce.
func (f *InFlight) ReserveNext(ctx context.Context, topic string, partition int, visibilityTimeout time.Duration, logTail int64) (ReserveResult, error) {
	sh, err := f.getOrCreate(ctx, topic, partition)
	if err != nil {
		return ReserveResult{}, err
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := f.timeNow()
	sh.purgeExpired(now) // evict expired entries before the cap check

	if len(sh.entries) >= sh.maxInFlight {
		return ReserveResult{SkipReason: "cap"}, nil
	}

	next := sh.committed + 1
	if next >= logTail {
		return ReserveResult{SkipReason: "empty"}, nil
	}

	for off := next; off < logTail; off++ {
		// After purgeExpired, every entry in sh.entries is unexpired.
		// No need to check expiresAtUnixMs here.
		if _, ok := sh.entries[off]; ok {
			continue // currently in-flight
		}
		if _, ok := sh.ackedAhead[off]; ok {
			continue // already acked out-of-order
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
	f.mu.RLock()
	sh := f.shards[shardKey{topic, partition}]
	f.mu.RUnlock()
	if sh == nil {
		return errs.ErrHandleStale
	}

	sh.mu.Lock()

	rsv, ok := sh.entries[offset]
	if !ok || rsv.nonce != nonce {
		sh.mu.Unlock()
		return errs.ErrHandleStale
	}

	// In-order: advance the frontier, collapsing any contiguous ackedAhead run.
	if offset == sh.committed+1 {
		advance := offset
		delete(sh.entries, advance)
		for {
			next := advance + 1
			if _, ok := sh.ackedAhead[next]; !ok {
				break
			}
			// entry for next was deleted when it was added to ackedAhead
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

	// Out-of-order: park in ackedAhead until the head catches up.
	if _, already := sh.ackedAhead[offset]; !already {
		if len(sh.ackedAhead) >= sh.maxAckedAhead {
			sh.mu.Unlock()
			return errs.ErrAckedAheadFull
		}
		sh.ackedAhead[offset] = struct{}{}
	}
	delete(sh.entries, offset)
	sh.mu.Unlock()
	return nil
}

// Snapshot returns current shard sizes for the metrics poller.
func (f *InFlight) Snapshot(topic string, partition int) (inFlight, ackedAhead int) {
	f.mu.RLock()
	sh := f.shards[shardKey{topic, partition}]
	f.mu.RUnlock()
	if sh == nil {
		return 0, 0
	}
	sh.mu.Lock()
	inFlight = len(sh.entries)
	ackedAhead = len(sh.ackedAhead)
	sh.mu.Unlock()
	return
}

// RefreshCaps updates limits on all live shards for a topic. Called
// when an operator alters a topic's caps via the alter endpoint.
func (f *InFlight) RefreshCaps(ctx context.Context, topic string) error {
	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return err
	}
	if caps.MaxInFlight <= 0 || caps.MaxAckedAhead <= 0 {
		return errors.New("consumer: caps must be positive")
	}
	f.mu.RLock()
	for k, sh := range f.shards {
		if k.topic != topic {
			continue
		}
		sh.mu.Lock()
		sh.maxInFlight = caps.MaxInFlight
		sh.maxAckedAhead = caps.MaxAckedAhead
		sh.mu.Unlock()
	}
	f.mu.RUnlock()
	return nil
}

// DropTopic removes all shards for a topic. Called on topic deletion.
func (f *InFlight) DropTopic(topic string) {
	f.mu.Lock()
	for k := range f.shards {
		if k.topic == topic {
			delete(f.shards, k)
		}
	}
	f.mu.Unlock()
}

// getOrCreate returns the shard for (topic, partition), lazily creating
// it with committed = -1 if it does not exist.
func (f *InFlight) getOrCreate(ctx context.Context, topic string, partition int) (*partitionShard, error) {
	key := shardKey{topic, partition}
	f.mu.RLock()
	sh := f.shards[key]
	f.mu.RUnlock()
	if sh != nil {
		return sh, nil
	}

	caps, err := f.resolve(ctx, topic)
	if err != nil {
		return nil, err
	}
	if caps.MaxInFlight <= 0 || caps.MaxAckedAhead <= 0 {
		return nil, errors.New("consumer: caps must be positive")
	}

	fresh := &partitionShard{
		committed:     -1,
		entries:       make(map[int64]reservation),
		expiry:        make(expiryHeap, 0, caps.MaxInFlight),
		ackedAhead:    make(map[int64]struct{}),
		maxInFlight:   caps.MaxInFlight,
		maxAckedAhead: caps.MaxAckedAhead,
	}

	f.mu.Lock()
	if existing := f.shards[key]; existing != nil {
		f.mu.Unlock()
		return existing, nil // lost the race, use the winner
	}
	f.shards[key] = fresh
	f.mu.Unlock()
	return fresh, nil
}

// RunPurger periodically sweeps all shards and evicts expired reservations.
// Run it in a goroutine alongside the broker; it stops when ctx is cancelled.
// A 1-second interval is a reasonable default for most deployments.
func (f *InFlight) RunPurger(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f.purgeAll()
		}
	}
}

// purgeAll sweeps every shard and evicts expired reservations.
func (f *InFlight) purgeAll() {
	now := f.timeNow()
	// Snapshot shard list under RLock so we don't hold f.mu while
	// locking individual shards (avoids lock-order issues).
	f.mu.RLock()
	shards := make([]*partitionShard, 0, len(f.shards))
	for _, sh := range f.shards {
		shards = append(shards, sh)
	}
	f.mu.RUnlock()

	for _, sh := range shards {
		sh.mu.Lock()
		sh.purgeExpired(now)
		sh.mu.Unlock()
	}
}

func nowUnixMs() int64 {
	return time.Now().UnixMilli()
}
