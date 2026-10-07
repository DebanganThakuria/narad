package sink

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// DefaultHeldBytes is remotes.max_held_bytes' default: 256 MiB.
const DefaultHeldBytes int64 = 256 << 20

// HeldBudget is the per-node budget for records held across a remote
// failure (remotes.max_held_bytes), first come first served. A lane that
// must wait copies its unsent records off the parent log so an outage
// pins no log frames; when the budget cannot take them the cursor keeps
// nothing and re-reads its slab later, skipping what the target already
// accepted. A limit of 0 or less holds nothing.
type HeldBudget struct {
	limit int64
	used  atomic.Int64
	// observe, when set, receives the bytes held after every change.
	// reportMu orders the reports: each reads the budget under it, so
	// the last one always carries what is held now, however the changes
	// and their reports interleave.
	observe  func(int64)
	reportMu sync.Mutex
}

// NewHeldBudget returns a budget of limit bytes; observe may be nil.
func NewHeldBudget(limit int64, observe func(int64)) *HeldBudget {
	return &HeldBudget{limit: limit, observe: observe}
}

// TryReserve takes n bytes, or reports false and takes nothing.
func (b *HeldBudget) TryReserve(n int64) bool {
	if b == nil || b.limit <= 0 {
		return false
	}
	for {
		used := b.used.Load()
		if used+n > b.limit {
			return false
		}
		if b.used.CompareAndSwap(used, used+n) {
			b.report()
			return true
		}
	}
}

// Release gives back n bytes.
func (b *HeldBudget) Release(n int64) {
	if b == nil || n == 0 {
		return
	}
	b.used.Add(-n)
	b.report()
}

// Used is the bytes held now.
func (b *HeldBudget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used.Load()
}

func (b *HeldBudget) report() {
	if b.observe == nil {
		return
	}
	b.reportMu.Lock()
	defer b.reportMu.Unlock()
	b.observe(b.used.Load())
}

// HeldSize is what holding recs costs.
func HeldSize(recs []topic.KeyedRecord) int64 {
	var n int64
	for i := range recs {
		n += int64(len(recs[i].Payload) + len(recs[i].Key))
	}
	return n
}

// Hold copies recs' payloads into one fresh buffer, as the local
// fan-out's retry does (fanout_cursor.go pendingRecords): the slab's
// payloads alias the parent log's decoded frames.
func Hold(recs []topic.KeyedRecord) []topic.KeyedRecord {
	size := 0
	for i := range recs {
		size += len(recs[i].Payload)
	}
	out := make([]topic.KeyedRecord, len(recs))
	arena := make([]byte, 0, size)
	for i, rec := range recs {
		start := len(arena)
		arena = append(arena, rec.Payload...)
		rec.Payload = arena[start:len(arena):len(arena)]
		out[i] = rec
	}
	return out
}

// Semaphore is a remote's max_in_flight slots on this node, shared by
// every lane of every cursor that sends to it. Its size follows the
// remote's limits live.
type Semaphore struct {
	mu    sync.Mutex
	limit int
	used  int
	wake  chan struct{}
}

// NewSemaphore returns a semaphore of n slots.
func NewSemaphore(n int) *Semaphore { return &Semaphore{limit: max(n, 1), wake: make(chan struct{})} }

// Acquire takes a slot and reports how long it waited.
func (s *Semaphore) Acquire(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	for {
		s.mu.Lock()
		if s.used < s.limit {
			s.used++
			s.mu.Unlock()
			return time.Since(start), nil
		}
		wake := s.wake
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-wake:
		}
	}
}

// Release gives a slot back.
func (s *Semaphore) Release() {
	s.mu.Lock()
	s.used--
	s.signalLocked()
	s.mu.Unlock()
}

// Resize sets the slot count; slots in use above a smaller limit drain
// as they are released.
func (s *Semaphore) Resize(n int) {
	s.mu.Lock()
	if n = max(n, 1); n != s.limit {
		s.limit = n
		s.signalLocked()
	}
	s.mu.Unlock()
}

// Limit is the current slot count.
func (s *Semaphore) Limit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.limit
}

func (s *Semaphore) signalLocked() {
	close(s.wake)
	s.wake = make(chan struct{})
}
