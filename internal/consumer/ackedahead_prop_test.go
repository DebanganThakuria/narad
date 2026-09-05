package consumer

// Randomized property test for the acked-ahead contract introduced by
// "acked-ahead cap gates delivery, never acks of live reservations":
//
//   (a) len(ackedAhead)+len(corrupt) <= MaxAckedAhead + MaxInFlight
//       (with a documented relaxation right after a cap is LOWERED,
//       see propState.bound);
//   (b) every offset above committed is in at most one of
//       entries / ackedAhead / corrupt, nothing at or below committed
//       lingers in any of them, and the nextFree hint never skips a
//       free offset;
//   (c) when the ahead set is full, ReserveNext hands out committed+1
//       or nothing;
//   (d) CommitHandle / SkipCorrupt for a live reservation never fail,
//       and in particular never return ErrAckedAheadFull;
//   (e) acking the frontier hole collapses the whole contiguous
//       resolved run in one step;
//   plus a brute-force oracle for ReserveNext (offset or skip reason)
//   and a model of every handle the test holds (a handle that is still
//   inside its visibility window must be exactly the live reservation;
//   the same offset is never handed out twice while a lease is live).
//
// The driver mixes reserve / ack / nack / extend / SkipCorrupt /
// SkipMissingBelow / clock advance (with and without a purger tick) /
// produce (tail growth) / RefreshCaps (raise and lower) / DropPartition
// with small caps so the ahead set fills constantly.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

const propVT = 10_000 // fake-clock visibility timeout, ms

type propHandle struct {
	offset, nonce, exp int64
}

type propState struct {
	t    *testing.T
	f    *InFlight
	rng  *rand.Rand
	now  int64
	caps Caps
	tail int64
	live map[int64]propHandle

	// refresh enables RefreshCaps steps; strict then also disables the
	// post-lowering relaxation of bound (a).
	refresh bool
	// hw is the high-water relaxation of bound (a) after a cap was
	// lowered below the state already in the shard: the set can hold at
	// most what was already reserved-or-resolved at that moment. It is
	// cleared as soon as the shard fits the new caps again.
	hw int

	// wake enables the liveness check: an operation that turns a
	// "cap"/"ahead_full" probe into a reservable one must have fired the
	// release notifier, or a parked long-poller sleeps out its wait.
	wake     bool
	notifies atomic.Int64

	stats map[string]int
}

func newPropState(t *testing.T, seed uint64, refresh, wake bool) *propState {
	st := &propState{
		t:       t,
		rng:     rand.New(rand.NewPCG(seed, 0xC0FFEE)),
		now:     1000,
		tail:    16,
		live:    map[int64]propHandle{},
		refresh: refresh,
		wake:    wake,
		stats:   map[string]int{},
	}
	st.caps = Caps{MaxInFlight: 3 + st.rng.IntN(6), MaxAckedAhead: 1 + st.rng.IntN(4)}
	st.f = NewInFlight(func(context.Context, string) (Caps, error) { return st.caps, nil }, nil)
	withClock(st.f, st.now)
	st.f.SetReleaseNotifier(func(string, int) { st.notifies.Add(1) })
	return st
}

func (st *propState) fatalf(format string, args ...any) {
	st.t.Helper()
	st.t.Fatalf("[step %d caps=%+v now=%d tail=%d] %s", st.stats["steps"], st.caps, st.now, st.tail, fmt.Sprintf(format, args...))
}

func (st *propState) shard() *partitionShard { return st.f.shard(testTopic, testPart) }

func setHas(sh *partitionShard, off int64) bool {
	_, a := sh.ackedAhead[off]
	_, c := sh.corrupt[off]
	return a || c
}

// oracleLocked is a brute-force ReserveNext: the offset it must hand
// out (or -1 and the skip reason). Must hold sh.mu, after purging.
func oracleLocked(sh *partitionShard, tail int64) (int64, string) {
	if len(sh.entries) >= sh.maxInFlight {
		return -1, "cap"
	}
	next := sh.committed + 1
	if next >= tail {
		return -1, "empty"
	}
	if sh.aheadFullLocked() {
		if sh.resolvedOrReservedLocked(next) {
			return -1, "ahead_full"
		}
		return next, ""
	}
	for off := next; off < tail; off++ {
		if !sh.resolvedOrReservedLocked(off) {
			return off, ""
		}
	}
	return -1, "all_reserved"
}

// probe is the raw (no purge, no side effects) reservability of the
// shard as a parked poller would see it; "" means reservable.
func (st *propState) probe() string {
	sh := st.shard()
	if sh == nil {
		return ""
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, reason := oracleLocked(sh, st.tail)
	return reason
}

// isLive reports whether the test believes h is still the live
// reservation: inside its visibility window under the fake clock. The
// broker purges with expiresAt <= now, so exp == now is expired.
func (st *propState) isLive(h propHandle) bool { return h.exp > st.now }

func (st *propState) pickHandle() (propHandle, bool) {
	if len(st.live) == 0 {
		return propHandle{}, false
	}
	keys := make([]int64, 0, len(st.live))
	for k := range st.live {
		keys = append(keys, k)
	}
	slices.Sort(keys) // map order is random; keep every seed reproducible
	return st.live[keys[st.rng.IntN(len(keys))]], true
}

// expectedCollapse returns where committed must land if off (== committed+1)
// is resolved now: the end of the contiguous resolved run above it.
func expectedCollapseLocked(sh *partitionShard, off int64) int64 {
	k := off
	for setHas(sh, k+1) {
		k++
	}
	return k
}

func (st *propState) opReserve() {
	sh := st.shard()
	wantOff, wantSkip := int64(0), ""
	aheadFull, committed := false, int64(-1)
	if sh != nil {
		sh.mu.Lock()
		sh.purgeExpiredLocked(st.now) // ReserveNext purges first; mirror it so the oracle sees the same state
		wantOff, wantSkip = oracleLocked(sh, st.tail)
		aheadFull, committed = sh.aheadFullLocked(), sh.committed
		sh.mu.Unlock()
	}
	r, err := st.f.ReserveNext(context.Background(), testTopic, testPart, propVT*time.Millisecond, st.tail)
	if err != nil {
		st.fatalf("ReserveNext: %v", err)
	}
	if r.Reserved != (wantOff >= 0) || (r.Reserved && r.Offset != wantOff) || (!r.Reserved && r.SkipReason != wantSkip) {
		st.fatalf("ReserveNext = %+v, oracle wants offset=%d skip=%q", r, wantOff, wantSkip)
	}
	if !r.Reserved {
		st.stats["skip_"+r.SkipReason]++
		return
	}
	if aheadFull && r.Offset != committed+1 {
		st.fatalf("(c) ahead set full but ReserveNext handed out %d, frontier hole is %d", r.Offset, committed+1)
	}
	if aheadFull {
		st.stats["reserve_hole_while_full"]++
	}
	if old, ok := st.live[r.Offset]; ok && st.isLive(old) {
		st.fatalf("offset %d handed out twice while lease live (old nonce %d exp %d, new nonce %d)", r.Offset, old.nonce, old.exp, r.Nonce)
	}
	if r.ExpiresAtUnixMs != st.now+propVT {
		st.fatalf("ReserveNext expiry = %d, want %d", r.ExpiresAtUnixMs, st.now+propVT)
	}
	st.live[r.Offset] = propHandle{offset: r.Offset, nonce: r.Nonce, exp: r.ExpiresAtUnixMs}
	st.stats["reserve"]++
}

// opResolve is ack (corrupt=false) or SkipCorrupt (corrupt=true).
func (st *propState) opResolve(corrupt bool) {
	h, ok := st.pickHandle()
	if !ok {
		return
	}
	sh := st.shard()
	sh.mu.Lock()
	live := st.isLive(h)
	if live {
		if rsv, ok := sh.entries[h.offset]; !ok || rsv.nonce != h.nonce {
			sh.mu.Unlock()
			st.fatalf("model says %+v is live but shard entry = %+v (present=%v)", h, rsv, ok)
		}
	}
	atFrontier := h.offset == sh.committed+1
	wasFull := sh.aheadFullLocked()
	wantCommitted := sh.committed
	if live && atFrontier {
		wantCommitted = expectedCollapseLocked(sh, h.offset)
	}
	sh.mu.Unlock()

	before := st.probe()
	n0 := st.notifies.Load()
	var err error
	if corrupt {
		err = st.f.SkipCorrupt(testTopic, testPart, h.offset, h.nonce)
	} else {
		err = st.f.CommitHandle(testTopic, testPart, h.offset, h.nonce)
	}
	delete(st.live, h.offset)
	if !live {
		if !errors.Is(err, ErrHandleStale) {
			st.fatalf("resolve of expired handle %+v: err = %v, want ErrHandleStale", h, err)
		}
		st.stats["resolve_stale"]++
		return
	}
	if errors.Is(err, ErrAckedAheadFull) {
		st.fatalf("(d) live reservation %+v rejected with ErrAckedAheadFull (ahead full before: %v)", h, wasFull)
	}
	if err != nil {
		st.fatalf("(d) live reservation %+v rejected: %v", h, err)
	}
	if got := committedOffset(st.f, testTopic, testPart); got != wantCommitted {
		st.fatalf("(e) after resolving %d (frontier=%v) committed = %d, want %d", h.offset, atFrontier, got, wantCommitted)
	}
	if wasFull && !atFrontier {
		st.stats["resolve_ahead_while_full"]++
	}
	if atFrontier {
		st.stats["resolve_frontier"]++
	} else {
		st.stats["resolve_ahead"]++
	}
	st.checkWake("resolve", before, n0)
}

func (st *propState) opNack() {
	h, ok := st.pickHandle()
	if !ok {
		return
	}
	live := st.isLive(h)
	before := st.probe()
	n0 := st.notifies.Load()
	err := st.f.ReleaseHandle(testTopic, testPart, h.offset, h.nonce)
	delete(st.live, h.offset)
	if live && err != nil {
		st.fatalf("nack of live handle %+v: %v", h, err)
	}
	if !live && !errors.Is(err, ErrHandleStale) {
		st.fatalf("nack of expired handle %+v: err = %v, want ErrHandleStale", h, err)
	}
	st.stats["nack"]++
	if live {
		st.checkWake("nack", before, n0)
	}
}

func (st *propState) opExtend() {
	h, ok := st.pickHandle()
	if !ok {
		return
	}
	live := st.isLive(h)
	before := st.probe()
	n0 := st.notifies.Load()
	exp, err := st.f.ExtendHandle(testTopic, testPart, h.offset, h.nonce, propVT*time.Millisecond)
	if !live {
		if !errors.Is(err, ErrHandleStale) {
			st.fatalf("extend of expired handle %+v: err = %v, want ErrHandleStale", h, err)
		}
		delete(st.live, h.offset)
		return
	}
	if err != nil || exp != st.now+propVT {
		st.fatalf("extend of live handle %+v: exp=%d err=%v, want %d", h, exp, err, st.now+propVT)
	}
	h.exp = exp
	st.live[h.offset] = h
	st.stats["extend"]++
	st.checkWake("extend", before, n0)
}

func (st *propState) opSkipMissingBelow() {
	h, ok := st.pickHandle()
	if !ok {
		return
	}
	live := st.isLive(h)
	oldest := h.offset + 1 + int64(st.rng.IntN(4))
	sh := st.shard()
	sh.mu.Lock()
	start := max(sh.committed, oldest-1)
	k := start
	for setHas(sh, k+1) && k+1 >= oldest {
		k++
	}
	wantCommitted, wasCommitted := k, sh.committed
	sh.mu.Unlock()

	before := st.probe()
	n0 := st.notifies.Load()
	moved, err := st.f.SkipMissingBelow(testTopic, testPart, h.offset, h.nonce, oldest)
	if !live {
		if !errors.Is(err, ErrHandleStale) {
			st.fatalf("SkipMissingBelow with expired handle %+v: err = %v, want ErrHandleStale", h, err)
		}
		delete(st.live, h.offset)
		return
	}
	if err != nil {
		st.fatalf("SkipMissingBelow(%+v, oldest=%d): %v", h, oldest, err)
	}
	if got := committedOffset(st.f, testTopic, testPart); got != wantCommitted || moved != wantCommitted-wasCommitted {
		st.fatalf("SkipMissingBelow(oldest=%d): committed=%d moved=%d, want committed=%d moved=%d", oldest, got, moved, wantCommitted, wantCommitted-wasCommitted)
	}
	for off := range st.live {
		if off < oldest {
			delete(st.live, off)
		}
	}
	st.stats["skip_missing"]++
	st.checkWake("skip_missing", before, n0)
}

func (st *propState) opAdvanceClock() {
	st.now += int64(st.rng.IntN(propVT*3/2 + 1))
	withClock(st.f, st.now)
	st.stats["clock"]++
	if st.rng.IntN(2) == 0 {
		before := st.probe()
		n0 := st.notifies.Load()
		st.f.purgeAll()
		st.stats["purge"]++
		st.checkWake("purge", before, n0)
	}
}

func (st *propState) opProduce() {
	st.tail += 1 + int64(st.rng.IntN(8))
	st.stats["produce"]++
}

func (st *propState) opRefreshCaps() {
	old := st.caps
	st.caps = Caps{MaxInFlight: 3 + st.rng.IntN(6), MaxAckedAhead: 1 + st.rng.IntN(4)}
	if err := st.f.RefreshCaps(context.Background(), testTopic); err != nil {
		st.fatalf("RefreshCaps: %v", err)
	}
	if sh := st.shard(); sh != nil {
		sh.mu.Lock()
		p := len(sh.ackedAhead) + len(sh.corrupt) + len(sh.entries)
		sh.mu.Unlock()
		lowered := st.caps.MaxInFlight < old.MaxInFlight || st.caps.MaxAckedAhead < old.MaxAckedAhead
		if lowered && p > st.caps.MaxInFlight+st.caps.MaxAckedAhead {
			st.hw = max(st.hw, p)
			st.stats["refresh_lowered_above"]++
		}
	}
	st.stats["refresh"]++
}

func (st *propState) opDropPartition() {
	st.f.DropPartition(testTopic, testPart)
	for _, h := range st.live {
		if err := st.f.CommitHandle(testTopic, testPart, h.offset, h.nonce); !errors.Is(err, ErrHandleStale) {
			st.fatalf("ack after DropPartition: err = %v, want ErrHandleStale", err)
		}
	}
	clear(st.live)
	st.hw = 0
	st.stats["drop"]++
}

func (st *propState) checkWake(op, before string, n0 int64) {
	if !st.wake {
		return
	}
	if before != "cap" && before != "ahead_full" {
		return
	}
	if after := st.probe(); after == "" && st.notifies.Load() == n0 {
		st.fatalf("(f) %s turned a %q partition reservable without firing the release notifier: a poller parked on %q is never woken", op, before, before)
	}
}

// bound is invariant (a)'s right-hand side.
func (st *propState) bound() int {
	return max(st.caps.MaxInFlight+st.caps.MaxAckedAhead, st.hw)
}

func (st *propState) checkInvariants() {
	sh := st.shard()
	if sh == nil {
		return
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if sh.maxInFlight != st.caps.MaxInFlight || sh.maxAckedAhead != st.caps.MaxAckedAhead {
		st.fatalf("shard caps (%d,%d) != resolver caps %+v", sh.maxInFlight, sh.maxAckedAhead, st.caps)
	}

	// (a)
	set := len(sh.ackedAhead) + len(sh.corrupt)
	if set+len(sh.entries) <= st.caps.MaxInFlight+st.caps.MaxAckedAhead {
		st.hw = 0
	}
	if set > st.bound() {
		st.fatalf("(a) ahead set = %d (acked %d + corrupt %d) exceeds bound %d (MaxAckedAhead %d + MaxInFlight %d, hw %d); entries=%d committed=%d",
			set, len(sh.ackedAhead), len(sh.corrupt), st.bound(), st.caps.MaxAckedAhead, st.caps.MaxInFlight, st.hw, len(sh.entries), sh.committed)
	}
	if set > st.stats["max_set"] {
		st.stats["max_set"] = set
	}

	// (b)
	for off := range sh.entries {
		if off <= sh.committed {
			st.fatalf("(b) reservation at %d is at or below committed %d", off, sh.committed)
		}
		if setHas(sh, off) {
			st.fatalf("(b) offset %d is both reserved and resolved", off)
		}
		if off >= st.tail {
			st.fatalf("(b) reservation at %d is past the log tail %d", off, st.tail)
		}
	}
	for off := range sh.ackedAhead {
		if off <= sh.committed {
			st.fatalf("(b) ackedAhead has %d at or below committed %d", off, sh.committed)
		}
		if _, dup := sh.corrupt[off]; dup {
			st.fatalf("(b) offset %d is both acked-ahead and corrupt", off)
		}
	}
	for off := range sh.corrupt {
		if off <= sh.committed {
			st.fatalf("(b) corrupt has %d at or below committed %d", off, sh.committed)
		}
	}
	if setHas(sh, sh.committed+1) {
		st.fatalf("(b)/(e) frontier hole %d is resolved but committed did not advance over it", sh.committed+1)
	}
	for off := sh.committed + 1; off < sh.nextFree; off++ {
		if !sh.resolvedOrReservedLocked(off) {
			st.fatalf("(b) nextFree hint %d skips free offset %d (committed %d)", sh.nextFree, off, sh.committed)
		}
	}

	// Handle model vs shard: every shard entry is a handle the test
	// holds with the same nonce and deadline; every handle inside its
	// window is a shard entry.
	for off, rsv := range sh.entries {
		h, ok := st.live[off]
		if !ok || h.nonce != rsv.nonce || h.exp != rsv.expiresAtUnixMs {
			st.fatalf("shard entry %d=%+v not held by the test (model %+v present=%v)", off, rsv, h, ok)
		}
	}
	for off, h := range st.live {
		if !st.isLive(h) {
			continue
		}
		if rsv, ok := sh.entries[off]; !ok || rsv.nonce != h.nonce {
			st.fatalf("live handle %+v missing from shard (entry %+v present=%v)", h, rsv, ok)
		}
	}
}

func (st *propState) step() {
	st.stats["steps"]++
	switch r := st.rng.IntN(100); {
	case r < 34:
		st.opReserve()
	case r < 60:
		st.opResolve(false)
	case r < 64:
		st.opResolve(true)
	case r < 70:
		st.opNack()
	case r < 76:
		st.opExtend()
	case r < 86:
		st.opAdvanceClock()
	case r < 94:
		st.opProduce()
	case r < 96:
		st.opSkipMissingBelow()
	case r < 99:
		if st.refresh {
			st.opRefreshCaps()
		} else {
			st.opReserve()
		}
	default:
		if st.rng.IntN(4) == 0 {
			st.opDropPartition()
		} else {
			st.opReserve()
		}
	}
	st.checkInvariants()
}

func runProp(t *testing.T, seeds, steps int, refresh, wake bool) {
	t.Helper()
	// Coverage guards, summed over all seeds: the interesting regime
	// (set full, hole served, live acks landing while full) must occur.
	var aheadFullSkips, holeWhileFull, ackWhileFull atomic.Int64
	t.Cleanup(func() {
		if aheadFullSkips.Load() == 0 || holeWhileFull.Load() == 0 || ackWhileFull.Load() == 0 {
			t.Errorf("the ahead-full regime was never exercised: ahead_full skips=%d hole reserves while full=%d acks while full=%d",
				aheadFullSkips.Load(), holeWhileFull.Load(), ackWhileFull.Load())
		}
	})
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			st := newPropState(t, uint64(seed), refresh, wake)
			for range steps {
				st.step()
			}
			aheadFullSkips.Add(int64(st.stats["skip_ahead_full"]))
			holeWhileFull.Add(int64(st.stats["reserve_hole_while_full"]))
			ackWhileFull.Add(int64(st.stats["resolve_ahead_while_full"]))
			if st.stats["max_set"] < st.caps.MaxAckedAhead && !refresh {
				t.Fatalf("seed %d: ahead set never reached the cap: %v", seed, st.stats)
			}
			t.Logf("stats: %v", st.stats)
		})
	}
}

// TestAckedAheadPropertyFixedCaps drives the shard with fixed caps: the
// strict bound (a) holds with no relaxation.
func TestAckedAheadPropertyFixedCaps(t *testing.T) {
	t.Parallel()
	runProp(t, 12, 4000, false, false)
}

// TestAckedAheadPropertyWithRefreshCaps adds RefreshCaps steps (raising
// and lowering both caps at random). Lowering a cap below state already
// in the shard is the one case where the set can transiently exceed the
// new cap + in-flight: it is then bounded by what was reserved-or-
// resolved at the moment of lowering (propState.hw) until the shard
// fits the new caps again.
func TestAckedAheadPropertyWithRefreshCaps(t *testing.T) {
	t.Parallel()
	runProp(t, 12, 4000, true, false)
}

// TestAckedAheadPropertyWakeOnUnblock adds the liveness check (f): an
// operation that makes a "cap"/"ahead_full" partition reservable must
// fire the release notifier. See TestLazyPurgeInAckSwallowsHoleExpiryWake
// for the deterministic version of the failure this finds.
func TestAckedAheadPropertyWakeOnUnblock(t *testing.T) {
	t.Parallel()
	runProp(t, 12, 4000, false, true)
}
