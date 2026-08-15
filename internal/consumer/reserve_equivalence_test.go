package consumer

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// ReserveNext used to find the next offset by scanning upward from
// committed+1 and skipping anything reserved, acked ahead, or corrupt.
// It now derives the same answer from scanCursor plus freeSet. These
// tests pin that the two agree, and that the bookkeeping the cursor
// depends on never drifts.
//
// bruteForceNextDeliverable is the OLD predicate, verbatim: the lowest
// offset above the frontier that resolvedOrReservedLocked would not
// reject. It terminates because nothing at or above scanCursor is in any
// of the three sets.
func bruteForceNextDeliverable(sh *partitionShard) int64 {
	for off := sh.committed + 1; ; off++ {
		if _, ok := sh.entries[off]; ok {
			continue
		}
		if _, ok := sh.ackedAhead[off]; ok {
			continue
		}
		if _, ok := sh.corrupt[off]; ok {
			continue
		}
		return off
	}
}

// checkShardInvariants asserts the structural facts the cursor lookup
// relies on. The load-bearing one is the partition property: every
// offset the shard has ever handed out and not yet passed the frontier
// on belongs to exactly one of entries / ackedAhead / corrupt / freeSet.
// If that holds, min(freeSet)-or-scanCursor is provably the lowest
// deliverable offset.
func checkShardInvariants(t *testing.T, sh *partitionShard, step string) {
	t.Helper()

	if sh.scanCursor < sh.committed+1 {
		t.Fatalf("%s: scanCursor %d fell below committed+1 %d", step, sh.scanCursor, sh.committed+1)
	}

	for off := range sh.freeSet {
		if off <= sh.committed {
			t.Fatalf("%s: freeSet holds offset %d at or below frontier %d", step, off, sh.committed)
		}
		if off >= sh.scanCursor {
			t.Fatalf("%s: freeSet holds offset %d at or above scanCursor %d", step, off, sh.scanCursor)
		}
		if _, dup := sh.entries[off]; dup {
			t.Fatalf("%s: offset %d is both free and reserved", step, off)
		}
		if _, dup := sh.ackedAhead[off]; dup {
			t.Fatalf("%s: offset %d is both free and acked-ahead", step, off)
		}
		if _, dup := sh.corrupt[off]; dup {
			t.Fatalf("%s: offset %d is both free and corrupt-skipped", step, off)
		}
	}

	// Every handed-out offset still above the frontier must be accounted
	// for. A gap here means an offset was dropped from entries without
	// being either resolved or released — it would never be redelivered.
	for off := sh.committed + 1; off < sh.scanCursor; off++ {
		n := 0
		if _, ok := sh.entries[off]; ok {
			n++
		}
		if _, ok := sh.ackedAhead[off]; ok {
			n++
		}
		if _, ok := sh.corrupt[off]; ok {
			n++
		}
		if _, ok := sh.freeSet[off]; ok {
			n++
		}
		if n != 1 {
			t.Fatalf("%s: offset %d in %d sets, want exactly 1 (committed=%d scanCursor=%d)",
				step, off, n, sh.committed, sh.scanCursor)
		}
	}

	// freeSet must not be a slow leak. Offsets only ever enter it from
	// entries, and a reserve drains it before extending scanCursor, so
	// in-flight plus free can never exceed MaxInFlight — which also bounds
	// the whole handed-out window by the two caps together.
	if n := len(sh.entries) + len(sh.freeSet); n > sh.maxInFlight {
		t.Fatalf("%s: in-flight(%d) + free(%d) = %d exceeds MaxInFlight %d",
			step, len(sh.entries), len(sh.freeSet), n, sh.maxInFlight)
	}
	if window, limit := sh.scanCursor-sh.committed-1, int64(sh.maxInFlight+sh.maxAckedAhead); window > limit {
		t.Fatalf("%s: handed-out window %d exceeds MaxInFlight+MaxAckedAhead %d", step, window, limit)
	}

	// freeHeap may carry stale slots but must never be MISSING a live one,
	// or nextDeliverableLocked would skip a deliverable offset.
	inHeap := make(map[int64]int, len(sh.freeHeap))
	for _, off := range sh.freeHeap {
		inHeap[off]++
	}
	for off := range sh.freeSet {
		if inHeap[off] == 0 {
			t.Fatalf("%s: freeSet offset %d missing from freeHeap", step, off)
		}
	}
}

// TestReserveNextMatchesLinearScan drives a shard through randomised
// reserve/ack/release/skip/extend/expire sequences and asserts after
// every step that the cursor lookup returns exactly what the old linear
// scan would have.
func TestReserveNextMatchesLinearScan(t *testing.T) {
	configs := []struct {
		name                       string
		maxInFlight, maxAckedAhead int
	}{
		// Small caps exercise the cap and ahead-full branches constantly.
		{"tight", 4, 4},
		{"medium", 16, 8},
		{"wide", 64, 64},
	}

	for _, cfg := range configs {
		for seed := int64(1); seed <= 20; seed++ {
			t.Run(fmt.Sprintf("%s/seed=%d", cfg.name, seed), func(t *testing.T) {
				rng := rand.New(rand.NewSource(seed))
				f := newClockedInFlight(cfg.maxInFlight, cfg.maxAckedAhead)
				ctx := context.Background()
				now := int64(1000)
				logTail := int64(64)

				// live tracks reservations the test believes are held, so ops
				// target real handles. Entries may go stale via expiry; the
				// operations tolerate that (they just fail with ErrHandleStale).
				live := map[int64]int64{}

				sh := f.shard(testTopic, testPart)
				for step := range 400 {
					switch rng.Intn(10) {
					case 0, 1, 2, 3: // reserve
						r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, logTail)
						if err != nil {
							t.Fatalf("step %d: ReserveNext: %v", step, err)
						}
						if r.Reserved {
							live[r.Offset] = r.Nonce
						}
					case 4, 5: // ack
						if off, nonce, ok := pickLive(rng, live); ok {
							_ = f.CommitHandle(testTopic, testPart, off, nonce)
							delete(live, off)
						}
					case 6: // release (nack)
						if off, nonce, ok := pickLive(rng, live); ok {
							_ = f.ReleaseHandle(testTopic, testPart, off, nonce)
							delete(live, off)
						}
					case 7: // corrupt skip
						if off, nonce, ok := pickLive(rng, live); ok {
							_ = f.SkipCorrupt(testTopic, testPart, off, nonce)
							delete(live, off)
						}
					case 8: // extend
						if off, nonce, ok := pickLive(rng, live); ok {
							_, _ = f.ExtendHandle(testTopic, testPart, off, nonce, testVT)
						}
					case 9: // let visibility windows lapse
						now += testVT.Milliseconds() + 1
						withClock(f, now)
						clear(live)
					}

					if sh == nil {
						sh = f.shard(testTopic, testPart)
						if sh == nil {
							continue // nothing reserved yet, shard not created
						}
					}

					// Grow the log occasionally so the cursor keeps advancing
					// instead of parking at a fixed tail.
					if rng.Intn(8) == 0 {
						logTail += int64(rng.Intn(8) + 1)
					}

					stepName := fmt.Sprintf("step %d", step)
					sh.mu.Lock()
					// The real ReserveNext purges before looking; mirror that so
					// the comparison sees the same state it would.
					sh.purgeExpiredLocked(f.now())
					want := bruteForceNextDeliverable(sh)
					got := sh.nextDeliverableLocked()
					checkShardInvariants(t, sh, stepName)
					sh.mu.Unlock()

					if got != want {
						t.Fatalf("%s: nextDeliverableLocked() = %d, linear scan = %d", stepName, got, want)
					}
				}
			})
		}
	}
}

func pickLive(rng *rand.Rand, live map[int64]int64) (int64, int64, bool) {
	if len(live) == 0 {
		return 0, 0, false
	}
	n := rng.Intn(len(live))
	for off, nonce := range live {
		if n == 0 {
			return off, nonce, true
		}
		n--
	}
	return 0, 0, false
}

// TestCompactFreeReclaimsStaleHeapSlots exercises the free-heap rebuild.
// Re-reserving a released offset removes it from freeSet but deliberately
// leaves its heap slot behind, so a partition that churns the same
// offsets accumulates stale slots. compactFreeLocked rebuilds from
// freeSet once they dominate — reusing the existing backing array, which
// is only safe because freeSet is never larger than the heap.
//
// The randomised equivalence test never drives the heap far enough to
// trigger this, so it needs its own case.
func TestCompactFreeReclaimsStaleHeapSlots(t *testing.T) {
	f := newClockedInFlight(64, 64)
	ctx := context.Background()
	tail := int64(64)
	sh, err := f.shardOrCreate(ctx, testTopic, testPart)
	if err != nil {
		t.Fatalf("shardOrCreate: %v", err)
	}

	// Reserve, release, re-reserve the same low offset repeatedly. Each
	// release pushes a heap slot; each re-reserve orphans it.
	churn := 3 * heapCompactSlack
	var peak int
	for range churn {
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
		if err != nil || !r.Reserved {
			t.Fatalf("ReserveNext: %+v %v", r, err)
		}
		if err := f.ReleaseHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
			t.Fatalf("ReleaseHandle: %v", err)
		}
		sh.mu.Lock()
		peak = max(peak, len(sh.freeHeap))
		sh.mu.Unlock()
	}

	sh.mu.Lock()
	defer sh.mu.Unlock()

	if peak <= heapCompactSlack {
		t.Fatalf("heap never grew past the compaction slack (peak %d); test is not exercising compaction", peak)
	}
	if len(sh.freeHeap) > 2*len(sh.freeSet)+heapCompactSlack {
		t.Fatalf("freeHeap (%d) still above the compaction threshold for freeSet (%d)",
			len(sh.freeHeap), len(sh.freeSet))
	}
	// The rebuild must preserve every live offset and the heap ordering.
	for off := range sh.freeSet {
		if !slices.Contains(sh.freeHeap, off) {
			t.Fatalf("compaction dropped live free offset %d", off)
		}
	}
	if got, want := sh.nextDeliverableLocked(), bruteForceNextDeliverable(sh); got != want {
		t.Fatalf("after compaction nextDeliverableLocked() = %d, linear scan = %d", got, want)
	}
	checkShardInvariants(t, sh, "after compaction")
}

// TestReleaseOffsetIgnoresOutOfRangeOffsets covers releaseOffsetLocked's
// guards directly: an offset at or below the frontier, and one at or
// above the cursor, must not enter freeSet. Neither is reachable through
// the public API — the frontier only advances over resolved offsets, and
// nothing above the cursor has been handed out — so the guards exist to
// keep the invariant local, and are asserted here rather than left
// uncovered.
func TestReleaseOffsetIgnoresOutOfRangeOffsets(t *testing.T) {
	f := newClockedInFlight(16, 16)
	ctx := context.Background()

	for range 3 {
		if r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, 16); err != nil || !r.Reserved {
			t.Fatalf("ReserveNext: %+v %v", r, err)
		}
	}
	sh := f.shard(testTopic, testPart)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	sh.releaseOffsetLocked(sh.committed) // at the frontier
	sh.releaseOffsetLocked(sh.committed - 5)
	sh.releaseOffsetLocked(sh.scanCursor) // never handed out
	sh.releaseOffsetLocked(sh.scanCursor + 5)

	if len(sh.freeSet) != 0 {
		t.Fatalf("freeSet = %v, want empty after out-of-range releases", sh.freeSet)
	}
	checkShardInvariants(t, sh, "out-of-range releases")
}

// TestReserveNextRedeliversExpiredInOffsetOrder pins the ordering
// guarantee the free heap exists to preserve: released offsets are handed
// back out lowest-first, not in release order or map order.
func TestReserveNextRedeliversExpiredInOffsetOrder(t *testing.T) {
	f := newClockedInFlight(16, 16)
	ctx := context.Background()
	tail := int64(16)

	var offsets []int64
	for range 5 {
		r := mustReserve(t, f, tail)
		offsets = append(offsets, r.Offset)
	}

	// Release out of order; redelivery must still be ascending.
	for _, i := range []int{3, 0, 4, 1, 2} {
		if err := f.ReleaseHandle(testTopic, testPart, offsets[i], 0); err == nil {
			t.Fatalf("ReleaseHandle with wrong nonce unexpectedly succeeded")
		}
	}

	sh := f.shard(testTopic, testPart)
	sh.mu.Lock()
	for _, i := range []int{3, 0, 4, 1, 2} {
		delete(sh.entries, offsets[i])
		sh.releaseOffsetLocked(offsets[i])
	}
	sh.mu.Unlock()

	for want := range int64(5) {
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
		if err != nil || !r.Reserved {
			t.Fatalf("ReserveNext: %+v %v", r, err)
		}
		if r.Offset != want {
			t.Fatalf("redelivery %d = offset %d, want %d", want, r.Offset, want)
		}
	}
}
