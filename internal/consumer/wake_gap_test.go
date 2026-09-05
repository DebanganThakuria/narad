package consumer

// Deterministic reproductions of the liveness gap found by
// TestAckedAheadPropertyWakeOnUnblock: the inline purgeExpiredLocked at
// the top of resolveReserved (and ExtendHandle) evicts an expired
// reservation, which frees a cap slot or the frontier hole, but that
// release is neither counted (the return value is dropped) nor
// announced. The background purger, when it next runs, finds nothing
// expired and stays silent too. A long-poller parked on "cap" or
// "ahead_full" therefore sleeps out its whole wait (up to
// max_consume_wait) unless a produce happens to broadcast on the log.
//
// This is pre-existing (master purges in the same place), but the PR
// makes "ahead_full" the designed steady state while the hole is held,
// so the redelivery of the hole after expiry now leans on this wake.
// These tests are EXPECTED TO FAIL on the branch as of 09792a8.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func reserveOrFatal(t *testing.T, f *InFlight) ReserveResult {
	t.Helper()
	return mustReserve(t, f, testDeepTail)
}

// The frontier hole expires while the ahead set is full; an ack for
// another live reservation purges it inline and parks that offset in the
// set; nothing wakes the pollers parked on "ahead_full" even though the
// hole is now reservable.
func TestLazyPurgeInAckSwallowsHoleExpiryWake(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(4, 1) // MaxInFlight 4, MaxAckedAhead 1
	var notifies atomic.Int64
	f.SetReleaseNotifier(func(string, int) { notifies.Add(1) })

	hole := reserveOrFatal(t, f)          // 0
	r1 := reserveOrFatal(t, f)            // 1
	r2 := reserveOrFatal(t, f)            // 2
	mustCommit(t, f, r1.Offset, r1.Nonce) // ahead set = {1}: full

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantSkip(t, r, err, "ahead_full") // pollers park here

	// Keep r2 alive across the clock jump; only the hole's lease lapses,
	// and the purger has not ticked yet.
	if _, err := f.ExtendHandle(testTopic, testPart, r2.Offset, r2.Nonce, 10*testVT); err != nil {
		t.Fatalf("extend: %v", err)
	}
	withClock(f, 1000+testVT.Milliseconds()+1)
	_ = hole

	n0 := notifies.Load()
	mustCommit(t, f, r2.Offset, r2.Nonce) // inline purge evicts 0, then parks 2

	// Offset 0 is reservable again ...
	r, err = f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
	// ... but nobody was told: the parked long-pollers sleep out their wait.
	if notifies.Load() == n0 {
		t.Fatalf("ack purged the expired frontier hole (offset 0 became reservable) without firing the release notifier; pollers parked on ahead_full are never woken")
	}
}

// Same gap for the MaxInFlight cap: pollers park on "cap", one lease
// lapses, an ack for another reservation purges it inline and computes
// wasAtCap AFTER the purge, so the slot it freed goes unannounced.
func TestLazyPurgeInAckSwallowsCapReleaseWake(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(2, 8) // MaxInFlight 2
	var notifies atomic.Int64
	f.SetReleaseNotifier(func(string, int) { notifies.Add(1) })

	r0 := reserveOrFatal(t, f)
	r1 := reserveOrFatal(t, f)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantSkip(t, r, err, "cap") // pollers park here

	// r0 lapses; extend r1 so it stays live past the clock jump.
	if _, err := f.ExtendHandle(testTopic, testPart, r1.Offset, r1.Nonce, 10*testVT); err != nil {
		t.Fatalf("extend: %v", err)
	}
	withClock(f, 1000+testVT.Milliseconds()+1)

	n0 := notifies.Load()
	// Ack of r1: inline purge evicts r0 (entries 2 -> 1), so wasAtCap is
	// false; r1 leaves at the frontier, nothing notifies.
	mustCommit(t, f, r1.Offset, r1.Nonce)
	_ = r0

	// purgeAll finds nothing expired: silent as well.
	f.purgeAll()

	r, err = f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
	if notifies.Load() == n0 {
		t.Fatalf("ack purged an expired reservation (a cap slot became free) without firing the release notifier; pollers parked on cap are never woken")
	}
}

// Sanity check of the contract the two tests above rely on: when the
// PURGER evicts the expired hole first, the wake does fire.
func TestPurgerEvictingHoleWakesAheadFullPollers(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(4, 1)
	var notifies atomic.Int64
	f.SetReleaseNotifier(func(string, int) { notifies.Add(1) })

	reserveOrFatal(t, f) // hole 0
	r1 := reserveOrFatal(t, f)
	mustCommit(t, f, r1.Offset, r1.Nonce)
	withClock(f, 1000+int64(2*time.Minute/time.Millisecond))
	n0 := notifies.Load()
	f.purgeAll()
	if notifies.Load() == n0 {
		t.Fatal("purger evicted the expired hole without notifying")
	}
}
