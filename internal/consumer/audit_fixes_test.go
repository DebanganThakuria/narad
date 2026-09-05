package consumer

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

func newAuditInFlight(t *testing.T, caps Caps) (*InFlight, *int) {
	t.Helper()
	commits := 0
	f := NewInFlight(func(context.Context, string) (Caps, error) { return caps, nil },
		func(string, int, int64) { commits++ })
	return f, &commits
}

// A plain ack (not at cap, ahead set not full) makes nothing reservable,
// so it must not wake long-pollers; an ack that frees a cap slot must.
func TestCommitHandleWakesOnlyWhenCapSlotFreed(t *testing.T) {
	f, _ := newAuditInFlight(t, Caps{MaxInFlight: 2, MaxAckedAhead: 10})
	var mu sync.Mutex
	wakes := 0
	f.SetReleaseNotifier(func(string, int) {
		mu.Lock()
		wakes++
		mu.Unlock()
	})
	ctx := context.Background()

	r0, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10)
	if err := f.CommitHandle("t", 0, r0.Offset, r0.Nonce); err != nil {
		t.Fatalf("CommitHandle(0): %v", err)
	}
	mu.Lock()
	if wakes != 0 {
		t.Fatalf("wakes after uncontended ack = %d, want 0", wakes)
	}
	mu.Unlock()

	r1, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10)
	r2, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10)
	if res, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10); res.Reserved || res.SkipReason != "cap" {
		t.Fatalf("third reserve = %+v, want cap", res)
	}
	if err := f.CommitHandle("t", 0, r2.Offset, r2.Nonce); err != nil { // out of order, frees a slot
		t.Fatalf("CommitHandle(2): %v", err)
	}
	mu.Lock()
	if wakes != 1 {
		t.Fatalf("wakes after ack at cap = %d, want 1", wakes)
	}
	mu.Unlock()
	_ = r1
}

// With the ahead set full, pollers park on "ahead_full" until the
// frontier hole is acked; that ack must wake them.
func TestCommitHandleWakesWhenFrontierAckEndsAheadFull(t *testing.T) {
	f, _ := newAuditInFlight(t, Caps{MaxInFlight: 10, MaxAckedAhead: 1})
	wakes := 0
	f.SetReleaseNotifier(func(string, int) { wakes++ })
	ctx := context.Background()

	r0, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10)
	r1, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10)
	if err := f.CommitHandle("t", 0, r1.Offset, r1.Nonce); err != nil {
		t.Fatalf("CommitHandle(1): %v", err)
	}
	if res, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10); res.Reserved || res.SkipReason != "ahead_full" {
		t.Fatalf("reserve with ahead full = %+v, want ahead_full", res)
	}
	wakes = 0
	if err := f.CommitHandle("t", 0, r0.Offset, r0.Nonce); err != nil {
		t.Fatalf("CommitHandle(0): %v", err)
	}
	if wakes != 1 {
		t.Fatalf("wakes after frontier ack = %d, want 1", wakes)
	}
	if res, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 10); !res.Reserved || res.Offset != 2 {
		t.Fatalf("reserve after collapse = %+v, want offset 2", res)
	}
}

// The nextFree hint must never hand out a reserved or resolved offset
// and must always return the LOWEST free offset, exactly like the full
// scan it replaces. Exercise it against a brute-force oracle under a
// random mix of reserve, ack (in and out of order), nack, and expiry.
func TestReserveNextHintMatchesBruteForceOracle(t *testing.T) {
	const tail = 200
	f, _ := newAuditInFlight(t, Caps{MaxInFlight: 64, MaxAckedAhead: 64})
	now := int64(1_000_000)
	f.setTimeNow(func() int64 { return now })
	ctx := context.Background()
	rng := rand.New(rand.NewPCG(7, 11))

	type live struct{ offset, nonce int64 }
	var reserved []live

	oracle := func() (int64, bool) {
		sh := f.shard("t", 0)
		if sh == nil {
			return 0, true
		}
		sh.mu.Lock()
		defer sh.mu.Unlock()
		sh.purgeExpiredLocked(now) // ReserveNext purges before scanning; mirror it
		if len(sh.entries) >= sh.maxInFlight {
			return 0, false
		}
		for off := sh.committed + 1; off < tail; off++ {
			if !sh.resolvedOrReservedLocked(off) {
				return off, true
			}
		}
		return 0, false
	}

	for step := range 5000 {
		switch rng.IntN(5) {
		case 0, 1: // reserve and compare with the oracle
			want, ok := oracle()
			res, err := f.ReserveNext(ctx, "t", 0, 50*time.Millisecond, tail)
			if err != nil {
				t.Fatalf("ReserveNext: %v", err)
			}
			if res.Reserved != ok || (ok && res.Offset != want) {
				t.Fatalf("step %d: ReserveNext = %+v, oracle = (%d, %v)", step, res, want, ok)
			}
			if res.Reserved {
				reserved = append(reserved, live{res.Offset, res.Nonce})
			}
		case 2: // ack a random live reservation
			if len(reserved) == 0 {
				continue
			}
			i := rng.IntN(len(reserved))
			r := reserved[i]
			reserved = append(reserved[:i], reserved[i+1:]...)
			if err := f.CommitHandle("t", 0, r.offset, r.nonce); err != nil && !errors.Is(err, ErrAckedAheadFull) && !errors.Is(err, ErrHandleStale) {
				t.Fatalf("CommitHandle: %v", err)
			}
		case 3: // nack a random live reservation
			if len(reserved) == 0 {
				continue
			}
			i := rng.IntN(len(reserved))
			r := reserved[i]
			reserved = append(reserved[:i], reserved[i+1:]...)
			if err := f.ReleaseHandle("t", 0, r.offset, r.nonce); err != nil && !errors.Is(err, ErrHandleStale) {
				t.Fatalf("ReleaseHandle: %v", err)
			}
		case 4: // let everything expire
			now += 60
			reserved = reserved[:0]
		}
	}
}

// Nonces bind an ack to the consumer that received the message; they
// must not be a predictable counter.
func TestReserveNextNoncesAreNotSequential(t *testing.T) {
	f, _ := newAuditInFlight(t, Caps{MaxInFlight: 100, MaxAckedAhead: 100})
	ctx := context.Background()
	var prev int64
	sequential := 0
	for i := range 50 {
		res, err := f.ReserveNext(ctx, "t", 0, time.Minute, 1000)
		if err != nil || !res.Reserved {
			t.Fatalf("ReserveNext(%d) = %+v, %v", i, res, err)
		}
		if res.Nonce <= 0 {
			t.Fatalf("nonce %d not positive", res.Nonce)
		}
		if res.Nonce == prev+1 {
			sequential++
		}
		prev = res.Nonce
	}
	if sequential > 2 {
		t.Fatalf("%d of 50 nonces were sequential; nonces must be unpredictable", sequential)
	}
	if first, _ := f.ReserveNext(ctx, "t", 1, time.Minute, 10); first.Nonce == 1 {
		t.Fatalf("fresh shard handed out nonce 1: the old guessable counter")
	}
}

// A partition whose reserved offset fell below the oldest retained
// offset is skipped to that offset in one step: the frontier moves,
// stale reservations below it are discarded, the advance is persisted,
// and the next reserve starts at the oldest retained offset.
func TestSkipMissingBelowJumpsFrontierToOldestRetained(t *testing.T) {
	f, commits := newAuditInFlight(t, Caps{MaxInFlight: 10, MaxAckedAhead: 10})
	ctx := context.Background()
	wakes := 0
	f.SetReleaseNotifier(func(string, int) { wakes++ })

	r0, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 1000)
	r1, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 1000)
	r2, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 1000)
	if err := f.CommitHandle("t", 0, r2.Offset, r2.Nonce); err != nil { // acked ahead, below oldest
		t.Fatalf("CommitHandle(2): %v", err)
	}

	if _, err := f.SkipMissingBelow("t", 0, r0.Offset, r0.Nonce, 0); !errors.Is(err, ErrInvalidSkip) {
		t.Fatalf("SkipMissingBelow with oldest<=offset error = %v, want ErrInvalidSkip", err)
	}
	if _, err := f.SkipMissingBelow("t", 0, r0.Offset, r0.Nonce+1, 500); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("SkipMissingBelow with wrong nonce error = %v, want ErrHandleStale", err)
	}

	skipped, err := f.SkipMissingBelow("t", 0, r0.Offset, r0.Nonce, 500)
	if err != nil {
		t.Fatalf("SkipMissingBelow: %v", err)
	}
	if skipped != 500 {
		t.Fatalf("skipped = %d, want 500", skipped)
	}
	if got := f.Next("t", 0); got != 500 {
		t.Fatalf("Next() = %d, want 500", got)
	}
	if *commits != 1 || wakes != 1 {
		t.Fatalf("commits=%d wakes=%d, want 1 and 1", *commits, wakes)
	}
	if inFlight, ahead := f.Snapshot("t", 0); inFlight != 0 || ahead != 0 {
		t.Fatalf("Snapshot after skip = (%d, %d), want (0, 0)", inFlight, ahead)
	}
	// The discarded reservation's handle is now stale.
	if err := f.CommitHandle("t", 0, r1.Offset, r1.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle(stale) error = %v, want ErrHandleStale", err)
	}
	if res, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 1000); !res.Reserved || res.Offset != 500 {
		t.Fatalf("ReserveNext after skip = %+v, want offset 500", res)
	}
}

// DropPartition discards a shard so the next access recovers the
// frontier from disk (the move carries consumer.offset with it).
func TestDropPartitionRecoversFrontierFromDiskOnNextAccess(t *testing.T) {
	f, _ := newAuditInFlight(t, Caps{MaxInFlight: 10, MaxAckedAhead: 10})
	ctx := context.Background()
	recovered := int64(41)
	f.SetCommittedRecovery(func(string, int) (int64, bool) { return recovered, true })

	if res, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 1000); res.Offset != 42 {
		t.Fatalf("first reserve offset = %d, want 42", res.Offset)
	}
	recovered = 99 // the "moved back" partition's consumer.offset
	f.DropPartition("t", 0)
	if res, _ := f.ReserveNext(ctx, "t", 0, time.Minute, 1000); res.Offset != 100 {
		t.Fatalf("reserve after DropPartition offset = %d, want 100 (recovered from disk)", res.Offset)
	}
	f.DropPartition("t", 7) // unknown partition: no-op
}
