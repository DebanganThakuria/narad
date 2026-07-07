package consumer

import (
	"errors"
	"testing"
)

// Extending a live reservation moves its deadline: the offset must not
// expire at the ORIGINAL deadline — the stale heap entry (same nonce,
// old expiry) must be skipped, not treated as the live reservation.
func TestExtendHandleOutlivesOriginalDeadline(t *testing.T) {
	f := newClockedInFlight(4, 4)
	r := mustReserve(t, f, 10) // reserved at t=1000 with testVT

	// Just before the original deadline, extend to a fresh window.
	withClock(f, 1000+testVT.Milliseconds()-1)
	exp, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce, testVT)
	if err != nil {
		t.Fatalf("ExtendHandle: %v", err)
	}
	wantExp := 1000 + testVT.Milliseconds() - 1 + testVT.Milliseconds()
	if exp != wantExp {
		t.Fatalf("ExtendHandle expiry = %d, want %d", exp, wantExp)
	}

	// Past the ORIGINAL deadline the reservation must still be live:
	// the same offset is not re-reservable and the handle still acks.
	withClock(f, 1000+testVT.Milliseconds()+1)
	r2, err := f.ReserveNext(t.Context(), testTopic, testPart, testVT, 10)
	if err != nil {
		t.Fatalf("ReserveNext: %v", err)
	}
	if r2.Reserved && r2.Offset == r.Offset {
		t.Fatalf("offset %d was re-reserved despite the extension", r.Offset)
	}
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle after extension: %v", err)
	}
}

// The extended deadline is still a deadline: once it passes, the
// reservation expires and the handle goes stale.
func TestExtendHandleExpiresAtNewDeadline(t *testing.T) {
	f := newClockedInFlight(4, 4)
	r := mustReserve(t, f, 10)

	withClock(f, 1500)
	if _, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce, testVT); err != nil {
		t.Fatalf("ExtendHandle: %v", err)
	}

	withClock(f, 1500+testVT.Milliseconds()+1)
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle past extended deadline = %v, want %v", err, ErrHandleStale)
	}
}

func TestExtendHandleRejectsInvalidHandles(t *testing.T) {
	f := newClockedInFlight(4, 4)
	r := mustReserve(t, f, 10)

	// Wrong nonce.
	if _, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce+1, testVT); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("wrong nonce = %v, want %v", err, ErrHandleStale)
	}
	// Missing shard.
	if _, err := f.ExtendHandle("ghost", 0, 0, 1, testVT); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("missing shard = %v, want %v", err, ErrHandleStale)
	}
	// Expired reservation, even before any purge ran.
	withClock(f, 1000+testVT.Milliseconds()+1)
	if _, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce, testVT); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("expired handle = %v, want %v", err, ErrHandleStale)
	}
}

// Repeated extensions keep working with the same handle, and each one
// supersedes the previous deadline.
func TestExtendHandleRepeatedly(t *testing.T) {
	f := newClockedInFlight(4, 4)
	r := mustReserve(t, f, 10)

	now := int64(1000)
	for range 5 {
		now += testVT.Milliseconds() / 2
		withClock(f, now)
		if _, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce, testVT); err != nil {
			t.Fatalf("ExtendHandle at %d: %v", now, err)
		}
	}
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle after repeated extensions: %v", err)
	}
}

// ReleaseHandle (nack) makes the offset immediately re-reservable —
// under a NEW nonce — and kills the old handle.
func TestReleaseHandleRedeliversImmediately(t *testing.T) {
	f := newClockedInFlight(4, 4)
	released := make(chan struct{}, 1)
	f.SetReleaseNotifier(func(string, int) { released <- struct{}{} })
	r := mustReserve(t, f, 10)

	if err := f.ReleaseHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("ReleaseHandle: %v", err)
	}
	select {
	case <-released:
	default:
		t.Fatal("ReleaseHandle did not fire the release notifier")
	}

	// Same clock instant: the offset is redeliverable right away.
	r2 := mustReserve(t, f, 10)
	if r2.Offset != r.Offset {
		t.Fatalf("re-reserved offset = %d, want %d", r2.Offset, r.Offset)
	}
	if r2.Nonce == r.Nonce {
		t.Fatal("re-reservation kept the released nonce")
	}

	// The released handle is dead for ack, extend, and repeat release.
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("ack with released handle = %v, want %v", err, ErrHandleStale)
	}
	if _, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce, testVT); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("extend with released handle = %v, want %v", err, ErrHandleStale)
	}
	if err := f.ReleaseHandle(testTopic, testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("second release = %v, want %v", err, ErrHandleStale)
	}
	// The new reservation is unaffected.
	if err := f.CommitHandle(testTopic, testPart, r2.Offset, r2.Nonce); err != nil {
		t.Fatalf("CommitHandle for re-reservation: %v", err)
	}
}

// The stale heap entry a release leaves behind must not evict a later
// re-reservation of the same offset when the old deadline passes.
func TestReleaseHandleStaleHeapEntryHarmless(t *testing.T) {
	f := newClockedInFlight(4, 4)
	r := mustReserve(t, f, 10)
	if err := f.ReleaseHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("ReleaseHandle: %v", err)
	}
	r2 := mustReserve(t, f, 10) // same offset, new nonce, same deadline epoch

	// Advance past the ORIGINAL (and re-reserved) deadline minus one:
	// popping the released entry must not touch the live reservation.
	withClock(f, 1000+testVT.Milliseconds()-1)
	if _, err := f.ExtendHandle(testTopic, testPart, r2.Offset, r2.Nonce, testVT); err != nil {
		t.Fatalf("extend live re-reservation: %v", err)
	}
	withClock(f, 1000+testVT.Milliseconds()+1)
	if err := f.CommitHandle(testTopic, testPart, r2.Offset, r2.Nonce); err != nil {
		t.Fatalf("CommitHandle extended re-reservation: %v", err)
	}
}

// An extension must not consume or leak MaxInFlight capacity.
func TestExtendHandleKeepsCapAccounting(t *testing.T) {
	f := newClockedInFlight(1, 4)
	r := mustReserve(t, f, 10)
	if _, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce, testVT); err != nil {
		t.Fatalf("ExtendHandle: %v", err)
	}
	got, err := f.ReserveNext(t.Context(), testTopic, testPart, testVT, 10)
	if err != nil {
		t.Fatalf("ReserveNext: %v", err)
	}
	if got.Reserved || got.SkipReason != "cap" {
		t.Fatalf("ReserveNext after extend = %+v, want cap skip", got)
	}
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle: %v", err)
	}
	mustReserve(t, f, 10) // slot freed by the ack
}
