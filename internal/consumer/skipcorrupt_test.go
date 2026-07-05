package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// SkipCorrupt at the frontier advances the committed offset, just like a
// commit — the difference is semantic (the offset is lost, not delivered).
func TestSkipCorruptAtFrontierAdvancesCommitted(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	nonces := reserveN(t, f, 1) // reserve offset 0
	if err := f.SkipCorrupt(testTopic, testPart, 0, nonces[0]); err != nil {
		t.Fatalf("SkipCorrupt: %v", err)
	}
	if got := committedOffset(f, testTopic, testPart); got != 0 {
		t.Fatalf("committed = %d, want 0", got)
	}
}

// A corrupt offset marked ahead of the frontier is collapsed past when a
// commit reaches it, mixing freely with acked-ahead offsets.
func TestSkipCorruptCollapsesContiguousAckedAndCorrupt(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)                                              // 0,1,2
	mustCommit(t, f, 2, nonces[2])                                           // acked-ahead {2}
	if err := f.SkipCorrupt(testTopic, testPart, 1, nonces[1]); err != nil { // corrupt-ahead {1}
		t.Fatalf("SkipCorrupt(1): %v", err)
	}
	// Frontier is still -1 (offset 0 unresolved).
	if got := committedOffset(f, testTopic, testPart); got != -1 {
		t.Fatalf("committed = %d, want -1 before head commit", got)
	}
	// Commit the head: frontier collapses 0 -> 1(corrupt) -> 2(acked).
	mustCommit(t, f, 0, nonces[0])
	if got := committedOffset(f, testTopic, testPart); got != 2 {
		t.Fatalf("committed = %d, want 2 (collapsed past corrupt+acked)", got)
	}
}

// ReserveNext never hands out a corrupt (poison) offset, even when it sits
// between in-flight offsets so the frontier can't collapse past it.
func TestReserveNextSkipsCorruptOffsetAhead(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	nonces := reserveN(t, f, 4)    // reserve 0,1,2,3
	mustCommit(t, f, 0, nonces[0]) // committed=0; 1,2,3 in-flight
	if err := f.SkipCorrupt(testTopic, testPart, 2, nonces[2]); err != nil {
		t.Fatalf("SkipCorrupt(2): %v", err)
	}
	// Frontier stays at 0 (offset 1 still in-flight blocks collapse).
	if got := committedOffset(f, testTopic, testPart); got != 0 {
		t.Fatalf("committed = %d, want 0", got)
	}
	// Next reservation skips corrupt 2 (and in-flight 1,3) -> offset 4.
	r := mustReserve(t, f, testDeepTail)
	if r.Offset != 4 {
		t.Fatalf("reserved offset %d, want 4 (corrupt 2 skipped)", r.Offset)
	}
}

// Skipping requires the live reservation nonce — a client (or a stale handle)
// cannot force a skip and silently drop data.
func TestSkipCorruptRejectsStaleHandle(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	nonces := reserveN(t, f, 1) // offset 0

	if err := f.SkipCorrupt(testTopic, testPart, 0, nonces[0]+999); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("wrong nonce: err=%v, want ErrHandleStale", err)
	}
	if err := f.SkipCorrupt(testTopic, testPart, 5, 1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("unreserved offset: err=%v, want ErrHandleStale", err)
	}
	if err := f.SkipCorrupt("no-such-topic", 0, 0, 1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("unknown shard: err=%v, want ErrHandleStale", err)
	}
	// The legitimate skip still works afterward.
	if err := f.SkipCorrupt(testTopic, testPart, 0, nonces[0]); err != nil {
		t.Fatalf("valid SkipCorrupt: %v", err)
	}
}

// The committed-frontier advance from a skip persists via onCommit, so the
// skip survives restart and the poison offset is not re-attempted.
func TestSkipCorruptPersistsAdvancedFrontier(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	gotOffset := int64(-2)
	caps := func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}
	f := NewInFlight(caps, func(_ string, _ int, off int64) {
		mu.Lock()
		gotOffset = off
		mu.Unlock()
	})
	withClock(f, 1000)

	nonces := reserveN(t, f, 1) // offset 0
	if err := f.SkipCorrupt(testTopic, testPart, 0, nonces[0]); err != nil {
		t.Fatalf("SkipCorrupt: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotOffset != 0 {
		t.Fatalf("onCommit persisted offset = %d, want 0", gotOffset)
	}
}

// The "ahead of frontier" budget is shared between acked-ahead and corrupt so
// poison offsets can't grow that state without bound.
func TestSkipCorruptRespectsCombinedAheadCap(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 2) // MaxAckedAhead = 2
	withClock(f, 1000)

	nonces := reserveN(t, f, 4) // 0,1,2,3; leave 0 unresolved so nothing collapses
	if err := f.SkipCorrupt(testTopic, testPart, 1, nonces[1]); err != nil {
		t.Fatalf("SkipCorrupt(1): %v", err) // corrupt-ahead {1}
	}
	mustCommit(t, f, 2, nonces[2]) // acked-ahead {2}; combined ahead = 2 = cap
	if err := f.SkipCorrupt(testTopic, testPart, 3, nonces[3]); !errors.Is(err, ErrAckedAheadFull) {
		t.Fatalf("skip beyond combined cap: err=%v, want ErrAckedAheadFull", err)
	}
}
