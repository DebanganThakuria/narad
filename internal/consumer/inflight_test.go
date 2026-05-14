package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// -- test helpers --------------------------------------------------------

func newTestInFlight(maxIF, maxAA int) *InFlight {
	caps := func(_ context.Context, _ string) (Caps, error) {
		return Caps{MaxInFlight: maxIF, MaxAckedAhead: maxAA}, nil
	}
	return NewInFlight(caps, nil) // nil onCommit — pure in-memory for tests
}

// committedOffset returns the last committed offset (Next - 1).
// Returns -1 when no messages have been committed yet.
func committedOffset(f *InFlight, topic string, partition int) int64 {
	return f.Next(topic, partition) - 1
}

func withClock(f *InFlight, now int64) {
	f.timeNow = func() int64 { return now }
}

const (
	testTopic    = "t"
	testPart     = 0
	testVT       = 30 * time.Second
	testDeepTail = 1_000_000
)

func mustReserve(t *testing.T, f *InFlight, logTail int64) ReserveResult {
	t.Helper()
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, logTail)
	if err != nil {
		t.Fatalf("ReserveNext: %v", err)
	}
	if !r.Reserved {
		t.Fatalf("ReserveNext: expected reservation, got skip=%q", r.SkipReason)
	}
	return r
}

func mustCommit(t *testing.T, f *InFlight, offset, nonce int64) {
	t.Helper()
	if err := f.CommitHandle(testTopic, testPart, offset, nonce); err != nil {
		t.Fatalf("CommitHandle(offset=%d): %v", offset, err)
	}
}

// reserveN reserves n messages in order and returns their nonces indexed by offset.
func reserveN(t *testing.T, f *InFlight, n int) map[int64]int64 {
	t.Helper()
	nonces := make(map[int64]int64, n)
	for range n {
		r := mustReserve(t, f, testDeepTail)
		nonces[r.Offset] = r.Nonce
	}
	return nonces
}

// -- tests ---------------------------------------------------------------

func TestReserveSkipsReservedUnexpiredOffset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	r1, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || !r1.Reserved || r1.Offset != 0 {
		t.Fatalf("first reserve: r=%+v err=%v", r1, err)
	}
	r2, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || !r2.Reserved || r2.Offset != 1 {
		t.Fatalf("second reserve: r=%+v err=%v", r2, err)
	}
	r3, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || !r3.Reserved || r3.Offset != 2 {
		t.Fatalf("third reserve: r=%+v err=%v", r3, err)
	}
}

func TestReserveSkipsAckedAhead(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	// Reserve 0, 1, 2 and capture nonces.
	nonces := reserveN(t, f, 3)

	// Ack offset 2 out-of-order → goes to ackedAhead.
	mustCommit(t, f, 2, nonces[2])

	// Advance clock past visibility timeout so 0 and 1 expire.
	withClock(f, 1000+60_000)

	// Reserve should pick 0 (expired), then 1 (expired), skip 2 (ackedAhead), pick 3.
	for _, want := range []int64{0, 1, 3} {
		r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 10)
		if err != nil || !r.Reserved || r.Offset != want {
			t.Fatalf("post-expiry reserve: got %+v, want offset=%d err=nil", r, want)
		}
	}
}

func TestCommitOutOfOrderDoesNotAdvanceFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	nonces := reserveN(t, f, 4)

	// Ack 2 and 3 out-of-order — frontier must stay at -1.
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 3, nonces[3])

	if got := committedOffset(f, testTopic, testPart); got != -1 {
		t.Fatalf("committed after out-of-order acks: got %d, want -1", got)
	}
}

func TestCommitFillsGapAndWalksForward(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	nonces := reserveN(t, f, 5) // offsets 0..4

	// Out-of-order: ack 2, 4, 1. Frontier stays at -1.
	for _, off := range []int64{2, 4, 1} {
		mustCommit(t, f, off, nonces[off])
	}
	if got := committedOffset(f, testTopic, testPart); got != -1 {
		t.Fatalf("before head ack: got %d, want -1", got)
	}

	// Ack 0 — walks through acked-ahead 1 and 2; stops at 3 (still in-flight).
	mustCommit(t, f, 0, nonces[0])
	if got := committedOffset(f, testTopic, testPart); got != 2 {
		t.Fatalf("after first walk: got %d, want 2", got)
	}

	// Ack 3 — walks through acked-ahead 4.
	mustCommit(t, f, 3, nonces[3])
	if got := committedOffset(f, testTopic, testPart); got != 4 {
		t.Fatalf("after second walk: got %d, want 4", got)
	}
}

func TestReserveInFlightCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newTestInFlight(3, 1024)
	withClock(f, 1000)

	for i := range int64(3) {
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
		if err != nil || !r.Reserved {
			t.Fatalf("reserve %d: %+v err=%v", i, r, err)
		}
	}
	r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("over cap: err=%v", err)
	}
	if r.Reserved || r.SkipReason != "cap" {
		t.Fatalf("expected cap-skip, got %+v", r)
	}
}

func TestCommitHandleAckedAheadCapRejects(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1024, 2) // max 2 out-of-order acks
	withClock(f, 1000)

	nonces := reserveN(t, f, 5)

	// Fill the ackedAhead set.
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 3, nonces[3])

	// Third out-of-order ack must be rejected.
	if err := f.CommitHandle(testTopic, testPart, 4, nonces[4]); !errors.Is(err, ErrAckedAheadFull) {
		t.Fatalf("want ErrAckedAheadFull, got %v", err)
	}

	// ackedAhead must not have grown (no leak from rejected insert).
	_, aa := f.Snapshot(testTopic, testPart)
	if aa != 2 {
		t.Fatalf("ackedAhead size leaked: got %d, want 2", aa)
	}
}

func TestCommitHandleStaleNonce(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	r1 := mustReserve(t, f, testDeepTail)

	// Advance clock past VT so offset 0 becomes eligible for re-reservation.
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)

	if r2.Nonce == r1.Nonce {
		t.Fatalf("nonce must change on re-reservation; both = %d", r1.Nonce)
	}

	// Old nonce is rejected.
	if err := f.CommitHandle(testTopic, testPart, r1.Offset, r1.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("stale nonce: want ErrHandleStale, got %v", err)
	}
	// New nonce succeeds.
	if err := f.CommitHandle(testTopic, testPart, r2.Offset, r2.Nonce); err != nil {
		t.Fatalf("fresh nonce: %v", err)
	}
}

func TestOnCommitCalledWhenFrontierAdvances(t *testing.T) {
	t.Parallel()

	var called []int64
	caps := func(_ context.Context, _ string) (Caps, error) {
		return Caps{MaxInFlight: 1024, MaxAckedAhead: 1024}, nil
	}
	onCommit := func(_ string, _ int, offset int64) {
		called = append(called, offset)
	}
	f := NewInFlight(caps, onCommit)
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)

	mustCommit(t, f, 0, nonces[0]) // frontier → 0; onCommit(0)
	mustCommit(t, f, 2, nonces[2]) // out-of-order; no onCommit
	mustCommit(t, f, 1, nonces[1]) // frontier → 1 then walks to 2; onCommit(2)

	if len(called) != 2 {
		t.Fatalf("onCommit called %d times, want 2; values=%v", len(called), called)
	}
	if called[0] != 0 || called[1] != 2 {
		t.Fatalf("onCommit values: got %v, want [0 2]", called)
	}
}

func TestExpiredEntriesDoNotBlockCap(t *testing.T) {
	t.Parallel()
	// maxInFlight = 2. Reserve 2 messages, let them expire without acking.
	// Without the heap, len(entries)=2 still counts toward the cap and
	// new consumers would get "cap" forever. With the heap, purgeExpired
	// removes them and new consumers get fresh reservations.
	f := newTestInFlight(2, 1024)
	withClock(f, 1000)

	mustReserve(t, f, testDeepTail) // offset 0
	mustReserve(t, f, testDeepTail) // offset 1

	// Advance clock past visibility timeout — both entries expire.
	withClock(f, 1000+60_000)

	// Without the heap fix this would return SkipReason="cap".
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext after expiry: %v", err)
	}
	if !r.Reserved {
		t.Fatalf("expired entries blocked cap: SkipReason=%q", r.SkipReason)
	}
	if r.Offset != 0 {
		t.Fatalf("expected offset 0 (re-reserved), got %d", r.Offset)
	}
}

func TestConcurrentReserveAcrossPartitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newTestInFlight(1024, 1024)

	const partitions = 8
	const perPart = 100

	var wg sync.WaitGroup
	var collisions atomic.Int64

	type seen struct {
		mu   sync.Mutex
		offs map[int64]struct{}
	}
	seenMap := make([]*seen, partitions)
	for i := range seenMap {
		seenMap[i] = &seen{offs: make(map[int64]struct{})}
	}

	for p := range partitions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perPart {
				r, err := f.ReserveNext(ctx, testTopic, p, testVT, testDeepTail)
				if err != nil || !r.Reserved {
					t.Errorf("p=%d: r=%+v err=%v", p, r, err)
					return
				}
				seenMap[p].mu.Lock()
				if _, dup := seenMap[p].offs[r.Offset]; dup {
					collisions.Add(1)
				}
				seenMap[p].offs[r.Offset] = struct{}{}
				seenMap[p].mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if c := collisions.Load(); c != 0 {
		t.Fatalf("got %d duplicate reservations across goroutines", c)
	}
	for p, s := range seenMap {
		if len(s.offs) != perPart {
			t.Fatalf("partition %d: got %d unique offsets, want %d", p, len(s.offs), perPart)
		}
	}
}
