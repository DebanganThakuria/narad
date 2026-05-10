package consumer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTracker is a minimal in-memory OffsetTracker for unit tests. It
// records the highest committed offset per (topic, partition) and
// returns committed+1 from Next.
type fakeTracker struct {
	mu     sync.Mutex
	offset map[string]int64 // "topic:partition" -> last committed
}

func newFakeTracker() *fakeTracker {
	return &fakeTracker{offset: make(map[string]int64)}
}

func (f *fakeTracker) key(topic string, partition int) string {
	return topic + ":" + itoa(partition)
}

func (f *fakeTracker) Next(_ context.Context, topic string, partition int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.offset[f.key(topic, partition)]
	if !ok {
		return 0, nil // no commits yet → next is 0
	}
	return v + 1, nil
}

func (f *fakeTracker) Commit(_ context.Context, topic string, partition int, offset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := f.key(topic, partition)
	if cur, ok := f.offset[k]; !ok || offset > cur {
		f.offset[k] = offset
	}
	return nil
}

func (f *fakeTracker) committed(topic string, partition int) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.offset[f.key(topic, partition)]
}

func itoa(n int) string {
	// avoid strconv import bloat; tests-only
	if n == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(b[pos:])
}

func newTestInFlight(maxIF, maxAA int) (*InFlight, *fakeTracker) {
	tracker := newFakeTracker()
	caps := func(_ context.Context, _ string) (Caps, error) {
		return Caps{MaxInFlight: maxIF, MaxAckedAhead: maxAA}, nil
	}
	return NewInFlight(tracker, caps), tracker
}

// withClock lets a test inject a fixed-now timeNow function.
func withClock(f *InFlight, now int64) {
	f.timeNow = func() int64 { return now }
}

const (
	testTopic    = "t"
	testPart     = 0
	testVT       = 30 * time.Second
	testDeepTail = 1_000_000
)

func TestReserveSkipsReservedUnexpiredOffset(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, _ := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	// First reserve takes offset 0.
	r1, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || !r1.Reserved || r1.Offset != 0 {
		t.Fatalf("first reserve: r=%+v err=%v", r1, err)
	}
	// Second reserve must skip 0 and take 1.
	r2, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || !r2.Reserved || r2.Offset != 1 {
		t.Fatalf("second reserve: r=%+v err=%v", r2, err)
	}
	// Third reserve takes 2.
	r3, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || !r3.Reserved || r3.Offset != 2 {
		t.Fatalf("third reserve: r=%+v err=%v", r3, err)
	}
}

func TestReserveSkipsAckedAhead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, _ := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	// Reserve 0, 1, 2.
	for i := int64(0); i < 3; i++ {
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
		if err != nil || r.Offset != i {
			t.Fatalf("reserve %d: %+v %v", i, r, err)
		}
	}
	// Ack offset 2 (out-of-order). Goes to ackedAhead.
	if err := f.Commit(ctx, testTopic, testPart, 2); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	// Free entries[0] and entries[1] to simulate visibility expiry of 0,1.
	// Easiest way: advance the clock and re-attempt; but for this test, we
	// directly clear entries via a fresh shard inspection — not exposed.
	// Alternative: reserve up to 5 and verify reserve scans past acked-ahead 2.
	// New reserve with logTail=10 should NOT pick 2 even after vt expiry.
	withClock(f, 1000+(60_000)) // jump past vt; entries[0],[1],[2] all expired
	r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, 10)
	if err != nil {
		t.Fatalf("reserve after expiry: %v", err)
	}
	if !r.Reserved {
		t.Fatalf("expected reservation; got %+v", r)
	}
	// Should have picked 0 (first expired-and-not-acked-ahead). Then we
	// reserve again and should get 1, then skip 2 (acked-ahead), get 3.
	if r.Offset != 0 {
		t.Fatalf("first post-expiry reserve: got %d, want 0", r.Offset)
	}
	r, _ = f.ReserveNext(ctx, testTopic, testPart, testVT, 10)
	if r.Offset != 1 {
		t.Fatalf("second post-expiry reserve: got %d, want 1", r.Offset)
	}
	r, _ = f.ReserveNext(ctx, testTopic, testPart, testVT, 10)
	if r.Offset != 3 {
		t.Fatalf("third post-expiry reserve: got %d, want 3 (skipping acked-ahead 2)", r.Offset)
	}
}

func TestCommitOutOfOrderInsertsAckedAhead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, tracker := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	// Reserve 0..3.
	for i := int64(0); i < 4; i++ {
		if _, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	// Ack 2 first; committed should NOT advance.
	if err := f.Commit(ctx, testTopic, testPart, 2); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	if got := tracker.committed(testTopic, testPart); got != 0 {
		t.Fatalf("committed after ack 2: got %d, want 0", got)
	}
	// Ack 3 also out of order.
	if err := f.Commit(ctx, testTopic, testPart, 3); err != nil {
		t.Fatalf("commit 3: %v", err)
	}
	if got := tracker.committed(testTopic, testPart); got != 0 {
		t.Fatalf("committed after ack 3: got %d, want 0", got)
	}
}

func TestCommitFillsGapAndWalksForward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, tracker := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	for i := int64(0); i < 5; i++ {
		if _, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	// Out-of-order: ack 2, 4, 1.
	for _, off := range []int64{2, 4, 1} {
		if err := f.Commit(ctx, testTopic, testPart, off); err != nil {
			t.Fatalf("commit %d: %v", off, err)
		}
	}
	if got := tracker.committed(testTopic, testPart); got != 0 {
		t.Fatalf("committed before head ack: got %d, want 0", got)
	}
	// Now ack 0 — should walk through 1, 2 (both acked-ahead) but stop
	// at 3 (still in flight). Final committed = 2.
	// Note: tracker stores last-committed offset; semantic is "committed+1 = next".
	// Walk: advance starts at 0, then 1∈ahead → 1, 2∈ahead → 2, 3∉ahead → stop.
	// So tracker stores 2 (last committed offset).
	if err := f.Commit(ctx, testTopic, testPart, 0); err != nil {
		t.Fatalf("commit 0: %v", err)
	}
	if got := tracker.committed(testTopic, testPart); got != 2 {
		t.Fatalf("committed after walk: got %d, want 2", got)
	}
	// Ack 3 — should walk through 4 also.
	if err := f.Commit(ctx, testTopic, testPart, 3); err != nil {
		t.Fatalf("commit 3: %v", err)
	}
	if got := tracker.committed(testTopic, testPart); got != 4 {
		t.Fatalf("committed after second walk: got %d, want 4", got)
	}
}

func TestReserveInFlightCap(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, _ := newTestInFlight(3, 1024)
	withClock(f, 1000)

	for i := int64(0); i < 3; i++ {
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
		if err != nil || !r.Reserved {
			t.Fatalf("reserve %d at boundary: %+v %v", i, r, err)
		}
	}
	r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("reserve over cap: err=%v", err)
	}
	if r.Reserved {
		t.Fatalf("expected cap-skip, got reserved %+v", r)
	}
	if r.SkipReason != "cap" {
		t.Fatalf("SkipReason: got %q, want cap", r.SkipReason)
	}
}

func TestCommitAckedAheadCapRejects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, _ := newTestInFlight(1024, 2)
	withClock(f, 1000)

	// Reserve some so commits are valid.
	for i := int64(0); i < 5; i++ {
		if _, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	// Fill ackedAhead with 2, 3.
	if err := f.Commit(ctx, testTopic, testPart, 2); err != nil {
		t.Fatal(err)
	}
	if err := f.Commit(ctx, testTopic, testPart, 3); err != nil {
		t.Fatal(err)
	}
	// Now try to ack 4 — cap is full, must reject.
	err := f.Commit(ctx, testTopic, testPart, 4)
	if !errors.Is(err, ErrAckedAheadFull) {
		t.Fatalf("expected ErrAckedAheadFull, got %v", err)
	}
	// Snapshot must show ackedAhead size still 2 (no leak from rejected insert).
	_, aa := f.Snapshot(testTopic, testPart)
	if aa != 2 {
		t.Fatalf("ackedAhead leaked: got %d, want 2", aa)
	}
}

func TestCheckHandleStaleOnReReserve(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, _ := newTestInFlight(1024, 1024)
	withClock(f, 1000)

	// First reserve at offset 0.
	r1, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || r1.Offset != 0 {
		t.Fatalf("reserve: %+v %v", r1, err)
	}
	// Advance clock past VT — entry expires.
	withClock(f, 1000+60_000)
	// Second reserve at offset 0 (re-reserved with new nonce).
	r2, err := f.ReserveNext(ctx, testTopic, testPart, testVT, testDeepTail)
	if err != nil || r2.Offset != 0 {
		t.Fatalf("re-reserve: %+v %v", r2, err)
	}
	if r2.Nonce == r1.Nonce {
		t.Fatalf("expected fresh nonce on re-reserve; got same %d", r1.Nonce)
	}
	// Original handle (r1.Nonce) must now fail CheckHandle.
	if err := f.CheckHandle(ctx, testTopic, testPart, r1.Offset, r1.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("stale handle CheckHandle: got %v, want ErrHandleStale", err)
	}
	// Fresh handle (r2.Nonce) should pass.
	if err := f.CheckHandle(ctx, testTopic, testPart, r2.Offset, r2.Nonce); err != nil {
		t.Fatalf("fresh handle CheckHandle: %v", err)
	}
}

func TestConcurrentReserveAcrossPartitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f, _ := newTestInFlight(1024, 1024)

	// 8 partitions × 100 reservations each, all in parallel. Race
	// detector catches anything corrupt.
	const partitions = 8
	const perPart = 100
	var wg sync.WaitGroup
	var collisions atomic.Int64
	type seen struct {
		mu   sync.Mutex
		offs map[int64]struct{}
	}
	seenMap := make([]*seen, partitions)
	for i := 0; i < partitions; i++ {
		seenMap[i] = &seen{offs: make(map[int64]struct{})}
	}

	for p := 0; p < partitions; p++ {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perPart; i++ {
				r, err := f.ReserveNext(ctx, testTopic, p, testVT, testDeepTail)
				if err != nil || !r.Reserved {
					t.Errorf("p=%d i=%d: r=%+v err=%v", p, i, r, err)
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
		t.Fatalf("got %d duplicate reservations", c)
	}
	for p, s := range seenMap {
		if len(s.offs) != perPart {
			t.Fatalf("partition %d: got %d unique offsets, want %d", p, len(s.offs), perPart)
		}
	}
}
