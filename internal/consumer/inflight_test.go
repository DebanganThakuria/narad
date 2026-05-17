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
	f.setTimeNow(func() int64 { return now })
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
		go func(partition int) {
			defer wg.Done()
			for range perPart {
				r, err := f.ReserveNext(ctx, testTopic, partition, testVT, testDeepTail)
				if err != nil || !r.Reserved {
					t.Errorf("p=%d: r=%+v err=%v", partition, r, err)
					return
				}
				seenMap[partition].mu.Lock()
				if _, dup := seenMap[partition].offs[r.Offset]; dup {
					collisions.Add(1)
				}
				seenMap[partition].offs[r.Offset] = struct{}{}
				seenMap[partition].mu.Unlock()
			}
		}(p)
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

func TestDecodeHandleRejectsInvalidInputs(t *testing.T) {
	t.Parallel()

	for _, input := range []string{
		"%%%",
		EncodeHandle(Handle{Partition: 0, Offset: 0, Nonce: 1}),
		EncodeHandle(Handle{Topic: testTopic, Partition: -1, Offset: 0, Nonce: 1}),
		EncodeHandle(Handle{Topic: testTopic, Partition: 0, Offset: -1, Nonce: 1}),
		EncodeHandle(Handle{Topic: testTopic, Partition: 0, Offset: 0, Nonce: 0}),
	} {
		if _, err := DecodeHandle(input); !errors.Is(err, ErrHandleMalformed) {
			t.Fatalf("DecodeHandle(%q) error = %v, want %v", input, err, ErrHandleMalformed)
		}
	}
}

func TestEncodeHandleRoundTrip(t *testing.T) {
	t.Parallel()

	want := Handle{Topic: testTopic, Partition: 2, Offset: 42, Nonce: 7}
	got, err := DecodeHandle(EncodeHandle(want))
	if err != nil {
		t.Fatalf("DecodeHandle() error = %v", err)
	}
	if got != want {
		t.Fatalf("DecodeHandle() = %+v, want %+v", got, want)
	}
}

func TestInitSeedsCommittedOffset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)

	if err := f.Init(context.Background(), testTopic, testPart, 5); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 6 {
		t.Fatalf("Next() = %d, want 6", got)
	}
}

func TestInitRejectsInvalidCaps(t *testing.T) {
	t.Parallel()
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 0, MaxAckedAhead: 1}, nil
	}, nil)

	if err := f.Init(context.Background(), testTopic, testPart, 0); err == nil {
		t.Fatal("Init() error = nil, want error")
	}
}

func TestRefreshCapsUpdatesExistingShard(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)

	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil || r.SkipReason != "cap" {
		t.Fatalf("ReserveNext() = %+v, err=%v, want cap skip", r, err)
	}

	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 3}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil || !r.Reserved || r.Offset != 1 {
		t.Fatalf("ReserveNext() after refresh = %+v, err=%v", r, err)
	}
}

func TestRefreshCapsRejectsInvalidCaps(t *testing.T) {
	t.Parallel()
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 0, MaxAckedAhead: 1}, nil
	}, nil)

	if err := f.RefreshCaps(context.Background(), testTopic); err == nil {
		t.Fatal("RefreshCaps() error = nil, want error")
	}
}

func TestGetOrCreatePropagatesResolverError(t *testing.T) {
	t.Parallel()
	resolveErr := errors.New("resolve failed")
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{}, resolveErr
	}, nil)

	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); !errors.Is(err, resolveErr) {
		t.Fatalf("ReserveNext() error = %v, want %v", err, resolveErr)
	}
}

func TestRunPurgerRemovesExpiredEntries(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if inFlight, _ := f.Snapshot(testTopic, testPart); inFlight != 1 {
		t.Fatalf("Snapshot() inFlight = %d, want 1", inFlight)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.RunPurger(ctx, 10*time.Millisecond)

	withClock(f, 1000+60_000)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if inFlight, _ := f.Snapshot(testTopic, testPart); inFlight == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	inFlight, _ := f.Snapshot(testTopic, testPart)
	t.Fatalf("Snapshot() inFlight = %d, want 0 after purger", inFlight)
}

func TestNextReturnsZeroForMissingShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)

	if got := f.Next("missing", 3); got != 0 {
		t.Fatalf("Next() = %d, want 0", got)
	}
}

func TestSnapshotReturnsZeroForMissingShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)

	inFlight, ackedAhead := f.Snapshot("missing", 3)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestCommitHandleReturnsStaleForMissingShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)

	if err := f.CommitHandle(testTopic, testPart, 0, 1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextReturnsEmptyWhenTailAtFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 0)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.Reserved || r.SkipReason != "empty" {
		t.Fatalf("ReserveNext() = %+v, want empty skip", r)
	}
}

func TestReserveNextReturnsAllReservedWhenOnlyAckedAheadRemains(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 2)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.Reserved || r.SkipReason != "all_reserved" {
		t.Fatalf("ReserveNext() = %+v, want all_reserved skip", r)
	}
}

func TestCommitHandleRepeatedOutOfOrderAckDoesNotGrowAckedAhead(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	if err := f.CommitHandle(testTopic, testPart, 2, nonces[2]); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() second ack error = %v, want %v", err, ErrHandleStale)
	}
	_, ackedAhead := f.Snapshot(testTopic, testPart)
	if ackedAhead != 1 {
		t.Fatalf("Snapshot() ackedAhead = %d, want 1", ackedAhead)
	}
}

func TestCommitHandleRejectsWrongNonce(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	r := mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce+1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestRefreshCapsSkipsOtherTopics(t *testing.T) {
	t.Parallel()
	caps := map[string]Caps{
		testTopic: {MaxInFlight: 1, MaxAckedAhead: 1},
		"other":   {MaxInFlight: 1, MaxAckedAhead: 1},
	}
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		return caps[topic], nil
	}, nil)
	withClock(f, 1000)

	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(testTopic) error = %v", err)
	}
	if _, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}

	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}

	if r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil || !r.Reserved {
		t.Fatalf("ReserveNext(testTopic) after refresh = %+v, err=%v", r, err)
	}
	if r, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail); err != nil || r.SkipReason != "cap" {
		t.Fatalf("ReserveNext(other) = %+v, err=%v, want cap skip", r, err)
	}
}

func TestCommitHandleAdvancesThroughAckedAheadAndCallsOnCommitOnce(t *testing.T) {
	t.Parallel()

	var commits []int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(_ string, _ int, offset int64) {
		commits = append(commits, offset)
	})
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])

	if got := committedOffset(f, testTopic, testPart); got != 2 {
		t.Fatalf("committedOffset() = %d, want 2", got)
	}
	if len(commits) != 1 || commits[0] != 2 {
		t.Fatalf("onCommit offsets = %v, want [2]", commits)
	}
}

func TestCommitHandleInOrderDeletesInFlightEntry(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestCommitHandleOutOfOrderDeletesEntryAndParksAckedAhead(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 2 || ackedAhead != 1 {
		t.Fatalf("Snapshot() = (%d, %d), want (2, 1)", inFlight, ackedAhead)
	}
}

func TestPurgeExpiredSkipsStaleHeapEntry(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)

	r1 := mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)
	if r2.Offset != r1.Offset {
		t.Fatalf("re-reserved offset = %d, want %d", r2.Offset, r1.Offset)
	}

	withClock(f, 1000+120_000)
	f.purgeAll()
	if inFlight, _ := f.Snapshot(testTopic, testPart); inFlight != 0 {
		t.Fatalf("Snapshot() inFlight = %d, want 0", inFlight)
	}
}

func TestGetOrCreateCreatesShardOncePerPartition(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		calls.Add(1)
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	withClock(f, 1000)

	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() first error = %v", err)
	}
	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() second error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("resolver calls = %d, want 1", got)
	}
}

func TestReserveNextUsesMillisecondsForExpiry(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1234)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, 1500*time.Millisecond, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.ExpiresAtUnixMs != 2734 {
		t.Fatalf("ExpiresAtUnixMs = %d, want 2734", r.ExpiresAtUnixMs)
	}
}

func TestRunPurgerReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.RunPurger(ctx, time.Hour)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunPurger() did not stop after context cancellation")
	}
}

func TestRefreshCapsPropagatesResolverError(t *testing.T) {
	t.Parallel()
	resolveErr := errors.New("boom")
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{}, resolveErr
	}, nil)

	if err := f.RefreshCaps(context.Background(), testTopic); !errors.Is(err, resolveErr) {
		t.Fatalf("RefreshCaps() error = %v, want %v", err, resolveErr)
	}
}

func TestInitPropagatesResolverError(t *testing.T) {
	t.Parallel()
	resolveErr := errors.New("boom")
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{}, resolveErr
	}, nil)

	if err := f.Init(context.Background(), testTopic, testPart, 0); !errors.Is(err, resolveErr) {
		t.Fatalf("Init() error = %v, want %v", err, resolveErr)
	}
}

func TestDecodeHandleRejectsJSONShapeMismatch(t *testing.T) {
	t.Parallel()

	bad := EncodeHandle(Handle{Topic: testTopic, Partition: 0, Offset: 0, Nonce: 1})
	bad = bad[:len(bad)-1] + "A"
	if _, err := DecodeHandle(bad); err == nil {
		t.Fatal("DecodeHandle() error = nil, want error")
	}
}

func TestCommitHandleDoesNotCallOnCommitForOutOfOrderAck(t *testing.T) {
	t.Parallel()
	var called atomic.Int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(string, int, int64) {
		called.Add(1)
	})
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	if got := called.Load(); got != 0 {
		t.Fatalf("onCommit calls = %d, want 0", got)
	}
}

func TestReserveNextStartsAtCommittedPlusOneAfterInit(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 3); err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 4 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 4", r)
	}
}

func TestReserveNextReturnsAllReservedWhenEntriesCoverWindow(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, 2)
	mustReserve(t, f, 2)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 2)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.Reserved || r.SkipReason != "all_reserved" {
		t.Fatalf("ReserveNext() = %+v, want all_reserved", r)
	}
}

func TestDropTopicDoesNothingForMissingTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic("missing")
}

func TestCommitHandleLeavesAckedAheadIntactOnCapError(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 1)
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	if err := f.CommitHandle(testTopic, testPart, 1, nonces[1]); !errors.Is(err, ErrAckedAheadFull) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrAckedAheadFull)
	}
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 2 || ackedAhead != 1 {
		t.Fatalf("Snapshot() = (%d, %d), want (2, 1)", inFlight, ackedAhead)
	}
}

func TestReserveNextAfterDropTopicRecreatesShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestNextReflectsFrontierAfterDropTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic(testTopic)
	if got := f.Next(testTopic, testPart); got != 0 {
		t.Fatalf("Next() = %d, want 0", got)
	}
}

func TestRefreshCapsWithoutExistingShardStillSucceeds(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
}

func TestInitOverridesExistingShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if err := f.Init(context.Background(), testTopic, testPart, 9); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 10 {
		t.Fatalf("Next() = %d, want 10", got)
	}
}

func TestCommitHandleRejectsStaleAfterDropTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestRunPurgerWithNoShardsIsSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.purgeAll()
}

func TestCommitHandleAdvancesFrontierAfterExpiredGapReReserved(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r0 := mustReserve(t, f, testDeepTail)
	_ = mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r0b := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r0b.Offset, r0b.Nonce)
	if got := committedOffset(f, testTopic, testPart); got != 0 {
		t.Fatalf("committedOffset() = %d, want 0", got)
	}
	if err := f.CommitHandle(testTopic, testPart, r0.Offset, r0.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() old nonce error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextAllReservedWithMixedEntriesAndAckedAhead(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 3)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.Reserved || r.SkipReason != "all_reserved" {
		t.Fatalf("ReserveNext() = %+v, want all_reserved", r)
	}
}

func TestCommitHandleAfterRefreshCapsUsesNewAckedAheadCap(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 10, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 2, nonces[2])
	caps = Caps{MaxInFlight: 10, MaxAckedAhead: 3}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if err := f.CommitHandle(testTopic, testPart, 3, nonces[3]); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
}

func TestDecodeHandleRejectsEmptyString(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle(""); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestEncodeHandleProducesNonEmptyString(t *testing.T) {
	t.Parallel()
	if got := EncodeHandle(Handle{Topic: testTopic, Partition: 0, Offset: 0, Nonce: 1}); got == "" {
		t.Fatal("EncodeHandle() = empty, want non-empty")
	}
}

func TestReserveNextReturnsDistinctNonces(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r1 := mustReserve(t, f, testDeepTail)
	r2 := mustReserve(t, f, testDeepTail)
	if r1.Nonce == r2.Nonce {
		t.Fatalf("nonces are equal: %d", r1.Nonce)
	}
}

func TestInitSetsSnapshotToZeroSizes(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.Init(context.Background(), testTopic, testPart, 5); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestReserveNextAfterInitHonorsCap(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1, 10)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 5); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	mustReserve(t, f, testDeepTail)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.Reserved || r.SkipReason != "cap" {
		t.Fatalf("ReserveNext() = %+v, want cap", r)
	}
}

func TestCommitHandleAfterInitAdvancesFromSeededFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 5); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	r := mustReserve(t, f, testDeepTail)
	if r.Offset != 6 {
		t.Fatalf("reserved offset = %d, want 6", r.Offset)
	}
	mustCommit(t, f, r.Offset, r.Nonce)
	if got := committedOffset(f, testTopic, testPart); got != 6 {
		t.Fatalf("committedOffset() = %d, want 6", got)
	}
}

func TestRefreshCapsAfterDropTopicNoops(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic(testTopic)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
}

func TestSnapshotAfterDropTopicReturnsZero(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestReserveNextWithExactTailAfterCommitIsEmpty(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, 1)
	mustCommit(t, f, r.Offset, r.Nonce)
	res, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if res.Reserved || res.SkipReason != "empty" {
		t.Fatalf("ReserveNext() = %+v, want empty", res)
	}
}

func TestCommitHandleOutOfOrderThenGapThenHeadPreservesOrder(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 3, nonces[3])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	if got := committedOffset(f, testTopic, testPart); got != 1 {
		t.Fatalf("committedOffset() = %d, want 1", got)
	}
	mustCommit(t, f, 2, nonces[2])
	if got := committedOffset(f, testTopic, testPart); got != 3 {
		t.Fatalf("committedOffset() = %d, want 3", got)
	}
}

func TestCommitHandleWithWrongPartitionIsStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle(testTopic, 1, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextDifferentPartitionsStartAtZeroIndependently(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r0, err := f.ReserveNext(context.Background(), testTopic, 0, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(0) error = %v", err)
	}
	r1, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(1) error = %v", err)
	}
	if r0.Offset != 0 || r1.Offset != 0 {
		t.Fatalf("offsets = (%d, %d), want (0, 0)", r0.Offset, r1.Offset)
	}
}

func TestCommitHandleDifferentPartitionsAdvanceIndependently(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r0, err := f.ReserveNext(context.Background(), testTopic, 0, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(0) error = %v", err)
	}
	r1, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(1) error = %v", err)
	}
	if err := f.CommitHandle(testTopic, 0, r0.Offset, r0.Nonce); err != nil {
		t.Fatalf("CommitHandle(0) error = %v", err)
	}
	if got := f.Next(testTopic, 0); got != 1 {
		t.Fatalf("Next(0) = %d, want 1", got)
	}
	if got := f.Next(testTopic, 1); got != 0 {
		t.Fatalf("Next(1) = %d, want 0", got)
	}
	if err := f.CommitHandle(testTopic, 1, r1.Offset, r1.Nonce); err != nil {
		t.Fatalf("CommitHandle(1) error = %v", err)
	}
}

func TestRefreshCapsForOneTopicKeepsOtherTopicState(t *testing.T) {
	t.Parallel()
	caps := map[string]Caps{
		testTopic: {MaxInFlight: 1, MaxAckedAhead: 1},
		"other":   {MaxInFlight: 1, MaxAckedAhead: 1},
	}
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		return caps[topic], nil
	}, nil)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), testTopic, 0, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(testTopic) error = %v", err)
	}
	if _, err := f.ReserveNext(context.Background(), "other", 0, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if got := f.Next("other", 0); got != 0 {
		t.Fatalf("Next(other) = %d, want 0", got)
	}
}

func TestPurgeAllWithMultipleShardsIsSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), testTopic, 0, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(0) error = %v", err)
	}
	if _, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(1) error = %v", err)
	}
	withClock(f, 1000+60_000)
	f.purgeAll()
	if inFlight, _ := f.Snapshot(testTopic, 0); inFlight != 0 {
		t.Fatalf("Snapshot(0) inFlight = %d, want 0", inFlight)
	}
	if inFlight, _ := f.Snapshot(testTopic, 1); inFlight != 0 {
		t.Fatalf("Snapshot(1) inFlight = %d, want 0", inFlight)
	}
}

func TestCommitHandleAckedAheadThenDropTopicClearsState(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	f.DropTopic(testTopic)
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestCommitHandleAckedAheadCapAfterRefreshStillEnforced(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 10, MaxAckedAhead: 2}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	nonces := reserveN(t, f, 5)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 3, nonces[3])
	if err := f.CommitHandle(testTopic, testPart, 4, nonces[4]); !errors.Is(err, ErrAckedAheadFull) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrAckedAheadFull)
	}
}

func TestDecodeHandleRejectsMissingTopic(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle(EncodeHandle(Handle{Partition: 0, Offset: 1, Nonce: 1})); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestDecodeHandleRejectsZeroNonce(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle(EncodeHandle(Handle{Topic: testTopic, Partition: 0, Offset: 1, Nonce: 0})); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestCommitHandleAfterDropTopicReturnsStaleEvenWithFormerNonce(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextVisibilityTimeoutZeroExpiresImmediatelyOnNextSweep(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, 0, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	f.purgeAll()
	if inFlight, _ := f.Snapshot(testTopic, testPart); inFlight != 0 {
		t.Fatalf("Snapshot() inFlight = %d, want 0", inFlight)
	}
}

func TestInitAfterDropTopicRecreatesSeededShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic(testTopic)
	if err := f.Init(context.Background(), testTopic, testPart, 2); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 3 {
		t.Fatalf("Next() = %d, want 3", got)
	}
}

func TestRefreshCapsWithNoMatchingShardStillCallsResolver(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		calls.Add(1)
		return Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("resolver calls = %d, want 1", got)
	}
}

func TestCommitHandleInOrderCallsOnCommitWithAdvancedOffset(t *testing.T) {
	t.Parallel()
	var got int64 = -1
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(string, int, int64) {
		got = 0
	})
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
	if got != 0 {
		t.Fatalf("onCommit got = %d, want 0", got)
	}
}

func TestReserveNextAfterAckedAheadCollapseStartsAtNewFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 3 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 3", r)
	}
}

func TestDropTopicClearsMultiplePartitionsForSameTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if _, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(partition 1) error = %v", err)
	}
	f.DropTopic(testTopic)
	if got := f.Next(testTopic, 0); got != 0 {
		t.Fatalf("Next(0) = %d, want 0", got)
	}
	if got := f.Next(testTopic, 1); got != 0 {
		t.Fatalf("Next(1) = %d, want 0", got)
	}
}

func TestCommitHandleWithMismatchedOffsetIsStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle(testTopic, testPart, 99, 1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextUsesSeparateShardsPerTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r1, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(testTopic) error = %v", err)
	}
	r2, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	if r1.Offset != 0 || r2.Offset != 0 {
		t.Fatalf("offsets = (%d, %d), want (0, 0)", r1.Offset, r2.Offset)
	}
}

func TestCommitHandleAfterAllReservedGapStillAdvancesWhenHeadAcked(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 0, nonces[0])
	if got := committedOffset(f, testTopic, testPart); got != 0 {
		t.Fatalf("committedOffset() = %d, want 0", got)
	}
	mustCommit(t, f, 1, nonces[1])
	if got := committedOffset(f, testTopic, testPart); got != 2 {
		t.Fatalf("committedOffset() = %d, want 2", got)
	}
}

func TestRefreshCapsCanIncreaseAckedAheadCapacity(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 10, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 2, nonces[2])
	caps = Caps{MaxInFlight: 10, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if err := f.CommitHandle(testTopic, testPart, 3, nonces[3]); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
}

func TestReserveNextReturnsOffsetZeroOnFreshShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestSnapshotCountsAfterOutOfOrderAndCommitCollapse(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 0, nonces[0])
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 1 || ackedAhead != 1 {
		t.Fatalf("Snapshot() = (%d, %d), want (1, 1)", inFlight, ackedAhead)
	}
	mustCommit(t, f, 1, nonces[1])
	inFlight, ackedAhead = f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestRunPurgerCanSweepAfterReReservation(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	mustReserve(t, f, testDeepTail)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.RunPurger(ctx, 10*time.Millisecond)
	withClock(f, 1000+120_000)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if inFlight, _ := f.Snapshot(testTopic, testPart); inFlight == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	inFlight, _ := f.Snapshot(testTopic, testPart)
	t.Fatalf("Snapshot() inFlight = %d, want 0", inFlight)
}

func TestReserveNextWithZeroTailOnFreshShardIsEmpty(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 0)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.Reserved || r.SkipReason != "empty" {
		t.Fatalf("ReserveNext() = %+v, want empty", r)
	}
}

func TestCommitHandleWithNoReservationAndExistingShardIsStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle(testTopic, testPart, 99, 1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestRefreshCapsAfterInitAffectsSeededShard(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 0); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	mustReserve(t, f, testDeepTail)
	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil || !r.Reserved {
		t.Fatalf("ReserveNext() = %+v, err=%v, want reserved", r, err)
	}
}

func TestDropTopicClearsSeededShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.Init(context.Background(), testTopic, testPart, 3); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	f.DropTopic(testTopic)
	if got := f.Next(testTopic, testPart); got != 0 {
		t.Fatalf("Next() = %d, want 0", got)
	}
}

func TestDecodeHandleRejectsNegativeOffset(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle(EncodeHandle(Handle{Topic: testTopic, Partition: 0, Offset: -1, Nonce: 1})); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestDecodeHandleRejectsNegativePartition(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle(EncodeHandle(Handle{Topic: testTopic, Partition: -1, Offset: 1, Nonce: 1})); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestCommitHandleWrongPartitionAfterReserveIsStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle(testTopic, 9, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextAfterFrontierAdvanceUsesNextOffset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r2.Reserved || r2.Offset != 1 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 1", r2)
	}
}

func TestSnapshotTracksOnlyCurrentPartition(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if _, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(partition 1) error = %v", err)
	}
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 1 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (1, 0)", inFlight, ackedAhead)
	}
}

func TestReserveNextAfterExpiredCapReleaseUsesLowestOffset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestCommitHandleDoesNotAdvancePastMissingGap(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 3, nonces[3])
	mustCommit(t, f, 0, nonces[0])
	if got := committedOffset(f, testTopic, testPart); got != 0 {
		t.Fatalf("committedOffset() = %d, want 0", got)
	}
}

func TestRefreshCapsInvalidAckedAheadCapRejected(t *testing.T) {
	t.Parallel()
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 1, MaxAckedAhead: 0}, nil
	}, nil)
	if err := f.RefreshCaps(context.Background(), testTopic); err == nil {
		t.Fatal("RefreshCaps() error = nil, want error")
	}
}

func TestInitInvalidAckedAheadCapRejected(t *testing.T) {
	t.Parallel()
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 1, MaxAckedAhead: 0}, nil
	}, nil)
	if err := f.Init(context.Background(), testTopic, testPart, 0); err == nil {
		t.Fatal("Init() error = nil, want error")
	}
}

func TestCommitHandleOnFreshShardWithoutReservationIsStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if err := f.CommitHandle(testTopic, testPart, 1, 1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestDropTopicLeavesDifferentTopicUntouched(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if _, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	f.DropTopic(testTopic)
	if got := f.Next("other", testPart); got != 0 {
		t.Fatalf("Next(other) = %d, want 0", got)
	}
}

func TestReserveNextOnOtherTopicUnaffectedByDrop(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	f.DropTopic(testTopic)
	if got := f.Next("other", testPart); got != 0 {
		t.Fatalf("Next(other) = %d, want 0", got)
	}
}

func TestRunPurgerContextAlreadyCanceledReturnsImmediately(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		f.RunPurger(ctx, time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunPurger() did not return")
	}
}

func TestRefreshCapsCanRunBeforeShardCreation(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
}

func TestInitCanBeCalledTwice(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.Init(context.Background(), testTopic, testPart, 1); err != nil {
		t.Fatalf("Init() first error = %v", err)
	}
	if err := f.Init(context.Background(), testTopic, testPart, 2); err != nil {
		t.Fatalf("Init() second error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 3 {
		t.Fatalf("Next() = %d, want 3", got)
	}
}

func TestCommitHandleStaleForDifferentTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle("other", testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextReusesExpiredOffsetBeforeHigherOffsets(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestCommitHandleCanAdvanceAfterReusedExpiredOffset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	if got := committedOffset(f, testTopic, testPart); got != 0 {
		t.Fatalf("committedOffset() = %d, want 0", got)
	}
}

func TestSnapshotAfterExpiredPurgeReturnsZero(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	f.purgeAll()
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestRefreshCapsResolverCanSeeTopic(t *testing.T) {
	t.Parallel()
	var seen string
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		seen = topic
		return Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if seen != testTopic {
		t.Fatalf("resolver saw topic %q, want %q", seen, testTopic)
	}
}

func TestInitResolverCanSeeTopic(t *testing.T) {
	t.Parallel()
	var seen string
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		seen = topic
		return Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	if err := f.Init(context.Background(), testTopic, testPart, 0); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if seen != testTopic {
		t.Fatalf("resolver saw topic %q, want %q", seen, testTopic)
	}
}

func TestReserveNextResolverCanSeeTopic(t *testing.T) {
	t.Parallel()
	var seen string
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		seen = topic
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if seen != testTopic {
		t.Fatalf("resolver saw topic %q, want %q", seen, testTopic)
	}
}

func TestCommitHandlePreservesAckedAheadUntilGapClosed(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 3, nonces[3])
	mustCommit(t, f, 0, nonces[0])
	_, ackedAhead := f.Snapshot(testTopic, testPart)
	if ackedAhead != 1 {
		t.Fatalf("Snapshot() ackedAhead = %d, want 1", ackedAhead)
	}
}

func TestReserveNextAfterAckedAheadCollapseStartsAtTailFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 3)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 2 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 2", r)
	}
}

func TestCommitHandleWrongNonceAfterDropTopicStillStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce+1); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextDoesNotNeedInit(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestInitThenDropThenReserveStartsFresh(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.Init(context.Background(), testTopic, testPart, 5); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	f.DropTopic(testTopic)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestCommitHandleAfterCapReleaseStillWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle(testTopic, testPart, r2.Offset, r2.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() old nonce error = %v, want %v", err, ErrHandleStale)
	}
}

func TestSnapshotAfterInitOverrideIsReset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if err := f.Init(context.Background(), testTopic, testPart, 9); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestRefreshCapsDoesNotCreateShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 0 {
		t.Fatalf("Next() = %d, want 0", got)
	}
}

func TestReserveNextAfterRefreshWithoutShardStillStartsAtZero(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestDropTopicAfterRefreshStillSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	f.DropTopic(testTopic)
}

func TestCommitHandleAckedAheadThenCollapseClearsAckedAhead(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	_, ackedAhead := f.Snapshot(testTopic, testPart)
	if ackedAhead != 0 {
		t.Fatalf("Snapshot() ackedAhead = %d, want 0", ackedAhead)
	}
}

func TestReserveNextCanReserveAfterCollapse(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 2 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 2", r)
	}
}

func TestCommitHandleOnCommittedOffsetIsStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestReserveNextAfterCommittedOffsetUsesFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	if got := f.Next(testTopic, testPart); got != 1 {
		t.Fatalf("Next() = %d, want 1", got)
	}
}

func TestRunPurgerSweepsWithoutPanickingAfterDrop(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic(testTopic)
	ctx, cancel := context.WithCancel(context.Background())
	go f.RunPurger(ctx, 10*time.Millisecond)
	cancel()
}

func TestDecodeHandleRejectsNonJSONPayload(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle("bm90LWpzb24"); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestCommitHandleOutOfOrderLeavesLowerOffsetsReservableAfterExpiry(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	withClock(f, 1000+60_000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestInitPreservesIndependentPartitions(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.Init(context.Background(), testTopic, 0, 1); err != nil {
		t.Fatalf("Init(0) error = %v", err)
	}
	if err := f.Init(context.Background(), testTopic, 1, 5); err != nil {
		t.Fatalf("Init(1) error = %v", err)
	}
	if got := f.Next(testTopic, 0); got != 2 {
		t.Fatalf("Next(0) = %d, want 2", got)
	}
	if got := f.Next(testTopic, 1); got != 6 {
		t.Fatalf("Next(1) = %d, want 6", got)
	}
}

func TestRefreshCapsPreservesShardState(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 0 {
		t.Fatalf("Next() = %d, want 0", got)
	}
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
}

func TestReserveNextZeroVisibilityStillReserves(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, 0, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved {
		t.Fatalf("ReserveNext() = %+v, want reserved", r)
	}
}

func TestCommitHandleOutOfOrderThenHeadAdvancesOnce(t *testing.T) {
	t.Parallel()
	var commits []int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(string, int, int64) {
		commits = append(commits, 0)
	})
	withClock(f, 1000)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	if len(commits) != 1 {
		t.Fatalf("onCommit calls = %d, want 1", len(commits))
	}
}

func TestNextAfterInitOverrideReflectsLatestCommit(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.Init(context.Background(), testTopic, testPart, 1); err != nil {
		t.Fatalf("Init() first error = %v", err)
	}
	if err := f.Init(context.Background(), testTopic, testPart, 4); err != nil {
		t.Fatalf("Init() second error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 5 {
		t.Fatalf("Next() = %d, want 5", got)
	}
}

func TestSnapshotAfterRefreshKeepsCounts(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 1 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (1, 0)", inFlight, ackedAhead)
	}
}

func TestCommitHandleAfterInitAndReserveCommitsExpectedOffset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 2); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	if got := committedOffset(f, testTopic, testPart); got != 3 {
		t.Fatalf("committedOffset() = %d, want 3", got)
	}
}

func TestReserveNextAfterPurgeAndCommitStartsAtNewFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r2.Offset, r2.Nonce)
	r3, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r3.Reserved || r3.Offset != 1 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 1", r3)
	}
	_ = r
}

func TestRefreshCapsWithExistingOtherTopicShardIgnoresIt(t *testing.T) {
	t.Parallel()
	caps := map[string]Caps{
		testTopic: {MaxInFlight: 1, MaxAckedAhead: 1},
		"other":   {MaxInFlight: 1, MaxAckedAhead: 1},
	}
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		return caps[topic], nil
	}, nil)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), "other", 0, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
}

func TestDropTopicAfterInitOverrideClearsLatestState(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.Init(context.Background(), testTopic, testPart, 1); err != nil {
		t.Fatalf("Init() first error = %v", err)
	}
	if err := f.Init(context.Background(), testTopic, testPart, 5); err != nil {
		t.Fatalf("Init() second error = %v", err)
	}
	f.DropTopic(testTopic)
	if got := f.Next(testTopic, testPart); got != 0 {
		t.Fatalf("Next() = %d, want 0", got)
	}
}

func TestReserveNextOnFreshTopicAfterOtherTopicStateStartsAtZero(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), "other", 0, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(testTopic) error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestSnapshotAfterOtherTopicDropUnchangedForCurrentTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if _, err := f.ReserveNext(context.Background(), "other", 0, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	f.DropTopic("other")
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 1 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (1, 0)", inFlight, ackedAhead)
	}
}

func TestCommitHandleAfterOtherTopicDropStillWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
}

func TestReserveNextAfterOtherTopicDropStillUsesCurrentFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic("other")
	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r2.Reserved || r2.Offset != 1 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 1", r2)
	}
}

func TestRunPurgerAfterOtherTopicDropStillSweepsCurrentTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.RunPurger(ctx, 10*time.Millisecond)
	withClock(f, 1000+60_000)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if inFlight, _ := f.Snapshot(testTopic, testPart); inFlight == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	inFlight, _ := f.Snapshot(testTopic, testPart)
	t.Fatalf("Snapshot() inFlight = %d, want 0", inFlight)
}

func TestCommitHandleAfterRefreshAndDropOtherTopicStillAdvances(t *testing.T) {
	t.Parallel()
	caps := map[string]Caps{
		testTopic: {MaxInFlight: 1, MaxAckedAhead: 1},
		"other":   {MaxInFlight: 1, MaxAckedAhead: 1},
	}
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		return caps[topic], nil
	}, nil)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	f.DropTopic("other")
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
}

func TestReserveNextAfterCommitAndOtherTopicDropStillStartsAtNext(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic("other")
	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r2.Reserved || r2.Offset != 1 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 1", r2)
	}
}

func TestDecodeHandleRoundTripAcrossTopics(t *testing.T) {
	t.Parallel()
	for _, h := range []Handle{{Topic: testTopic, Partition: 0, Offset: 1, Nonce: 1}, {Topic: "other", Partition: 2, Offset: 9, Nonce: 3}} {
		got, err := DecodeHandle(EncodeHandle(h))
		if err != nil {
			t.Fatalf("DecodeHandle() error = %v", err)
		}
		if got != h {
			t.Fatalf("DecodeHandle() = %+v, want %+v", got, h)
		}
	}
}

func TestReserveNextAfterAckedAheadCollapseAndOtherTopicDropStillUsesFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	f.DropTopic("other")
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 2 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 2", r)
	}
}

func TestCommitHandleAfterFreshShardRefreshAndDropOtherTopicStillWorks(t *testing.T) {
	t.Parallel()
	caps := map[string]Caps{
		testTopic: {MaxInFlight: 10, MaxAckedAhead: 10},
		"other":   {MaxInFlight: 10, MaxAckedAhead: 10},
	}
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		return caps[topic], nil
	}, nil)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	f.DropTopic("other")
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
}

func TestSnapshotAfterCommitAndOtherTopicDropStillZero(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic("other")
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestReserveNextAfterNoopRefreshStillWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestRunPurgerNoShardsContextCanceledSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.RunPurger(ctx, time.Hour)
}

func TestCommitHandleAfterInitOverrideThenReserveWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 1); err != nil {
		t.Fatalf("Init() first error = %v", err)
	}
	if err := f.Init(context.Background(), testTopic, testPart, 4); err != nil {
		t.Fatalf("Init() second error = %v", err)
	}
	r := mustReserve(t, f, testDeepTail)
	if r.Offset != 5 {
		t.Fatalf("reserved offset = %d, want 5", r.Offset)
	}
}

func TestCommitHandleOutOfOrderAfterInitParksAckedAhead(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 4); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	r1 := mustReserve(t, f, testDeepTail)
	r2 := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r2.Offset, r2.Nonce)
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 1 || ackedAhead != 1 {
		t.Fatalf("Snapshot() = (%d, %d), want (1, 1)", inFlight, ackedAhead)
	}
	_ = r1
}

func TestReserveNextAfterInitOutOfOrderCollapseStartsAtCorrectOffset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 4); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	r1 := mustReserve(t, f, testDeepTail)
	r2 := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r2.Offset, r2.Nonce)
	mustCommit(t, f, r1.Offset, r1.Nonce)
	r3, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r3.Reserved || r3.Offset != 7 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 7", r3)
	}
}

func TestRefreshCapsThenInitStillUsesInitCommit(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if err := f.Init(context.Background(), testTopic, testPart, 2); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if got := f.Next(testTopic, testPart); got != 3 {
		t.Fatalf("Next() = %d, want 3", got)
	}
}

func TestDropTopicAfterRefreshAndInitClearsState(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	if err := f.Init(context.Background(), testTopic, testPart, 2); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	f.DropTopic(testTopic)
	if got := f.Next(testTopic, testPart); got != 0 {
		t.Fatalf("Next() = %d, want 0", got)
	}
}

func TestDecodeHandleRejectsCorruptBase64(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle("@"); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestCommitHandleCanAdvanceFromSeededPartitionAfterRefresh(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	if err := f.Init(context.Background(), testTopic, testPart, 1); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	mustCommit(t, f, r.Offset, r.Nonce)
	if got := committedOffset(f, testTopic, testPart); got != 2 {
		t.Fatalf("committedOffset() = %d, want 2", got)
	}
}

func TestRunPurgerWithEmptyIntervalContextCancelStillReturns(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.RunPurger(ctx, time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunPurger() did not return")
	}
}

func TestReserveNextAfterCommitAndRefreshUsesAdvancedFrontier(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps() error = %v", err)
	}
	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r2.Reserved || r2.Offset != 1 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 1", r2)
	}
}

func TestCommitHandleOutOfOrderDoesNotCallOnCommit(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(string, int, int64) {
		calls.Add(1)
	})
	withClock(f, 1000)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	if got := calls.Load(); got != 0 {
		t.Fatalf("onCommit calls = %d, want 0", got)
	}
}

func TestCommitHandleHeadAfterOutOfOrderCallsOnCommit(t *testing.T) {
	t.Parallel()
	var commits []int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(string, int, int64) {
		commits = append(commits, 1)
	})
	withClock(f, 1000)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	if len(commits) != 1 {
		t.Fatalf("onCommit calls = %d, want 1", len(commits))
	}
}

func TestInitAfterExistingShardReplacesEntries(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if err := f.Init(context.Background(), testTopic, testPart, 4); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	inFlight, ackedAhead := f.Snapshot(testTopic, testPart)
	if inFlight != 0 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (0, 0)", inFlight, ackedAhead)
	}
}

func TestReserveNextAfterInitReplacementStartsAtNewFrontier(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if err := f.Init(context.Background(), testTopic, testPart, 4); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 5 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 5", r)
	}
}

func TestSnapshotForDifferentTopicIsIndependent(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if _, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	inFlight, ackedAhead := f.Snapshot("other", testPart)
	if inFlight != 1 || ackedAhead != 0 {
		t.Fatalf("Snapshot(other) = (%d, %d), want (1, 0)", inFlight, ackedAhead)
	}
}

func TestCommitHandleWrongTopicAfterReserveIsStale(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	if err := f.CommitHandle("other", testPart, r.Offset, r.Nonce); !errors.Is(err, ErrHandleStale) {
		t.Fatalf("CommitHandle() error = %v, want %v", err, ErrHandleStale)
	}
}

func TestDropTopicCanRemoveOtherTopicOnly(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if _, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	f.DropTopic("other")
	if got := f.Next(testTopic, testPart); got != 0 {
		t.Fatalf("Next(testTopic) = %d, want 0", got)
	}
	if got := f.Next("other", testPart); got != 0 {
		t.Fatalf("Next(other) = %d, want 0", got)
	}
}

func TestReserveNextAfterOtherTopicDropStillCountsCurrentEntries(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(1, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if r.Reserved || r.SkipReason != "cap" {
		t.Fatalf("ReserveNext() = %+v, want cap", r)
	}
}

func TestCommitHandleAfterOtherTopicDropPreservesCurrentState(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	mustCommit(t, f, r.Offset, r.Nonce)
	if got := committedOffset(f, testTopic, testPart); got != 0 {
		t.Fatalf("committedOffset() = %d, want 0", got)
	}
}

func TestRunPurgerAfterCurrentAndOtherTopicStateSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	mustReserve(t, f, testDeepTail)
	if _, err := f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other) error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.RunPurger(ctx, time.Hour)
}

func TestDecodeHandleRejectsWhitespace(t *testing.T) {
	t.Parallel()
	if _, err := DecodeHandle("   "); !errors.Is(err, ErrHandleMalformed) {
		t.Fatalf("DecodeHandle() error = %v, want %v", err, ErrHandleMalformed)
	}
}

func TestEncodeHandleDeterministicForSameInput(t *testing.T) {
	t.Parallel()
	h := Handle{Topic: testTopic, Partition: 0, Offset: 1, Nonce: 1}
	if a, b := EncodeHandle(h), EncodeHandle(h); a != b {
		t.Fatalf("EncodeHandle() outputs differ: %q vs %q", a, b)
	}
}

func TestReserveNextDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r1, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(testTopic,1) error = %v", err)
	}
	r2, err := f.ReserveNext(context.Background(), "other", 2, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(other,2) error = %v", err)
	}
	if r1.Offset != 0 || r2.Offset != 0 {
		t.Fatalf("offsets = (%d, %d), want (0, 0)", r1.Offset, r2.Offset)
	}
}

func TestCommitHandleDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r1, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(testTopic,1) error = %v", err)
	}
	r2, err := f.ReserveNext(context.Background(), "other", 2, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(other,2) error = %v", err)
	}
	if err := f.CommitHandle(testTopic, 1, r1.Offset, r1.Nonce); err != nil {
		t.Fatalf("CommitHandle(testTopic,1) error = %v", err)
	}
	if err := f.CommitHandle("other", 2, r2.Offset, r2.Nonce); err != nil {
		t.Fatalf("CommitHandle(other,2) error = %v", err)
	}
}

func TestSnapshotDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), testTopic, 1, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(testTopic,1) error = %v", err)
	}
	inFlight, ackedAhead := f.Snapshot(testTopic, 1)
	if inFlight != 1 || ackedAhead != 0 {
		t.Fatalf("Snapshot() = (%d, %d), want (1, 0)", inFlight, ackedAhead)
	}
}

func TestDropTopicDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	if _, err := f.ReserveNext(context.Background(), "other", 2, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext(other,2) error = %v", err)
	}
	f.DropTopic(testTopic)
	if got := f.Next("other", 2); got != 0 {
		t.Fatalf("Next(other,2) = %d, want 0", got)
	}
}

func TestReserveNextAfterDropOfDifferentTopicStillWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic("other")
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved || r.Offset != 0 {
		t.Fatalf("ReserveNext() = %+v, want reserved offset 0", r)
	}
}

func TestCommitHandleAfterDropOfDifferentTopicStillWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
		t.Fatalf("CommitHandle() error = %v", err)
	}
}
