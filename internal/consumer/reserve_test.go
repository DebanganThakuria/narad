package consumer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReserveSkipsReservedUnexpiredOffset(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1024, 1024)

	for want := int64(0); want < 3; want++ {
		r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
		wantReserved(t, r, err, want)
	}
}

func TestReserveSkipsAckedAhead(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1024, 1024)

	// Reserve 0, 1, 2 and capture nonces.
	nonces := reserveN(t, f, 3)

	// Ack offset 2 out-of-order → goes to ackedAhead.
	mustCommit(t, f, 2, nonces[2])

	// Advance clock past visibility timeout so 0 and 1 expire.
	withClock(f, 1000+60_000)

	// Reserve should pick 0 (expired), then 1 (expired), skip 2 (ackedAhead), pick 3.
	for _, want := range []int64{0, 1, 3} {
		r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 10)
		wantReserved(t, r, err, want)
	}
}

func TestReserveInFlightCap(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(3, 1024)

	reserveN(t, f, 3)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantSkip(t, r, err, "cap")
}

func TestExpiredEntriesDoNotBlockCap(t *testing.T) {
	t.Parallel()
	// maxInFlight = 2. Reserve 2 messages, let them expire without acking.
	// Without the heap, len(entries)=2 still counts toward the cap and
	// new consumers would get "cap" forever. With the heap, the purge in
	// ReserveNext removes them and new consumers get fresh reservations.
	f := newClockedInFlight(2, 1024)

	mustReserve(t, f, testDeepTail) // offset 0
	mustReserve(t, f, testDeepTail) // offset 1

	// Advance clock past visibility timeout — both entries expire.
	withClock(f, 1000+60_000)

	// Without the heap fix this would return SkipReason="cap".
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0) // offset 0 re-reserved
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

func TestReserveNextReturnsEmptyWhenTailAtFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 0)
	wantSkip(t, r, err, "empty")
}

func TestReserveNextReturnsAllReservedWhenOnlyAckedAheadRemains(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 2)
	wantSkip(t, r, err, "all_reserved")
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

func TestReserveNextStartsAtCommittedPlusOneAfterInit(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustInit(t, f, 3)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 4)
}

func TestReserveNextReturnsAllReservedWhenEntriesCoverWindow(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, 2)
	mustReserve(t, f, 2)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 2)
	wantSkip(t, r, err, "all_reserved")
}

func TestReserveNextAfterDropTopicRecreatesShard(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
}

func TestReserveNextAllReservedWithMixedEntriesAndAckedAhead(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 3)
	wantSkip(t, r, err, "all_reserved")
}

func TestReserveNextReturnsDistinctNonces(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r1 := mustReserve(t, f, testDeepTail)
	r2 := mustReserve(t, f, testDeepTail)
	if r1.Nonce == r2.Nonce {
		t.Fatalf("nonces are equal: %d", r1.Nonce)
	}
}

func TestReserveNextAfterInitHonorsCap(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1, 10)
	mustInit(t, f, 5)
	mustReserve(t, f, testDeepTail)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantSkip(t, r, err, "cap")
}

func TestReserveNextWithExactTailAfterCommitIsEmpty(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, 1)
	mustCommit(t, f, r.Offset, r.Nonce)

	res, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	wantSkip(t, res, err, "empty")
}

func TestReserveNextDifferentPartitionsStartAtZeroIndependently(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r0 := mustReserveOn(t, f, testTopic, 0)
	r1 := mustReserveOn(t, f, testTopic, 1)
	if r0.Offset != 0 || r1.Offset != 0 {
		t.Fatalf("offsets = (%d, %d), want (0, 0)", r0.Offset, r1.Offset)
	}
}

func TestReserveNextVisibilityTimeoutZeroExpiresImmediatelyOnNextSweep(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, 0, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	f.purgeAll()
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestReserveNextAfterAckedAheadCollapseStartsAtNewFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 3)
}

func TestReserveNextUsesSeparateShardsPerTopic(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r1 := mustReserveOn(t, f, testTopic, testPart)
	r2 := mustReserveOn(t, f, "other", testPart)
	if r1.Offset != 0 || r2.Offset != 0 {
		t.Fatalf("offsets = (%d, %d), want (0, 0)", r1.Offset, r2.Offset)
	}
}

func TestReserveNextReturnsOffsetZeroOnFreshShard(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
}

func TestReserveNextWithZeroTailOnFreshShardIsEmpty(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 0)
	wantSkip(t, r, err, "empty")
}

func TestReserveNextAfterFrontierAdvanceUsesNextOffset(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)

	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r2, err, 1)
}

func TestReserveNextAfterExpiredCapReleaseUsesLowestOffset(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1, 10)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
}

func TestReserveNextOnOtherTopicUnaffectedByDrop(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserveOn(t, f, "other", testPart)
	f.DropTopic(testTopic)
	wantNext(t, f, "other", testPart, 0)
}

func TestReserveNextReusesExpiredOffsetBeforeHigherOffsets(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
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

func TestReserveNextAfterAckedAheadCollapseStartsAtTailFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 3)
	wantReserved(t, r, err, 2)
}

func TestReserveNextDoesNotNeedInit(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	wantReserved(t, r, err, 0)
}

func TestReserveNextAfterRefreshWithoutShardStillStartsAtZero(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
	withClock(f, 1000)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
}

func TestReserveNextCanReserveAfterCollapse(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 2)
}

func TestReserveNextAfterCommittedOffsetUsesFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	wantNext(t, f, testTopic, testPart, 1)
}

func TestReserveNextZeroVisibilityStillReserves(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, 0, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if !r.Reserved {
		t.Fatalf("ReserveNext() = %+v, want reserved", r)
	}
}

func TestReserveNextAfterPurgeAndCommitStartsAtNewFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r2.Offset, r2.Nonce)

	r3, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r3, err, 1)
}

func TestReserveNextOnFreshTopicAfterOtherTopicStateStartsAtZero(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserveOn(t, f, "other", 0)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
}

func TestReserveNextAfterOtherTopicDropStillUsesCurrentFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic("other")

	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r2, err, 1)
}

func TestReserveNextAfterCommitAndOtherTopicDropStillStartsAtNext(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic("other")

	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r2, err, 1)
}

func TestReserveNextAfterAckedAheadCollapseAndOtherTopicDropStillUsesFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 2)
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	f.DropTopic("other")

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 2)
}

func TestReserveNextAfterNoopRefreshStillWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
	withClock(f, 1000)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	wantReserved(t, r, err, 0)
}

func TestReserveNextAfterInitOutOfOrderCollapseStartsAtCorrectOffset(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustInit(t, f, 4)
	r1 := mustReserve(t, f, testDeepTail)
	r2 := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r2.Offset, r2.Nonce)
	mustCommit(t, f, r1.Offset, r1.Nonce)

	r3, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r3, err, 7)
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
	mustRefresh(t, f)

	r2, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r2, err, 1)
}

func TestReserveNextAfterInitReplacementStartsAtNewFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustInit(t, f, 4)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 5)
}

func TestReserveNextAfterOtherTopicDropStillCountsCurrentEntries(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1, 10)
	mustReserve(t, f, testDeepTail)
	f.DropTopic("other")

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantSkip(t, r, err, "cap")
}

func TestReserveNextDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r1 := mustReserveOn(t, f, testTopic, 1)
	r2 := mustReserveOn(t, f, "other", 2)
	if r1.Offset != 0 || r2.Offset != 0 {
		t.Fatalf("offsets = (%d, %d), want (0, 0)", r1.Offset, r2.Offset)
	}
}

func TestReserveNextAfterDropOfDifferentTopicStillWorks(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic("other")
	withClock(f, 1000)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, 1)
	wantReserved(t, r, err, 0)
}
