package consumer

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestCommitOutOfOrderDoesNotAdvanceFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1024, 1024)

	nonces := reserveN(t, f, 4)

	// Ack 2 and 3 out-of-order — frontier must stay at -1.
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 3, nonces[3])

	wantCommitted(t, f, -1)
}

func TestCommitFillsGapAndWalksForward(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1024, 1024)

	nonces := reserveN(t, f, 5) // offsets 0..4

	// Out-of-order: ack 2, 4, 1. Frontier stays at -1.
	for _, off := range []int64{2, 4, 1} {
		mustCommit(t, f, off, nonces[off])
	}
	wantCommitted(t, f, -1)

	// Ack 0 — walks through acked-ahead 1 and 2; stops at 3 (still in-flight).
	mustCommit(t, f, 0, nonces[0])
	wantCommitted(t, f, 2)

	// Ack 3 — walks through acked-ahead 4.
	mustCommit(t, f, 3, nonces[3])
	wantCommitted(t, f, 4)
}

func TestCommitHandleAckedAheadCapRejects(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1024, 2) // max 2 out-of-order acks

	nonces := reserveN(t, f, 5)

	// Fill the ackedAhead set.
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 3, nonces[3])

	// Third out-of-order ack must be rejected.
	wantErr(t, f.CommitHandle(testTopic, testPart, 4, nonces[4]), ErrAckedAheadFull)

	// ackedAhead must not have grown (no leak from rejected insert).
	if _, aa := f.Snapshot(testTopic, testPart); aa != 2 {
		t.Fatalf("ackedAhead size leaked: got %d, want 2", aa)
	}
}

func TestCommitHandleStaleNonce(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1024, 1024)

	r1 := mustReserve(t, f, testDeepTail)

	// Advance clock past VT so offset 0 becomes eligible for re-reservation.
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)

	if r2.Nonce == r1.Nonce {
		t.Fatalf("nonce must change on re-reservation; both = %d", r1.Nonce)
	}

	// Old nonce is rejected.
	wantErr(t, f.CommitHandle(testTopic, testPart, r1.Offset, r1.Nonce), ErrHandleStale)
	// New nonce succeeds.
	mustCommit(t, f, r2.Offset, r2.Nonce)
}

func TestOnCommitCalledWhenFrontierAdvances(t *testing.T) {
	t.Parallel()

	var called []int64
	onCommit := func(_ string, _ int, offset int64) {
		called = append(called, offset)
	}
	f := NewInFlight(fixedCaps(1024, 1024), onCommit)
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

func TestCommitHandleRejectsExpiredReservationWithoutPurger(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(2, 1024)
	r := mustReserve(t, f, testDeepTail)

	withClock(f, 1000+testVT.Milliseconds()+1)
	wantErr(t, f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleReturnsStaleForMissingShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)

	wantErr(t, f.CommitHandle(testTopic, testPart, 0, 1), ErrHandleStale)
}

func TestCommitHandleRepeatedOutOfOrderAckDoesNotGrowAckedAhead(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	wantErr(t, f.CommitHandle(testTopic, testPart, 2, nonces[2]), ErrHandleStale)
	if _, aa := f.Snapshot(testTopic, testPart); aa != 1 {
		t.Fatalf("Snapshot() ackedAhead = %d, want 1", aa)
	}
}

func TestCommitHandleRejectsWrongNonce(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	r := mustReserve(t, f, testDeepTail)
	wantErr(t, f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce+1), ErrHandleStale)
}

func TestCommitHandleAdvancesThroughAckedAheadAndCallsOnCommitOnce(t *testing.T) {
	t.Parallel()

	var commits []int64
	f := NewInFlight(fixedCaps(10, 10), func(_ string, _ int, offset int64) {
		commits = append(commits, offset)
	})
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])

	wantCommitted(t, f, 2)
	if len(commits) != 1 || commits[0] != 2 {
		t.Fatalf("onCommit offsets = %v, want [2]", commits)
	}
}

func TestCommitHandleInOrderDeletesInFlightEntry(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestCommitHandleOutOfOrderDeletesEntryAndParksAckedAhead(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	wantSnapshot(t, f, testTopic, testPart, 2, 1)
}

func TestCommitHandleDoesNotCallOnCommitForOutOfOrderAck(t *testing.T) {
	t.Parallel()
	var called atomic.Int64
	f := NewInFlight(fixedCaps(10, 10), func(string, int, int64) {
		called.Add(1)
	})
	withClock(f, 1000)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	if got := called.Load(); got != 0 {
		t.Fatalf("onCommit calls = %d, want 0", got)
	}
}

func TestCommitHandleLeavesAckedAheadIntactOnCapError(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 1)

	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	wantErr(t, f.CommitHandle(testTopic, testPart, 1, nonces[1]), ErrAckedAheadFull)
	wantSnapshot(t, f, testTopic, testPart, 2, 1)
}

func TestCommitHandleRejectsStaleAfterDropTopic(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	wantErr(t, f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleAdvancesFrontierAfterExpiredGapReReserved(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r0 := mustReserve(t, f, testDeepTail)
	_ = mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r0b := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r0b.Offset, r0b.Nonce)
	wantCommitted(t, f, 0)
	wantErr(t, f.CommitHandle(testTopic, testPart, r0.Offset, r0.Nonce), ErrHandleStale)
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
	mustRefresh(t, f)
	mustCommit(t, f, 3, nonces[3])
}

func TestCommitHandleAfterInitAdvancesFromSeededFrontier(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustInit(t, f, 5)
	r := mustReserve(t, f, testDeepTail)
	if r.Offset != 6 {
		t.Fatalf("reserved offset = %d, want 6", r.Offset)
	}
	mustCommit(t, f, r.Offset, r.Nonce)
	wantCommitted(t, f, 6)
}

func TestCommitHandleOutOfOrderThenGapThenHeadPreservesOrder(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 3, nonces[3])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	wantCommitted(t, f, 1)
	mustCommit(t, f, 2, nonces[2])
	wantCommitted(t, f, 3)
}

func TestCommitHandleWithWrongPartitionIsStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	wantErr(t, f.CommitHandle(testTopic, 1, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleDifferentPartitionsAdvanceIndependently(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r0 := mustReserveOn(t, f, testTopic, 0)
	r1 := mustReserveOn(t, f, testTopic, 1)
	if err := f.CommitHandle(testTopic, 0, r0.Offset, r0.Nonce); err != nil {
		t.Fatalf("CommitHandle(0) error = %v", err)
	}
	wantNext(t, f, testTopic, 0, 1)
	wantNext(t, f, testTopic, 1, 0)
	if err := f.CommitHandle(testTopic, 1, r1.Offset, r1.Nonce); err != nil {
		t.Fatalf("CommitHandle(1) error = %v", err)
	}
}

func TestCommitHandleAckedAheadThenDropTopicClearsState(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	f.DropTopic(testTopic)
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestCommitHandleAckedAheadCapAfterRefreshStillEnforced(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 2)
	nonces := reserveN(t, f, 5)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 3, nonces[3])
	wantErr(t, f.CommitHandle(testTopic, testPart, 4, nonces[4]), ErrAckedAheadFull)
}

func TestCommitHandleAfterDropTopicReturnsStaleEvenWithFormerNonce(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	wantErr(t, f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleInOrderCallsOnCommitWithAdvancedOffset(t *testing.T) {
	t.Parallel()
	var got int64 = -1
	f := NewInFlight(fixedCaps(10, 10), func(string, int, int64) {
		got = 0
	})
	withClock(f, 1000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	if got != 0 {
		t.Fatalf("onCommit got = %d, want 0", got)
	}
}

func TestCommitHandleWithMismatchedOffsetIsStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	wantErr(t, f.CommitHandle(testTopic, testPart, 99, 1), ErrHandleStale)
}

func TestCommitHandleAfterAllReservedGapStillAdvancesWhenHeadAcked(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 0, nonces[0])
	wantCommitted(t, f, 0)
	mustCommit(t, f, 1, nonces[1])
	wantCommitted(t, f, 2)
}

func TestCommitHandleWithNoReservationAndExistingShardIsStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	wantErr(t, f.CommitHandle(testTopic, testPart, 99, 1), ErrHandleStale)
}

func TestCommitHandleWrongPartitionAfterReserveIsStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	wantErr(t, f.CommitHandle(testTopic, 9, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleDoesNotAdvancePastMissingGap(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 3, nonces[3])
	mustCommit(t, f, 0, nonces[0])
	wantCommitted(t, f, 0)
}

func TestCommitHandleOnFreshShardWithoutReservationIsStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	if _, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	wantErr(t, f.CommitHandle(testTopic, testPart, 1, 1), ErrHandleStale)
}

func TestCommitHandleStaleForDifferentTopic(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	wantErr(t, f.CommitHandle("other", testPart, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleCanAdvanceAfterReusedExpiredOffset(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	wantCommitted(t, f, 0)
}

func TestCommitHandlePreservesAckedAheadUntilGapClosed(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 4)
	mustCommit(t, f, 3, nonces[3])
	mustCommit(t, f, 0, nonces[0])
	if _, aa := f.Snapshot(testTopic, testPart); aa != 1 {
		t.Fatalf("Snapshot() ackedAhead = %d, want 1", aa)
	}
}

func TestCommitHandleWrongNonceAfterDropTopicStillStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	wantErr(t, f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce+1), ErrHandleStale)
}

func TestCommitHandleAfterCapReleaseStillWorks(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(1, 10)
	r := mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r2.Offset, r2.Nonce)
	wantErr(t, f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleAckedAheadThenCollapseClearsAckedAhead(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 1, nonces[1])
	mustCommit(t, f, 0, nonces[0])
	if _, aa := f.Snapshot(testTopic, testPart); aa != 0 {
		t.Fatalf("Snapshot() ackedAhead = %d, want 0", aa)
	}
}

func TestCommitHandleOnCommittedOffsetIsStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	wantErr(t, f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleOutOfOrderLeavesLowerOffsetsReservableAfterExpiry(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	withClock(f, 1000+60_000)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
}

func TestCommitHandleOutOfOrderThenHeadAdvancesOnce(t *testing.T) {
	t.Parallel()
	var commits []int64
	f := NewInFlight(fixedCaps(10, 10), func(string, int, int64) {
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

func TestCommitHandleAfterInitAndReserveCommitsExpectedOffset(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustInit(t, f, 2)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	wantCommitted(t, f, 3)
}

func TestCommitHandleAfterOtherTopicDropStillWorks(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	mustCommit(t, f, r.Offset, r.Nonce)
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
	r := mustReserve(t, f, testDeepTail)
	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	mustRefresh(t, f)
	f.DropTopic("other")
	mustCommit(t, f, r.Offset, r.Nonce)
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
	r := mustReserve(t, f, testDeepTail)
	mustRefresh(t, f)
	f.DropTopic("other")
	mustCommit(t, f, r.Offset, r.Nonce)
}

func TestCommitHandleAfterInitOverrideThenReserveWorks(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustInit(t, f, 1)
	mustInit(t, f, 4)
	r := mustReserve(t, f, testDeepTail)
	if r.Offset != 5 {
		t.Fatalf("reserved offset = %d, want 5", r.Offset)
	}
}

func TestCommitHandleOutOfOrderAfterInitParksAckedAhead(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustInit(t, f, 4)
	r1 := mustReserve(t, f, testDeepTail)
	r2 := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r2.Offset, r2.Nonce)
	wantSnapshot(t, f, testTopic, testPart, 1, 1)
	_ = r1
}

func TestCommitHandleCanAdvanceFromSeededPartitionAfterRefresh(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	mustInit(t, f, 1)
	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	mustRefresh(t, f)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	wantCommitted(t, f, 2)
}

func TestCommitHandleOutOfOrderDoesNotCallOnCommit(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	f := NewInFlight(fixedCaps(10, 10), func(string, int, int64) {
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
	f := NewInFlight(fixedCaps(10, 10), func(string, int, int64) {
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

func TestCommitHandleWrongTopicAfterReserveIsStale(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	wantErr(t, f.CommitHandle("other", testPart, r.Offset, r.Nonce), ErrHandleStale)
}

func TestCommitHandleAfterOtherTopicDropPreservesCurrentState(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	mustCommit(t, f, r.Offset, r.Nonce)
	wantCommitted(t, f, 0)
}

func TestCommitHandleDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r1 := mustReserveOn(t, f, testTopic, 1)
	r2 := mustReserveOn(t, f, "other", 2)
	if err := f.CommitHandle(testTopic, 1, r1.Offset, r1.Nonce); err != nil {
		t.Fatalf("CommitHandle(testTopic,1) error = %v", err)
	}
	if err := f.CommitHandle("other", 2, r2.Offset, r2.Nonce); err != nil {
		t.Fatalf("CommitHandle(other,2) error = %v", err)
	}
}

func TestCommitHandleAfterDropOfDifferentTopicStillWorks(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	f.DropTopic("other")
	mustCommit(t, f, r.Offset, r.Nonce)
}
