package consumer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestNewPartitionShardDoesNotPreallocateMaxInFlight(t *testing.T) {
	t.Parallel()

	sh := newPartitionShard(-1, Caps{MaxInFlight: 65_536, MaxAckedAhead: 65_536})
	if got, want := cap(sh.expiry), initialExpiryHeapCap; got != want {
		t.Fatalf("expiry cap = %d, want %d", got, want)
	}

	sh = newPartitionShard(-1, Caps{MaxInFlight: 8, MaxAckedAhead: 8})
	if got, want := cap(sh.expiry), 8; got != want {
		t.Fatalf("small expiry cap = %d, want %d", got, want)
	}
}

func TestInitSeedsCommittedOffset(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)

	mustInit(t, f, 5)
	wantNext(t, f, testTopic, testPart, 6)
}

func TestInitRejectsInvalidCaps(t *testing.T) {
	t.Parallel()
	f := NewInFlight(fixedCaps(0, 1), nil)

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

	mustReserve(t, f, testDeepTail)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantSkip(t, r, err, "cap")

	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 3}
	mustRefresh(t, f)
	r, err = f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 1)
}

func TestRefreshCapsRejectsInvalidCaps(t *testing.T) {
	t.Parallel()
	f := NewInFlight(fixedCaps(0, 1), nil)

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

	_, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantErr(t, err, resolveErr)
}

func TestNextReturnsZeroForMissingShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	wantNext(t, f, "missing", 3, 0)
}

func TestSnapshotReturnsZeroForMissingShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	wantSnapshot(t, f, "missing", 3, 0, 0)
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

	mustReserveOn(t, f, testTopic, testPart)
	mustReserveOn(t, f, "other", testPart)

	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	mustRefresh(t, f)

	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 1)
	r, err = f.ReserveNext(context.Background(), "other", testPart, testVT, testDeepTail)
	wantSkip(t, r, err, "cap")
}

func TestGetOrCreateCreatesShardOncePerPartition(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		calls.Add(1)
		return Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	withClock(f, 1000)

	mustReserveOn(t, f, testTopic, testPart)
	mustReserveOn(t, f, testTopic, testPart)
	if got := calls.Load(); got != 1 {
		t.Fatalf("resolver calls = %d, want 1", got)
	}
}

func TestRefreshCapsPropagatesResolverError(t *testing.T) {
	t.Parallel()
	resolveErr := errors.New("boom")
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{}, resolveErr
	}, nil)

	wantErr(t, f.RefreshCaps(context.Background(), testTopic), resolveErr)
}

func TestInitPropagatesResolverError(t *testing.T) {
	t.Parallel()
	resolveErr := errors.New("boom")
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return Caps{}, resolveErr
	}, nil)

	wantErr(t, f.Init(context.Background(), testTopic, testPart, 0), resolveErr)
}

func TestDropTopicDoesNothingForMissingTopic(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic("missing")
}

func TestNextReflectsFrontierAfterDropTopic(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic(testTopic)
	wantNext(t, f, testTopic, testPart, 0)
}

func TestRefreshCapsWithoutExistingShardStillSucceeds(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
}

func TestInitOverridesExistingShard(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustInit(t, f, 9)
	wantNext(t, f, testTopic, testPart, 10)
}

func TestInitSetsSnapshotToZeroSizes(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustInit(t, f, 5)
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestRefreshCapsAfterDropTopicNoops(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic(testTopic)
	mustRefresh(t, f)
}

func TestSnapshotAfterDropTopicReturnsZero(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	f.DropTopic(testTopic)
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
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
	mustReserveOn(t, f, testTopic, 0)
	mustReserveOn(t, f, "other", 0)
	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	mustRefresh(t, f)
	wantNext(t, f, "other", 0, 0)
}

func TestInitAfterDropTopicRecreatesSeededShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic(testTopic)
	mustInit(t, f, 2)
	wantNext(t, f, testTopic, testPart, 3)
}

func TestRefreshCapsWithNoMatchingShardStillCallsResolver(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		calls.Add(1)
		return Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	mustRefresh(t, f)
	if got := calls.Load(); got != 1 {
		t.Fatalf("resolver calls = %d, want 1", got)
	}
}

func TestDropTopicClearsMultiplePartitionsForSameTopic(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserveOn(t, f, testTopic, 1)
	f.DropTopic(testTopic)
	wantNext(t, f, testTopic, 0, 0)
	wantNext(t, f, testTopic, 1, 0)
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
	mustRefresh(t, f)
	mustCommit(t, f, 3, nonces[3])
}

func TestSnapshotCountsAfterOutOfOrderAndCommitCollapse(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	nonces := reserveN(t, f, 3)
	mustCommit(t, f, 2, nonces[2])
	mustCommit(t, f, 0, nonces[0])
	wantSnapshot(t, f, testTopic, testPart, 1, 1)
	mustCommit(t, f, 1, nonces[1])
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestRefreshCapsAfterInitAffectsSeededShard(t *testing.T) {
	t.Parallel()
	caps := Caps{MaxInFlight: 1, MaxAckedAhead: 1}
	f := NewInFlight(func(context.Context, string) (Caps, error) {
		return caps, nil
	}, nil)
	withClock(f, 1000)
	mustInit(t, f, 0)
	mustReserve(t, f, testDeepTail)
	caps = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	mustRefresh(t, f)
	if r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail); err != nil || !r.Reserved {
		t.Fatalf("ReserveNext() = %+v, err=%v, want reserved", r, err)
	}
}

func TestDropTopicClearsSeededShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustInit(t, f, 3)
	f.DropTopic(testTopic)
	wantNext(t, f, testTopic, testPart, 0)
}

func TestSnapshotTracksOnlyCurrentPartition(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserveOn(t, f, testTopic, 1)
	wantSnapshot(t, f, testTopic, testPart, 1, 0)
}

func TestRefreshCapsInvalidAckedAheadCapRejected(t *testing.T) {
	t.Parallel()
	f := NewInFlight(fixedCaps(1, 0), nil)
	if err := f.RefreshCaps(context.Background(), testTopic); err == nil {
		t.Fatal("RefreshCaps() error = nil, want error")
	}
}

func TestInitInvalidAckedAheadCapRejected(t *testing.T) {
	t.Parallel()
	f := NewInFlight(fixedCaps(1, 0), nil)
	if err := f.Init(context.Background(), testTopic, testPart, 0); err == nil {
		t.Fatal("Init() error = nil, want error")
	}
}

func TestDropTopicLeavesDifferentTopicUntouched(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserveOn(t, f, "other", testPart)
	f.DropTopic(testTopic)
	wantNext(t, f, "other", testPart, 0)
}

func TestRefreshCapsCanRunBeforeShardCreation(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
}

func TestInitCanBeCalledTwice(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustInit(t, f, 1)
	mustInit(t, f, 2)
	wantNext(t, f, testTopic, testPart, 3)
}

func TestRefreshCapsResolverCanSeeTopic(t *testing.T) {
	t.Parallel()
	var seen string
	f := NewInFlight(func(_ context.Context, topic string) (Caps, error) {
		seen = topic
		return Caps{MaxInFlight: 1, MaxAckedAhead: 1}, nil
	}, nil)
	mustRefresh(t, f)
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
	mustInit(t, f, 0)
	if seen != testTopic {
		t.Fatalf("resolver saw topic %q, want %q", seen, testTopic)
	}
}

func TestInitThenDropThenReserveStartsFresh(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustInit(t, f, 5)
	f.DropTopic(testTopic)
	withClock(f, 1000)
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	wantReserved(t, r, err, 0)
}

func TestSnapshotAfterInitOverrideIsReset(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustInit(t, f, 9)
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestRefreshCapsDoesNotCreateShard(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
	wantNext(t, f, testTopic, testPart, 0)
}

func TestDropTopicAfterRefreshStillSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
	f.DropTopic(testTopic)
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
	wantNext(t, f, testTopic, 0, 2)
	wantNext(t, f, testTopic, 1, 6)
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
	mustRefresh(t, f)
	wantNext(t, f, testTopic, testPart, 0)
	mustCommit(t, f, r.Offset, r.Nonce)
}

func TestNextAfterInitOverrideReflectsLatestCommit(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustInit(t, f, 1)
	mustInit(t, f, 4)
	wantNext(t, f, testTopic, testPart, 5)
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
	mustRefresh(t, f)
	wantSnapshot(t, f, testTopic, testPart, 1, 0)
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
	mustReserveOn(t, f, "other", 0)
	caps[testTopic] = Caps{MaxInFlight: 2, MaxAckedAhead: 2}
	mustRefresh(t, f)
}

func TestDropTopicAfterInitOverrideClearsLatestState(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustInit(t, f, 1)
	mustInit(t, f, 5)
	f.DropTopic(testTopic)
	wantNext(t, f, testTopic, testPart, 0)
}

func TestSnapshotAfterOtherTopicDropUnchangedForCurrentTopic(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserveOn(t, f, "other", 0)
	f.DropTopic("other")
	wantSnapshot(t, f, testTopic, testPart, 1, 0)
}

func TestSnapshotAfterCommitAndOtherTopicDropStillZero(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	r := mustReserve(t, f, testDeepTail)
	mustCommit(t, f, r.Offset, r.Nonce)
	f.DropTopic("other")
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestRefreshCapsThenInitStillUsesInitCommit(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
	mustInit(t, f, 2)
	wantNext(t, f, testTopic, testPart, 3)
}

func TestDropTopicAfterRefreshAndInitClearsState(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	mustRefresh(t, f)
	mustInit(t, f, 2)
	f.DropTopic(testTopic)
	wantNext(t, f, testTopic, testPart, 0)
}

func TestInitAfterExistingShardReplacesEntries(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustInit(t, f, 4)
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestSnapshotForDifferentTopicIsIndependent(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserveOn(t, f, "other", testPart)
	wantSnapshot(t, f, "other", testPart, 1, 0)
}

func TestDropTopicCanRemoveOtherTopicOnly(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserveOn(t, f, "other", testPart)
	f.DropTopic("other")
	wantNext(t, f, testTopic, testPart, 0)
	wantNext(t, f, "other", testPart, 0)
}

func TestSnapshotDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserveOn(t, f, testTopic, 1)
	wantSnapshot(t, f, testTopic, 1, 1, 0)
}

func TestDropTopicDifferentTopicDifferentPartitionIndependent(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserveOn(t, f, "other", 2)
	f.DropTopic(testTopic)
	wantNext(t, f, "other", 2, 0)
}
