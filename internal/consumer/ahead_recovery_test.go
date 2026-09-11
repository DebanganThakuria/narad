package consumer

import (
	"context"
	"slices"
	"testing"
)

// TestAheadRecoverySeedsAndCollapses pins the restart story for
// out-of-order acks: the persisted set is installed above the recovered
// frontier, entries at or below it are ignored, a run right above the
// frontier collapses into it, and the advance is reported through
// onCommit so the frontier file catches up.
func TestAheadRecoverySeedsAndCollapses(t *testing.T) {
	t.Parallel()
	var committed []int64
	f := NewInFlight(fixedCaps(1024, 1024), func(_ string, _ int, off int64) { committed = append(committed, off) })
	withClock(f, 1000)
	f.SetCommittedRecovery(func(string, int) (int64, bool) { return 3, true })
	// The ahead record was written against a later frontier (the crash
	// landed between the two file writes of one flush): its frontier
	// wins, 4 and 5 collapse into it, 8 stays acked ahead.
	f.SetAheadRecovery(func(string, int) (int64, []int64, bool) { return 4, []int64{2, 4, 5, 8}, true })

	next, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext() error = %v", err)
	}
	if next.Offset != 6 {
		t.Fatalf("first offset after recovery = %d, want 6 (frontier 4, then 5 acked ahead, then the hole)", next.Offset)
	}
	if !slices.Equal(committed, []int64{5}) {
		t.Fatalf("onCommit calls = %v, want [5]: the collapse over the recovered run is persisted", committed)
	}
	c, offsets, _, ok := f.AheadSnapshot(testTopic, testPart)
	if !ok || c != 5 || !slices.Equal(offsets, []int64{8}) {
		t.Fatalf("AheadSnapshot = committed %d offsets %v ok %v, want 5 [8]", c, offsets, ok)
	}
	// 6 is reserved; 7 comes next, and 8 is never handed out again.
	second, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil || second.Offset != 7 {
		t.Fatalf("second reserve = %+v err %v, want offset 7", second, err)
	}
	third, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, testDeepTail)
	if err != nil || third.Offset != 9 {
		t.Fatalf("third reserve = %+v err %v, want offset 9 (8 was acked before the restart)", third, err)
	}
}

// TestAheadSnapshotVersionTracksTheSet pins what the committer relies
// on: the version changes on an out-of-order ack and on a frontier
// advance that drains the set, and stays put otherwise.
func TestAheadSnapshotVersionTracksTheSet(t *testing.T) {
	t.Parallel()
	f := NewInFlight(fixedCaps(1024, 1024), nil)
	withClock(f, 1000)
	nonces := reserveN(t, f, 3)
	_, _, v0, _ := f.AheadSnapshot(testTopic, testPart)

	mustCommit(t, f, 2, nonces[2]) // out of order: set grows
	c, offsets, v1, _ := f.AheadSnapshot(testTopic, testPart)
	if c != -1 || !slices.Equal(offsets, []int64{2}) || v1 == v0 {
		t.Fatalf("after ack 2: committed %d offsets %v version %d->%d", c, offsets, v0, v1)
	}
	mustCommit(t, f, 0, nonces[0]) // in order: frontier 0, set unchanged
	_, _, v2, _ := f.AheadSnapshot(testTopic, testPart)
	if v2 != v1 {
		t.Fatalf("an in-order ack that does not touch the set changed the version %d->%d", v1, v2)
	}
	mustCommit(t, f, 1, nonces[1]) // frontier walks over 2: set drains
	c, offsets, v3, _ := f.AheadSnapshot(testTopic, testPart)
	if c != 2 || len(offsets) != 0 || v3 == v2 {
		t.Fatalf("after the drain: committed %d offsets %v version %d->%d", c, offsets, v2, v3)
	}
}

func TestDropPartitionNotifies(t *testing.T) {
	t.Parallel()
	f := NewInFlight(fixedCaps(8, 8), nil)
	withClock(f, 1000)
	var dropped []string
	f.SetDropNotifier(func(topic string, partition int) { dropped = append(dropped, topic) })
	reserveN(t, f, 1)
	f.DropPartition(testTopic, testPart)
	if !slices.Equal(dropped, []string{testTopic}) {
		t.Fatalf("drop notifier calls = %v, want [%s]", dropped, testTopic)
	}
}

func TestDropTopicNotifiesEveryPartition(t *testing.T) {
	t.Parallel()
	f := NewInFlight(fixedCaps(8, 8), nil)
	withClock(f, 1000)
	var dropped []int
	f.SetDropNotifier(func(topic string, partition int) {
		if topic == testTopic {
			dropped = append(dropped, partition)
		}
	})
	mustReserveOn(t, f, testTopic, 0)
	mustReserveOn(t, f, testTopic, 3)
	mustReserveOn(t, f, "other", 0)
	f.DropTopic(testTopic)
	slices.Sort(dropped)
	if !slices.Equal(dropped, []int{0, 3}) {
		t.Fatalf("drop notifier partitions = %v, want [0 3] (and nothing for the other topic)", dropped)
	}
}

// A corrupt skip resolves into the corrupt set, which is not persisted:
// it must neither bump the acked-ahead version nor dirty the committer.
func TestSkipCorruptDoesNotDirtyPersistedSet(t *testing.T) {
	t.Parallel()
	calls := 0
	f := NewInFlight(fixedCaps(8, 8), func(string, int, int64) { calls++ })
	withClock(f, 1000)
	nonces := reserveN(t, f, 2)
	_, _, v0, _ := f.AheadSnapshot(testTopic, testPart)
	if err := f.SkipCorrupt(testTopic, testPart, 1, nonces[1]); err != nil {
		t.Fatalf("SkipCorrupt: %v", err)
	}
	_, _, v1, _ := f.AheadSnapshot(testTopic, testPart)
	if v1 != v0 || calls != 0 {
		t.Fatalf("corrupt skip above the frontier changed the version (%d->%d) or called onCommit %d times", v0, v1, calls)
	}
}
