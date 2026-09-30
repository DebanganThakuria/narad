package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// wp8CountSyncs counts c's writeouts per file name while fn runs.
func wp8CountSyncs(t *testing.T, c *ConsumerOffsetCommitter, fn func()) map[string]int {
	t.Helper()
	counts := make(map[string]int)
	prev := c.io.writeOut
	c.io.writeOut = func(f *os.File) error {
		counts[filepath.Base(f.Name())]++
		return prev(f)
	}
	defer func() { c.io.writeOut = prev }()
	fn()
	return counts
}

// TestWP8CommitterOneSyncPerPartitionWhenAheadCarriesTheFrontier pins
// the sync economy: once a partition is primed, a durability point
// writes out its consumer.ahead once, whether the acked-ahead set or
// only the frontier changed (the record carries the frontier, and
// recovery takes the larger of both files), and consumer.offset is
// written only when it is levelled: at a partition's first writeout,
// at most every 30s after, and at Close.
func TestWP8CommitterOneSyncPerPartitionWhenAheadCarriesTheFrontier(t *testing.T) {
	dataDir := t.TempDir()
	dir := mustCreatePartitionDir(t, dataDir, "t", 0)
	c := zzWP23ManualCommitter(dataDir)
	src := &fakeAheadSource{committed: 4, offsets: []int64{6, 9}, version: 1}
	c.SetAheadSource(src.source)

	// First touch: the prime writes the file out, then the window.
	c.Commit("t", 0, 4)
	counts := wp8CountSyncs(t, c, func() {
		if err := c.flush(); err != nil {
			t.Fatal(err)
		}
	})
	if counts["consumer.ahead"] != 2 || counts["consumer.offset"] != 1 {
		t.Fatalf("first flush wrote out %v, want consumer.ahead twice (prime, window) and consumer.offset once (first level)", counts)
	}

	// The set changes: one writeout, consumer.offset left behind.
	src.committed, src.offsets, src.version = 5, []int64{7}, 2
	c.Commit("t", 0, 5)
	counts = wp8CountSyncs(t, c, func() {
		if err := c.flush(); err != nil {
			t.Fatal(err)
		}
	})
	if counts["consumer.ahead"] != 1 || counts["consumer.offset"] != 0 {
		t.Fatalf("flush with a changed acked-ahead set wrote out %v, want consumer.ahead once and consumer.offset never", counts)
	}

	// Only the frontier moves: still consumer.ahead alone.
	src.committed, src.offsets, src.version = 9, []int64{11}, 2
	c.Commit("t", 0, 9)
	counts = wp8CountSyncs(t, c, func() {
		if err := c.flush(); err != nil {
			t.Fatal(err)
		}
	})
	if counts["consumer.ahead"] != 1 || counts["consumer.offset"] != 0 {
		t.Fatalf("flush with only the frontier moved wrote out %v, want consumer.ahead once", counts)
	}
	rec, ok, err := storage.ReadConsumerAhead(dir)
	if err != nil || !ok || rec.Committed != 9 {
		t.Fatalf("consumer.ahead = %+v ok %v err %v, want committed 9", rec, ok, err)
	}
	if next, err := wp8RecoveredInFlight(dataDir).ReserveNext(context.Background(), "t", 0, time.Minute, 100); err != nil || next.Offset != 10 {
		t.Fatalf("recovered reserve = %+v err %v, want offset 10 (frontier 9, 11 acked ahead)", next, err)
	}

	// A graceful Close brings consumer.offset up to the frontier, so a
	// reader of that file alone sees where consumers got to.
	counts = wp8CountSyncs(t, c, func() {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	})
	if counts["consumer.ahead"] != 0 || counts["consumer.offset"] != 1 {
		t.Fatalf("Close wrote out %v, want consumer.offset once (the level) and a clean consumer.ahead never", counts)
	}
	if got, _, _ := storage.ReadConsumerOffset(dir); got != 9 {
		t.Fatalf("consumer.offset after Close = %d, want 9", got)
	}
}

// TestWP8CommitterWritesTheShardsSnapshotNotALateCommit pins that the
// source's snapshot is what a tick persists. A commit above the live
// shard's frontier can only come from a shard dropped before it (acks
// call Commit after releasing the shard lock), and the directory may
// hold another lineage's state by now: writing its offset, as the
// committer once did through consumer.offset, would skip offsets that
// lineage never acked.
func TestWP8CommitterWritesTheShardsSnapshotNotALateCommit(t *testing.T) {
	dataDir := t.TempDir()
	dir := mustCreatePartitionDir(t, dataDir, "t", 0)
	c := zzWP23ManualCommitter(dataDir)
	src := &fakeAheadSource{committed: 3, offsets: []int64{5}, version: 1}
	c.SetAheadSource(src.source)
	c.Commit("t", 0, 7)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := storage.ReadConsumerOffset(dir); got != 3 {
		t.Fatalf("consumer.offset = %d, want the shard's 3", got)
	}
	if rec, ok, _ := storage.ReadConsumerAhead(dir); !ok || rec.Committed != 3 || len(rec.Offsets) != 1 || rec.Offsets[0] != 5 {
		t.Fatalf("consumer.ahead = %+v ok %v, want committed 3 with 5 acked ahead", rec, ok)
	}
}
