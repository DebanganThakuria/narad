package runtime

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// wp8CountSyncs counts data syncs per file name while fn runs.
func wp8CountSyncs(t *testing.T, fn func()) map[string]int {
	t.Helper()
	var mu sync.Mutex
	counts := make(map[string]int)
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSyncData {
			mu.Lock()
			counts[filepath.Base(path)]++
			mu.Unlock()
		}
		return nil
	})
	defer restore()
	fn()
	return counts
}

// TestWP8CommitterOneSyncPerPartitionWhenAheadCarriesTheFrontier pins
// the fsync economy: a flush that writes consumer.ahead (whose record
// carries the frontier, and recovery takes the larger of both files)
// does not also sync consumer.offset, and a flush with an unchanged
// acked-ahead set writes consumer.offset alone.
func TestWP8CommitterOneSyncPerPartitionWhenAheadCarriesTheFrontier(t *testing.T) {
	dataDir := t.TempDir()
	dir := mustCreatePartitionDir(t, dataDir, "t", 0)
	c := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	src := &fakeAheadSource{committed: 4, offsets: []int64{6, 9}, version: 1}
	c.SetAheadSource(src.source)

	c.Commit("t", 0, 4)
	counts := wp8CountSyncs(t, func() {
		if err := c.flush(); err != nil {
			t.Fatal(err)
		}
	})
	if counts["consumer.ahead"] != 1 || counts["consumer.offset"] != 0 {
		t.Fatalf("flush with a changed acked-ahead set synced %v, want consumer.ahead once and consumer.offset never", counts)
	}

	// Frontier moves, set unchanged: only consumer.offset is written.
	src.committed = 5
	c.Commit("t", 0, 5)
	counts = wp8CountSyncs(t, func() {
		if err := c.flush(); err != nil {
			t.Fatal(err)
		}
	})
	if counts["consumer.ahead"] != 0 || counts["consumer.offset"] != 1 {
		t.Fatalf("flush with an unchanged set synced %v, want consumer.offset once", counts)
	}
	if got, _, _ := storage.ReadConsumerOffset(dir); got != 5 {
		t.Fatalf("consumer.offset = %d, want 5", got)
	}

	// The set changes again with the frontier: consumer.ahead carries
	// the new frontier and consumer.offset stays behind until Close.
	src.committed, src.offsets, src.version = 9, []int64{11}, 2
	c.Commit("t", 0, 9)
	counts = wp8CountSyncs(t, func() {
		if err := c.flush(); err != nil {
			t.Fatal(err)
		}
	})
	if counts["consumer.offset"] != 0 {
		t.Fatalf("flush synced consumer.offset %d times although consumer.ahead carried the frontier", counts["consumer.offset"])
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
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := storage.ReadConsumerOffset(dir); got != 9 {
		t.Fatalf("consumer.offset after Close = %d, want 9", got)
	}
}

// TestWP8CommitterWritesBothWhenAheadLagsTheCommit covers the one case
// where consumer.ahead cannot stand in for consumer.offset: the source
// reports a frontier below the queued commit (a commit from a dropped
// shard that reached the committer after the drop). Both files are
// written, as before, so nothing a commit carried is lost.
func TestWP8CommitterWritesBothWhenAheadLagsTheCommit(t *testing.T) {
	dataDir := t.TempDir()
	dir := mustCreatePartitionDir(t, dataDir, "t", 0)
	c := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	src := &fakeAheadSource{committed: 3, offsets: []int64{5}, version: 1}
	c.SetAheadSource(src.source)
	c.Commit("t", 0, 7)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := storage.ReadConsumerOffset(dir); got != 7 {
		t.Fatalf("consumer.offset = %d, want 7", got)
	}
	if rec, ok, _ := storage.ReadConsumerAhead(dir); !ok || rec.Committed != 3 {
		t.Fatalf("consumer.ahead = %+v ok %v, want committed 3", rec, ok)
	}
	_ = c.Close()
}
