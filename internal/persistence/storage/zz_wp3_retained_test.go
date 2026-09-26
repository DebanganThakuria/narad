package storage

import (
	"path/filepath"
	"runtime"
	"testing"
)

// wp3HeapPerLog opens n logs, runs commit on each, and returns the live
// heap each open log holds afterwards.
func wp3HeapPerLog(t *testing.T, n int, commit func(*Log)) float64 {
	t.Helper()
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	logs := make([]*Log, 0, n)
	for range n {
		l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{})
		if err != nil {
			t.Fatal(err)
		}
		commit(l)
		logs = append(logs, l)
	}
	// Twice: a sync.Pool keeps what was put back for one extra cycle.
	runtime.GC()
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	for _, l := range logs {
		_ = l.Close()
	}
	return (float64(after.HeapAlloc) - float64(before.HeapAlloc)) / float64(n)
}

// A log that committed keeps no flush buffers of its own: before, every
// Log pinned a 64 KiB read-back buffer after its first commit, however
// small its frames.
func TestWP3CommittedLogRetainsNoVerifyBuffer(t *testing.T) {
	if wp3RaceEnabled {
		t.Skip("heap accounting differs under the race detector")
	}
	wp3NoFsync(t)
	perLog := wp3HeapPerLog(t, 100, func(l *Log) {
		wp3CommitBatch(t, l, wp3Records(7, 190))
	})
	t.Logf("heap per committed log: %.1f KiB", perLog/1024)
	if perLog > 32<<10 {
		t.Fatalf("each committed log holds %.1f KiB of heap, want under 32 KiB", perLog/1024)
	}
}

// One large batch does not leave its encode buffers pinned on the log:
// before, a log kept buffers the size of the largest frame it ever
// encoded (up to 2 x 4 MiB) for its lifetime.
func TestWP3LargeBatchLeavesNoEncodeBuffers(t *testing.T) {
	if wp3RaceEnabled {
		t.Skip("heap accounting differs under the race detector")
	}
	wp3NoFsync(t)
	perLog := wp3HeapPerLog(t, 16, func(l *Log) {
		wp3CommitBatch(t, l, wp3Records(1024, 1000)) // a 1 MiB frame
		for range 3 {
			wp3CommitBatch(t, l, wp3Records(7, 190))
		}
	})
	t.Logf("heap per log after a 1 MiB batch: %.1f KiB", perLog/1024)
	if perLog > 256<<10 {
		t.Fatalf("each log holds %.1f KiB of heap after one large batch, want under 256 KiB", perLog/1024)
	}
}
