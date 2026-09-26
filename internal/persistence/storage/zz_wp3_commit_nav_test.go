package storage

import (
	"path/filepath"
	"sync/atomic"
	"testing"
)

// The commit read-back finds each frame it just wrote from the position
// writeFrame recorded, without walking frame headers in the file: the
// sparse index keeps one anchor per 32 KiB, so before, almost every
// commit read the previous frame's header to step onto its own.
func TestWP3CommitReadBackReadsNoFrameHeaders(t *testing.T) {
	var hdr atomic.Int64
	frameHeaderReadHook = func() { hdr.Add(1) }
	t.Cleanup(func() { frameHeaderReadHook = nil })

	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const commits = 50
	for range commits {
		wp3CommitBatch(t, l, wp3Records(7, 190))
	}
	if got := hdr.Load(); got != 0 {
		t.Fatalf("%d frame header reads over %d commits, want 0", got, commits)
	}

	// Split batches (several frames per commit) resolve from the cache too.
	old := maxFrameInnerBytes
	maxFrameInnerBytes = 600
	t.Cleanup(func() { maxFrameInnerBytes = old })
	for range 5 {
		wp3CommitBatch(t, l, wp3Records(9, 190))
	}
	if got := hdr.Load(); got != 0 {
		t.Fatalf("%d frame header reads verifying split batches, want 0", got)
	}
	for off := range l.HighWatermark() {
		if _, err := l.Read(off); err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
	}
}
