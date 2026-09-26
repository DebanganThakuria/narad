package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// wp3FrameCached reports whether the frame holding offset is in the
// decoded-frame cache, without reading it (a read would cache it).
func wp3FrameCached(t *testing.T, l *Log, offset int64) bool {
	t.Helper()
	entry, _, unlock, ok, err := l.indexEntryForRead(offset)
	if err != nil || !ok {
		t.Fatalf("indexEntryForRead(%d): ok=%v err=%v", offset, ok, err)
	}
	unlock()
	_, cached := l.frameCache.get(frameKey{segmentBase: entry.segmentBaseOffset, framePos: entry.framePos})
	return cached
}

// A frame written while the partition has readers goes straight into the
// decoded-frame cache, so the consumers woken by its commit read it from
// memory instead of each re-reading and re-decoding it from the file.
func TestWP3WrittenFrameCachedWhenLogHasReaders(t *testing.T) {
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// No reader yet: the frame is not cached.
	a, _ := wp3CommitBatch(t, l, wp3Records(7, 190))
	if wp3FrameCached(t, l, a) {
		t.Fatal("frame cached although nothing reads the log")
	}

	// A consumer reads; the next frame is cached as written.
	if _, err := l.ReadShared(a); err != nil {
		t.Fatal(err)
	}
	want := wp3Records(7, 190)
	for i := range want {
		want[i][0] = 'B'
	}
	written := make([][]byte, len(want))
	for i, r := range want {
		written[i] = append([]byte(nil), r...)
	}
	b, lastB := wp3CommitBatch(t, l, written)
	if !wp3FrameCached(t, l, b) {
		t.Fatal("frame written after a read is not in the frame cache")
	}
	for off := b; off <= lastB; off++ {
		got, err := l.ReadShared(off)
		if err != nil {
			t.Fatalf("ReadShared(%d): %v", off, err)
		}
		if !bytes.Equal(got, want[off-b]) {
			t.Fatalf("ReadShared(%d) = %q..., want %q...", off, got[:8], want[off-b][:8])
		}
	}

	// The reads above count as a reader for the next batch only: a batch
	// written with no read since is not cached.
	wp3CommitBatch(t, l, wp3Records(7, 190))
	c, _ := wp3CommitBatch(t, l, wp3Records(7, 190))
	if wp3FrameCached(t, l, c) {
		t.Fatal("frame cached although nothing read the log since the previous batch")
	}
}

// Without the commit read-back nothing is cached as written: a record
// served from memory must be one whose on-disk CRC a commit verified.
func TestWP3WrittenFrameNotCachedWithoutCommitVerify(t *testing.T) {
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{DisableCommitVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, _ := wp3CommitBatch(t, l, wp3Records(7, 190))
	if _, err := l.ReadShared(a); err != nil {
		t.Fatal(err)
	}
	b, _ := wp3CommitBatch(t, l, wp3Records(7, 190))
	if wp3FrameCached(t, l, b) {
		t.Fatal("frame cached as written with DisableCommitVerify set")
	}
}

// A frame too large for the cache budget is not cached as written: it
// would evict every frame the consumers are reading.
func TestWP3LargeWrittenFrameNotCached(t *testing.T) {
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, _ := wp3CommitBatch(t, l, wp3Records(7, 190))
	if _, err := l.ReadShared(a); err != nil {
		t.Fatal(err)
	}
	big, _ := wp3CommitBatch(t, l, wp3Records(2, maxCachedWriteFrameBytes/2+1))
	if wp3FrameCached(t, l, big) {
		t.Fatal("oversized frame cached as written")
	}
}

// A commit whose read-back fails after its frame was cached as written
// discards the frame, cache entry included: the retry lands different
// bytes at the same offsets and file position, and reads return those.
func TestWP3FailedCommitDropsFrameCachedAsWritten(t *testing.T) {
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	a, _ := wp3CommitBatch(t, l, wp3Records(7, 190))
	if _, err := l.ReadShared(a); err != nil {
		t.Fatal(err)
	}

	// Scribble over the new frame's payload after it is written and
	// before its fsync, so the commit's read-back fails its CRC.
	corrupt := true
	fsyncHook = func(s *segment) error {
		if !corrupt {
			return nil
		}
		corrupt = false
		f, err := os.OpenFile(s.path, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.WriteAt([]byte("garbage!"), s.sizeBytes-16)
		return err
	}
	t.Cleanup(func() { fsyncHook = nil })

	first, last, err := l.AppendBatchOwned(wp3Records(7, 190))
	if err != nil {
		t.Fatal(err)
	}
	var verr VerifyError
	if err := l.CommitDurable(first, last); !errors.As(err, &verr) {
		t.Fatalf("CommitDurable = %v, want a VerifyError", err)
	}
	if hwm := l.HighWatermark(); hwm != first {
		t.Fatalf("high-watermark %d after the failed commit, want %d", hwm, first)
	}

	// The retry: same offsets, different bytes.
	retry := make([][]byte, 7)
	for i := range retry {
		retry[i] = fmt.Appendf(nil, "retry-%d", i)
	}
	rf, rl := wp3CommitBatch(t, l, retry)
	if rf != first {
		t.Fatalf("retry landed at %d, want %d", rf, first)
	}
	for off := rf; off <= rl; off++ {
		got, err := l.ReadShared(off)
		if err != nil {
			t.Fatalf("ReadShared(%d): %v", off, err)
		}
		if want := fmt.Sprintf("retry-%d", off-rf); string(got) != want {
			t.Fatalf("ReadShared(%d) = %q, want %q (a discarded frame was served)", off, got, want)
		}
	}
}
