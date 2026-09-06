package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// The ingress dispatcher retries a failed commit by appending the same
// WAL records again. Before the fix, a commit whose segment write
// failed left the records in the flushing snapshot; the retry's drain
// wrote that snapshot first and the retried copy second, and the
// retry's commit advanced the high-watermark past both, so consumers
// saw every record of the failed batch twice (audit finding 2.1).
//
// Now the failed commit discards its tail: the retry is assigned the
// same offset and exactly one copy is ever visible.
func TestFailedCommitThenRetryDeliversExactlyOnce(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	payload := []byte("order-42")

	// First attempt: the append succeeds, the commit's write fails.
	off1, err := l.Append(payload)
	if err != nil {
		t.Fatal(err)
	}
	broken, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	_ = broken.Close()
	l.rwmu.Lock()
	active := l.segments[len(l.segments)-1]
	good := active.file
	active.file = broken // every WriteAt now fails, as on a full or failing disk
	l.rwmu.Unlock()

	if err := l.CommitDurable(off1, off1); err == nil {
		t.Fatal("CommitDurable succeeded with a broken segment file")
	}
	if got := l.HighWatermark(); got != 0 {
		t.Fatalf("HWM after failed commit = %d, want 0", got)
	}
	if got := l.NextOffset(); got != off1 {
		t.Fatalf("NextOffset after failed commit = %d, want the batch's offset %d back", got, off1)
	}
	if _, err := l.Read(off1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(%d) after failed commit: err=%v, want ErrOffsetNotFound (record discarded)", off1, err)
	}

	// The disk recovers.
	l.rwmu.Lock()
	active.file = good
	l.rwmu.Unlock()

	// The dispatcher's retry: the same WAL record is appended again.
	off2, err := l.Append(payload)
	if err != nil {
		t.Fatal(err)
	}
	if off2 != off1 {
		t.Fatalf("retry got offset %d, want the discarded batch's offset %d", off2, off1)
	}
	if err := l.CommitDurable(off2, off2); err != nil {
		t.Fatalf("retry CommitDurable: %v", err)
	}

	if got := l.HighWatermark(); got != off2+1 {
		t.Fatalf("HWM = %d, want %d", got, off2+1)
	}
	got, err := l.Read(off2)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("Read(%d) = (%q, %v), want %q", off2, got, err, payload)
	}
	if _, err := l.Read(off2 + 1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(%d): err=%v, want ErrOffsetNotFound (no second copy)", off2+1, err)
	}
	if frames := scanFramePositions(t, l.dir); len(frames) != 1 {
		t.Fatalf("frames on disk = %d, want exactly 1", len(frames))
	}
}

// A commit that wrote and fsynced its frames but failed to persist the
// high-watermark must truncate those frames: they are above the
// high-watermark, the dispatcher will re-append them, and a second copy
// on disk would become visible as soon as the retry commits.
func TestFailedCommitHWMPersistFailureTruncatesTail(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	off0, err := l.Append([]byte("committed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off0, off0); err != nil {
		t.Fatalf("CommitDurable(0): %v", err)
	}
	sizeAfterFirst := fileSize(t, l.dir)

	// Break the high-watermark persist: release the held descriptor and
	// point the path at a directory, so the reopen fails.
	if err := l.closeHWMFile(); err != nil {
		t.Fatal(err)
	}
	goodPath := l.hwmPath
	l.hwmPath = l.dir

	off1, err := l.Append([]byte("uncommitted"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off1, off1); err == nil {
		t.Fatal("CommitDurable succeeded although the high-watermark could not be persisted")
	}

	if got := l.HighWatermark(); got != off0+1 {
		t.Fatalf("HWM = %d, want %d", got, off0+1)
	}
	if got := l.NextOffset(); got != off1 {
		t.Fatalf("NextOffset = %d, want %d (tail rewound)", got, off1)
	}
	if got := fileSize(t, l.dir); got != sizeAfterFirst {
		t.Fatalf("segment size after failed commit = %d, want %d (frame truncated)", got, sizeAfterFirst)
	}
	if got := l.durableTail.Load(); got != off1 {
		t.Fatalf("durableTail = %d, want %d", got, off1)
	}
	if _, err := l.Read(off1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(%d): err=%v, want ErrOffsetNotFound", off1, err)
	}

	// Retry after the disk recovers.
	l.hwmPath = goodPath
	off1b, err := l.Append([]byte("uncommitted"))
	if err != nil {
		t.Fatal(err)
	}
	if off1b != off1 {
		t.Fatalf("retry offset = %d, want %d", off1b, off1)
	}
	if err := l.CommitDurable(off1b, off1b); err != nil {
		t.Fatalf("retry CommitDurable: %v", err)
	}
	if got, err := l.Read(off1b); err != nil || string(got) != "uncommitted" {
		t.Fatalf("Read(%d) = (%q, %v)", off1b, got, err)
	}
	if frames := scanFramePositions(t, l.dir); len(frames) != 2 {
		t.Fatalf("frames on disk = %d, want 2 (one per committed record)", len(frames))
	}
	persisted, err := l.PersistedHighWatermark()
	if err != nil || persisted != off1b+1 {
		t.Fatalf("persisted HWM = (%d, %v), want %d", persisted, err, off1b+1)
	}
}

// An fsync failure poisons the log: the commit fails, every later
// append and commit fails with the latched error (even though the next
// fsync would "succeed"), committed records stay readable, and a
// reopen clears the state. Audit finding 2.2.
func TestFsyncFailurePoisonsLogUntilReopen(t *testing.T) {
	path := testLogPath(t)
	l, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	off0, err := l.Append([]byte("committed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off0, off0); err != nil {
		t.Fatalf("CommitDurable(0): %v", err)
	}

	injected := errors.New("input/output error")
	fsyncHook = func(*segment) error { return injected }
	t.Cleanup(func() { fsyncHook = nil })

	off1, err := l.Append([]byte("lost-to-eio"))
	if err != nil {
		t.Fatal(err)
	}
	err = l.CommitDurable(off1, off1)
	if !errors.Is(err, ErrLogPoisoned) || !errors.Is(err, injected) {
		t.Fatalf("CommitDurable after fsync failure = %v, want ErrLogPoisoned wrapping the fsync error", err)
	}
	if l.Poisoned() == nil {
		t.Fatal("Poisoned() = nil after an fsync failure")
	}

	// The disk "recovers": a later fsync would return success, which
	// proves nothing about the pages that failed. The log must not trust it.
	fsyncHook = nil
	if _, err := l.Append([]byte("after-poison")); !errors.Is(err, ErrLogPoisoned) {
		t.Fatalf("Append on a poisoned log = %v, want ErrLogPoisoned", err)
	}
	if _, _, err := l.AppendBatch([][]byte{[]byte("after-poison")}); !errors.Is(err, ErrLogPoisoned) {
		t.Fatalf("AppendBatch on a poisoned log = %v, want ErrLogPoisoned", err)
	}
	if err := l.CommitDurable(off1, off1); !errors.Is(err, ErrLogPoisoned) {
		t.Fatalf("CommitDurable on a poisoned log = %v, want ErrLogPoisoned", err)
	}
	if got := l.HighWatermark(); got != off0+1 {
		t.Fatalf("HWM = %d, want %d (nothing exposed after the failure)", got, off0+1)
	}
	if got, err := l.Read(off0); err != nil || string(got) != "committed" {
		t.Fatalf("committed record unreadable on a poisoned log: (%q, %v)", got, err)
	}
	if err := l.Close(); !errors.Is(err, ErrLogPoisoned) {
		t.Fatalf("Close of a poisoned log = %v, want ErrLogPoisoned", err)
	}

	// Reopen: the log rescans the files and serves again.
	l2, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if l2.Poisoned() != nil {
		t.Fatal("reopened log is still poisoned")
	}
	if got, err := l2.Read(off0); err != nil || string(got) != "committed" {
		t.Fatalf("Read(%d) after reopen = (%q, %v)", off0, got, err)
	}
	off, err := l2.Append([]byte("after-reopen"))
	if err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	if err := l2.CommitDurable(off, off); err != nil {
		t.Fatalf("CommitDurable after reopen: %v", err)
	}
}

// A segment roll that fails (the partition directory cannot take a new
// file) fails the commit before anything is written, and the retry
// lands exactly one copy once the roll succeeds.
func TestFailedCommitRollFailureRetriesOnce(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 64 // one frame fills a segment
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	// A first small commit creates the hwm file while the directory is
	// still writable.
	offA, err := l.Append([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(offA, offA); err != nil {
		t.Fatalf("CommitDurable(a): %v", err)
	}

	payload := bytes.Repeat([]byte{'b'}, 64)
	off0, err := l.Append(payload)
	if err != nil {
		t.Fatal(err)
	}
	// The directory refuses new files from here on, so the roll that the
	// filling commit wants to do after it succeeds is deferred, and the
	// next commit's pre-write roll fails.
	if err := os.Chmod(l.dir, 0o500); err != nil {
		t.Fatal(err)
	}
	restore := func() { _ = os.Chmod(l.dir, 0o700) }
	t.Cleanup(restore)
	if err := l.CommitDurable(off0, off0); err != nil {
		t.Fatalf("filling CommitDurable: %v (a deferred roll must not fail a committed batch)", err)
	}
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after deferred roll = %d, want 1", got)
	}

	off1, err := l.Append([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off1, off1); err == nil {
		t.Fatal("second CommitDurable succeeded although the roll could not create a segment")
	}
	if got := l.NextOffset(); got != off1 {
		t.Fatalf("NextOffset after failed roll = %d, want %d", got, off1)
	}
	if got := l.HighWatermark(); got != off0+1 {
		t.Fatalf("HWM after failed roll = %d, want %d", got, off0+1)
	}

	restore()
	off1b, err := l.Append([]byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if off1b != off1 {
		t.Fatalf("retry offset = %d, want %d", off1b, off1)
	}
	if err := l.CommitDurable(off1b, off1b); err != nil {
		t.Fatalf("retry CommitDurable: %v", err)
	}
	if got := l.SegmentCount(); got < 2 {
		t.Fatalf("segments after the retry = %d, want the roll to have happened", got)
	}
	if got, err := l.Read(off1b); err != nil || string(got) != "second" {
		t.Fatalf("Read(%d) = (%q, %v)", off1b, got, err)
	}
	if _, err := l.Read(off1b + 1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(%d): err=%v, want ErrOffsetNotFound (no duplicate)", off1b+1, err)
	}
}

// A crash in the middle of a commit's frame write leaves a torn frame at
// the tail of the active segment and the high-watermark at its
// pre-commit value. Recovery truncates the torn frame, keeps the
// high-watermark, and the ingress WAL's re-commit lands at the offset
// the torn batch had.
func TestRecoveryTruncatesTornCommitFrame(t *testing.T) {
	path := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	l, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	off0, err := l.Append([]byte("committed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off0, off0); err != nil {
		t.Fatalf("CommitDurable: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sizeBefore := fileSize(t, path)

	// The "crash": half of the next batch's frame reaches the file.
	var enc frameEncoder
	frame, err := enc.encodeFrame([][]byte{[]byte("torn-batch-record")}, off0+1, codec.NewNoopCodec())
	if err != nil {
		t.Fatal(err)
	}
	segs := segmentPaths(t, path)
	f, err := os.OpenFile(segs[len(segs)-1], os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(frame[:len(frame)/2]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	l2, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if got := fileSize(t, path); got != sizeBefore {
		t.Fatalf("segment size after recovery = %d, want %d (torn frame truncated)", got, sizeBefore)
	}
	if got := l2.NextOffset(); got != off0+1 {
		t.Fatalf("NextOffset after recovery = %d, want %d", got, off0+1)
	}
	if got := l2.HighWatermark(); got != off0+1 {
		t.Fatalf("HWM after recovery = %d, want %d", got, off0+1)
	}
	if _, err := l2.Read(off0 + 1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(%d): err=%v, want ErrOffsetNotFound", off0+1, err)
	}

	// The WAL re-commits the torn batch.
	off1, err := l2.Append([]byte("torn-batch-record"))
	if err != nil {
		t.Fatal(err)
	}
	if off1 != off0+1 {
		t.Fatalf("re-commit offset = %d, want %d", off1, off0+1)
	}
	if err := l2.CommitDurable(off1, off1); err != nil {
		t.Fatalf("CommitDurable after recovery: %v", err)
	}
	if got, err := l2.Read(off1); err != nil || string(got) != "torn-batch-record" {
		t.Fatalf("Read(%d) = (%q, %v)", off1, got, err)
	}
}

// Written frames stay in the flushing snapshot until an fsync proves
// them durable; only then is the in-memory copy released.
func TestFlushingSnapshotClearedOnlyAfterSync(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	if _, err := l.Append([]byte("r0")); err != nil {
		t.Fatal(err)
	}
	// Write without a forced sync (the batched-sync interval has not
	// elapsed): the frame is on disk but not durable.
	if err := l.flusher.drainOnce(false, true, nil); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
	if frames := scanFramePositions(t, l.dir); len(frames) != 1 {
		t.Fatalf("frames on disk = %d, want 1", len(frames))
	}
	if l.hasPendingFlushing() {
		t.Fatal("a written frame must not count as pending (it needs no rewrite)")
	}
	if got, ok := l.readFlushing(0); !ok || string(got) != "r0" {
		t.Fatalf("readFlushing(0) = (%q, %v), want the record still in the snapshot", got, ok)
	}
	if got := l.durableTail.Load(); got != 0 {
		t.Fatalf("durableTail = %d before the sync, want 0", got)
	}

	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if _, ok := l.readFlushing(0); ok {
		t.Fatal("snapshot still holds the record after a successful sync")
	}
	if got := l.durableTail.Load(); got != 1 {
		t.Fatalf("durableTail = %d after the sync, want 1", got)
	}
	if got, err := l.Read(0); err != nil || string(got) != "r0" {
		t.Fatalf("Read(0) from disk = (%q, %v)", got, err)
	}
}

// A failed commit also discards anything appended after the failed
// batch: those records are above the high-watermark too, and their
// owner will re-append them.
func TestFailedCommitDiscardsLaterAppendsToo(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	if err := l.closeHWMFile(); err != nil {
		t.Fatal(err)
	}
	goodPath := l.hwmPath
	l.hwmPath = l.dir

	first, last, err := l.AppendBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(first, last); err == nil {
		t.Fatal("CommitDurable succeeded with an unwritable hwm path")
	}
	if got := l.NextOffset(); got != 0 {
		t.Fatalf("NextOffset = %d, want 0", got)
	}
	if got := fileSize(t, l.dir); got != 0 {
		t.Fatalf("segment size = %d, want 0 (whole batch truncated)", got)
	}

	l.hwmPath = goodPath
	first, last, err = l.AppendBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err != nil {
		t.Fatal(err)
	}
	if first != 0 || last != 2 {
		t.Fatalf("retry offsets = [%d,%d], want [0,2]", first, last)
	}
	if err := l.CommitDurable(first, last); err != nil {
		t.Fatalf("retry CommitDurable: %v", err)
	}
	for i, want := range []string{"a", "b", "c"} {
		if got, err := l.Read(int64(i)); err != nil || string(got) != want {
			t.Fatalf("Read(%d) = (%q, %v), want %q", i, got, err, want)
		}
	}
	if _, err := l.Read(3); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(3): err=%v, want ErrOffsetNotFound", err)
	}
}

// frameBoundaryAtOrAboveLocked finds the first frame at or above an
// offset by walking headers from the sparse anchor, and never splits a
// frame.
func TestFrameBoundaryAtOrAbove(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	// Three frames: [0,2), [2,5), [5,6).
	for _, batch := range [][][]byte{
		{[]byte("a"), []byte("b")},
		{[]byte("c"), []byte("d"), []byte("e")},
		{[]byte("f")},
	} {
		if _, _, err := l.AppendBatch(batch); err != nil {
			t.Fatal(err)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	frames := scanFramePositions(t, l.dir)
	if len(frames) != 3 {
		t.Fatalf("frames = %d, want 3", len(frames))
	}

	l.rwmu.Lock()
	defer l.rwmu.Unlock()
	active := l.segments[0]
	for _, tc := range []struct {
		offset       int64
		wantOffset   int64
		wantFrameIdx int
	}{
		{0, 0, 0},
		{1, 2, 1}, // straddles frame 0: cut after it
		{2, 2, 1},
		{3, 5, 2},
		{5, 5, 2},
		{6, 6, -1}, // nothing above
	} {
		gotOffset, gotPos := l.frameBoundaryAtOrAboveLocked(active, tc.offset)
		wantPos := active.sizeBytes
		if tc.wantFrameIdx >= 0 {
			wantPos = frames[tc.wantFrameIdx]
		}
		if gotOffset != tc.wantOffset || gotPos != wantPos {
			t.Errorf("boundary(%d) = (%d, %d), want (%d, %d)", tc.offset, gotOffset, gotPos, tc.wantOffset, wantPos)
		}
	}
}

// Regression guard for the segment file the audit test checks: a
// discard never removes the active segment file itself, only its tail.
func TestDiscardKeepsActiveSegmentFile(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()
	if err := l.closeHWMFile(); err != nil {
		t.Fatal(err)
	}
	l.hwmPath = l.dir
	off, err := l.Append([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off, off); err == nil {
		t.Fatal("CommitDurable succeeded with an unwritable hwm path")
	}
	if _, err := os.Stat(filepath.Join(l.dir, segmentFileName(0))); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(segmentPaths(t, l.dir)); got != fmt.Sprint([]string{filepath.Join(l.dir, segmentFileName(0))}) {
		t.Fatalf("segments = %s", got)
	}
}
