package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// A failed writeBatch leaves its drained records in the flushing
// snapshot. The next drain must append new buffer data after them —
// never overwrite the still-unwritten snapshot — and the retry must
// eventually land every record on disk with no offset gap.
func TestFailedFlushRetriedNotLost(t *testing.T) {
	path := testLogPath(t)
	l, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	for i := range 2 {
		if _, err := l.Append(fmt.Appendf(nil, "old-%d", i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Simulate a drain whose writeBatch failed: records moved into the
	// flushing snapshot, nothing written, nothing cleared. (The flusher
	// goroutine is idle under slowFlushOpts, so calling this here is the
	// same single-drainer sequence run() would produce.)
	recs, base := l.drainBufferForFlush()
	if base != 0 || len(recs) != 2 {
		t.Fatalf("first drain: base=%d len=%d, want 0/2", base, len(recs))
	}

	for i := range 2 {
		if _, err := l.Append(fmt.Appendf(nil, "new-%d", i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// The retry drain must return the whole run from the original base.
	recs, base = l.drainBufferForFlush()
	if base != 0 || len(recs) != 4 {
		t.Fatalf("retry drain: base=%d len=%d, want 0/4 (snapshot overwritten?)", base, len(recs))
	}
	// Pending records stay readable while unwritten.
	if got, ok := l.readFlushing(0); !ok || !bytes.Equal(got, []byte("old-0")) {
		t.Fatalf("readFlushing(0) got=%q ok=%v", got, ok)
	}

	// Sync retries the pending snapshot even though the buffer is empty.
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if got := scanFramePositions(t, path); len(got) != 1 {
		t.Fatalf("after Sync: expected 1 frame on disk, got %d", len(got))
	}
	for i, want := range []string{"old-0", "old-1", "new-0", "new-1"} {
		got, err := l.Read(int64(i))
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Read(%d) got=%q err=%v want %q", i, got, err, want)
		}
	}
}

// clearFlushingThrough after a partial batch write must drop only the
// written prefix and keep the unwritten suffix readable for retry.
func TestClearFlushingThroughPartial(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	for i := range 3 {
		if _, err := l.Append(fmt.Appendf(nil, "r%d", i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, base := l.drainBufferForFlush(); base != 0 {
		t.Fatalf("drain base = %d, want 0", base)
	}

	l.clearFlushingThrough(2)
	if _, ok := l.readFlushing(1); ok {
		t.Fatalf("readFlushing(1) should be cleared")
	}
	if got, ok := l.readFlushing(2); !ok || !bytes.Equal(got, []byte("r2")) {
		t.Fatalf("readFlushing(2) got=%q ok=%v", got, ok)
	}

	l.clearFlushingThrough(3)
	if l.hasPendingFlushing() {
		t.Fatalf("flushing snapshot should be fully cleared")
	}
}

// Close must surface a failed final drain instead of silently dropping
// buffered acked records.
func TestCloseSurfacesFinalDrainError(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	if _, err := l.Append([]byte("doomed")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Force the final drain's segment write to fail by closing the
	// active segment's file handle out from under the flusher.
	l.rwmu.Lock()
	f := l.segments[len(l.segments)-1].file
	l.rwmu.Unlock()
	if err := f.Close(); err != nil {
		t.Fatalf("close segment fd: %v", err)
	}

	if err := l.Close(); err == nil {
		t.Fatalf("Close returned nil after final drain failed")
	}
}

// A drained batch bigger than the readable frame limit must be split
// into multiple frames — one giant frame would be written but rejected
// by decodeHeader on every read (a poison frame).
func TestWriteBatchSplitsOversizedBatch(t *testing.T) {
	origLimit := maxFrameInnerBytes
	maxFrameInnerBytes = 32
	defer func() { maxFrameInnerBytes = origLimit }()

	path := testLogPath(t)
	l, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	records := make([][]byte, 8)
	for i := range records {
		records[i] = fmt.Appendf(nil, "record-%d-xxxxxxxx", i)
	}
	if _, _, err := l.AppendBatch(records); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if frames := scanFramePositions(t, path); len(frames) < 2 {
		t.Fatalf("expected batch split into multiple frames, got %d", len(frames))
	}
	for i, want := range records {
		got, err := l.Read(int64(i))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) got=%q err=%v want %q", i, got, err, want)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recovery must see the same continuous offsets across split frames.
	l2, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if got := l2.NextOffset(); got != int64(len(records)) {
		t.Fatalf("NextOffset after reopen = %d, want %d", got, len(records))
	}
	for i, want := range records {
		got, err := l2.Read(int64(i))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reopen Read(%d) got=%q err=%v want %q", i, got, err, want)
		}
	}
}

func TestFrameSplitLen(t *testing.T) {
	origLimit := maxFrameInnerBytes
	maxFrameInnerBytes = 20
	defer func() { maxFrameInnerBytes = origLimit }()

	// 4+len(r) per record: each 6-byte record costs 10.
	rec := []byte("6bytes")
	if got := frameSplitLen([][]byte{rec, rec, rec}); got != 2 {
		t.Fatalf("frameSplitLen = %d, want 2", got)
	}
	if got := frameSplitLen([][]byte{rec}); got != 1 {
		t.Fatalf("frameSplitLen single = %d, want 1", got)
	}
	// A single record over the limit still yields 1 (surfaces as an
	// encodeFrame error rather than an infinite split loop).
	if got := frameSplitLen([][]byte{make([]byte, 100)}); got != 1 {
		t.Fatalf("frameSplitLen oversized = %d, want 1", got)
	}
}
