package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// ---- helpers --------------------------------------------------------

// testLogPath returns a fresh per-test log file path.
func testLogPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "p.log")
}

// slowFlushOpts returns Options that effectively disable timer-based
// auto-flush, so tests that read from the buffer aren't racing the
// flusher. Close() still forces a final flush, which is how tests
// trigger disk persistence.
func slowFlushOpts(t *testing.T, c codec.Codec) Options {
	t.Helper()
	if c == nil {
		c = codec.NewNoopCodec()
	}
	return Options{
		Codec:         c,
		FlushBytes:    1 << 30, // 1 GiB — never crossed in tests
		FlushRecords:  1 << 20,
		FlushInterval: 1 * time.Hour,
	}
}

// fastFlushOpts returns Options that flush as soon as anything is
// pushed (FlushRecords=1, short interval). Useful for tests that want
// disk-side behaviour without explicitly closing.
func fastFlushOpts(t *testing.T, c codec.Codec) Options {
	t.Helper()
	if c == nil {
		c = codec.NewNoopCodec()
	}
	return Options{
		Codec:         c,
		FlushBytes:    1,
		FlushRecords:  1,
		FlushInterval: 5 * time.Millisecond,
	}
}

// requireRead opens a log, calls fn, closes the log, and surfaces
// errors with t.Fatalf.
func mustWriteAndClose(t *testing.T, path string, opts Options, fn func(*Log)) {
	t.Helper()
	l, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	fn(l)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// ---- 1. Round-trip via buffer (no flush) ---------------------------

func TestAppendReadFromBuffer(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	off, err := l.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if off != 0 {
		t.Fatalf("first offset want 0 got %d", off)
	}

	got, err := l.Read(0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("got %q", got)
	}
	if l.NextOffset() != 1 {
		t.Fatalf("NextOffset got %d", l.NextOffset())
	}
}

// ---- 2. Round-trip after flush (reopens log to force disk path) ----

func TestRoundTripAfterFlushAndReopen(t *testing.T) {
	path := testLogPath(t)
	mustWriteAndClose(t, path, slowFlushOpts(t, nil), func(l *Log) {
		for i := range 5 {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
	})

	l, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if l.NextOffset() != 5 {
		t.Fatalf("NextOffset after reopen want 5 got %d", l.NextOffset())
	}
	for i := range int64(5) {
		got, err := l.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		want := fmt.Appendf(nil, "rec-%d", i)
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", i, got, want)
		}
	}
}

func TestHighWatermarkPersistsAcrossRestart(t *testing.T) {
	path := testLogPath(t)
	mustWriteAndClose(t, path, slowFlushOpts(t, nil), func(l *Log) {
		for i := range 3 {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
		if err := l.AdvanceHighWatermark(2); err != nil {
			t.Fatalf("AdvanceHighWatermark: %v", err)
		}
	})

	l, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if got := l.HighWatermark(); got != 2 {
		t.Fatalf("HighWatermark() = %d, want 2", got)
	}
	if got := l.NextOffset(); got != 3 {
		t.Fatalf("NextOffset() = %d, want 3", got)
	}
}

func TestHighWatermarkMissingFileBootstrapsFromTail(t *testing.T) {
	path := testLogPath(t)
	mustWriteAndClose(t, path, slowFlushOpts(t, nil), func(l *Log) {
		for i := range 2 {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
	})
	if err := os.Remove(filepath.Join(path, hwmFileName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Remove(hwm): %v", err)
	}

	l, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if got := l.HighWatermark(); got != 2 {
		t.Fatalf("HighWatermark() = %d, want 2", got)
	}
}

func TestHighWatermarkClampsToRecoveredTail(t *testing.T) {
	path := testLogPath(t)
	mustWriteAndClose(t, path, slowFlushOpts(t, nil), func(l *Log) {
		if _, err := l.Append([]byte("rec-0")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	})
	if err := os.WriteFile(filepath.Join(path, hwmFileName), []byte{0, 0, 0, 0, 0, 0, 0, 9}, 0o644); err != nil {
		t.Fatalf("WriteFile(hwm): %v", err)
	}

	l, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if got := l.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", got)
	}
}

func TestHighWatermarkHiddenTailSurvivesRestart(t *testing.T) {
	path := testLogPath(t)
	mustWriteAndClose(t, path, slowFlushOpts(t, nil), func(l *Log) {
		for i := range 3 {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		}
		if err := l.AdvanceHighWatermark(1); err != nil {
			t.Fatalf("AdvanceHighWatermark: %v", err)
		}
	})

	l, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if l.NextOffset() != 3 {
		t.Fatalf("NextOffset after reopen want 3 got %d", l.NextOffset())
	}
	if got := l.HighWatermark(); got != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", got)
	}
	for i := range int64(3) {
		got, err := l.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		want := fmt.Appendf(nil, "rec-%d", i)
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", i, got, want)
		}
	}
}

// ---- 3. Batch round-trip ------------------------------------------

func TestAppendBatch(t *testing.T) {
	path := testLogPath(t)
	records := [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")}

	mustWriteAndClose(t, path, slowFlushOpts(t, nil), func(l *Log) {
		first, last, err := l.AppendBatch(records)
		if err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		if first != 0 || last != 2 {
			t.Fatalf("offsets first=%d last=%d", first, last)
		}
	})

	l, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	for i, want := range records {
		got, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", i, got, want)
		}
	}
}

// ---- 7. Compression round-trip + on-disk size sanity ---------------

func TestZstdCompressionRoundTripShrinks(t *testing.T) {
	zc, err := codec.NewZstdCodec(zstd.SpeedBestCompression)
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}

	// A repetitive payload — zstd should compress this aggressively.
	payload := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 200)

	pathZstd := testLogPath(t)
	mustWriteAndClose(t, pathZstd, slowFlushOpts(t, zc), func(l *Log) {
		for i := 0; i < 20; i++ {
			if _, err := l.Append(payload); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
	})

	pathRaw := testLogPath(t)
	mustWriteAndClose(t, pathRaw, slowFlushOpts(t, codec.NewNoopCodec()), func(l *Log) {
		for i := 0; i < 20; i++ {
			if _, err := l.Append(payload); err != nil {
				t.Fatalf("Append: %v", err)
			}
		}
	})

	zSize := fileSize(t, pathZstd)
	rSize := fileSize(t, pathRaw)
	if zSize >= rSize {
		t.Fatalf("zstd file (%d) not smaller than raw (%d)", zSize, rSize)
	}
	// Sanity ratio: this corpus should compress at least 4x.
	if rSize/zSize < 4 {
		t.Logf("warning: weaker compression than expected (raw=%d zstd=%d)", rSize, zSize)
	}

	// And the round-trip read still works.
	l, err := NewLog(pathZstd, slowFlushOpts(t, zc))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	for i := int64(0); i < 20; i++ {
		got, err := l.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("Read %d payload mismatch (len got=%d want=%d)", i, len(got), len(payload))
		}
	}
}

// ---- 4. CRC mismatch in middle of file: skip-and-continue ---------

func TestRecoverySkipsCorruptMiddleFrame(t *testing.T) {
	path := testLogPath(t)

	// Write three single-record frames as three separate Open→Append→
	// Close cycles, so each Close produces its own frame on disk
	// (otherwise the slow-flush opts would coalesce them all into one
	// frame at the final Close).
	for i := 0; i < 3; i++ {
		mustWriteAndClose(t, path, slowFlushOpts(t, codec.NewNoopCodec()), func(l *Log) {
			if _, _, err := l.AppendBatch([][]byte{[]byte(fmt.Sprintf("frame-%d", i))}); err != nil {
				t.Fatalf("AppendBatch: %v", err)
			}
		})
	}

	// Locate frame boundaries by walking the (still-valid) file.
	frames := scanFramePositions(t, path)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}

	// Corrupt one byte in the middle of frame 1's payload (skipping
	// past the 27-byte header so we don't accidentally land on the
	// magic and bias the test).
	sizeBefore := fileSize(t, path)
	corruptByteAt(t, path, frames[1]+headerSize+2)

	// Reopen — recovery should skip the corrupt frame, NOT truncate.
	l, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if got := fileSize(t, path); got != sizeBefore {
		t.Fatalf("file truncated after CRC mismatch: was %d now %d", sizeBefore, got)
	}

	// Frames 0 and 2 readable.
	if got, err := l.Read(0); err != nil || !bytes.Equal(got, []byte("frame-0")) {
		t.Fatalf("Read(0) got=%q err=%v", got, err)
	}
	if got, err := l.Read(2); err != nil || !bytes.Equal(got, []byte("frame-2")) {
		t.Fatalf("Read(2) got=%q err=%v", got, err)
	}
	// Offset 1 is gone.
	if _, err := l.Read(1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(1) want ErrOffsetNotFound got %v", err)
	}
}

// ---- 5. Magic-byte resync ----------------------------------------

func TestRecoveryResyncsAfterMagicWipe(t *testing.T) {
	path := testLogPath(t)

	for i := 0; i < 3; i++ {
		mustWriteAndClose(t, path, slowFlushOpts(t, codec.NewNoopCodec()), func(l *Log) {
			if _, _, err := l.AppendBatch([][]byte{[]byte(fmt.Sprintf("frame-%d", i))}); err != nil {
				t.Fatalf("AppendBatch: %v", err)
			}
		})
	}

	frames := scanFramePositions(t, path)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}

	// Wipe the magic of frame 1 entirely — this looks like garbage
	// until we hit the next valid magic at frames[2].
	wipeMagic(t, path, frames[1])

	l, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if got, err := l.Read(0); err != nil || !bytes.Equal(got, []byte("frame-0")) {
		t.Fatalf("Read(0) got=%q err=%v", got, err)
	}
	if got, err := l.Read(2); err != nil || !bytes.Equal(got, []byte("frame-2")) {
		t.Fatalf("Read(2) got=%q err=%v", got, err)
	}
	if _, err := l.Read(1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(1) want ErrOffsetNotFound got %v", err)
	}
}

// ---- 6. Torn tail truncation -------------------------------------

func TestRecoveryTruncatesTornTail(t *testing.T) {
	path := testLogPath(t)

	mustWriteAndClose(t, path, slowFlushOpts(t, codec.NewNoopCodec()), func(l *Log) {
		for i := 0; i < 2; i++ {
			if _, _, err := l.AppendBatch([][]byte{[]byte(fmt.Sprintf("frame-%d", i))}); err != nil {
				t.Fatalf("AppendBatch: %v", err)
			}
		}
	})

	sizeBefore := fileSize(t, path)
	activePath := activeSegmentPath(t, path)

	// Simulate an interrupted write: append a partial frame's worth of
	// bytes to the end of the active segment file (just a magic + a
	// few header bytes, no full payload).
	f, err := os.OpenFile(activePath, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := f.Write([]byte{magicByte0, magicByte1, 0, 0, 0, 0, 5}); err != nil {
		t.Fatalf("write torn: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := fileSize(t, path); got <= sizeBefore {
		t.Fatalf("torn write didn't grow file: %d -> %d", sizeBefore, got)
	}

	l, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if got := fileSize(t, path); got != sizeBefore {
		t.Fatalf("torn tail not truncated: was %d now %d (expected %d)", got, fileSize(t, path), sizeBefore)
	}

	for i := int64(0); i < 2; i++ {
		got, err := l.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		want := []byte(fmt.Sprintf("frame-%d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", i, got, want)
		}
	}
}

// ---- 8. Graceful shutdown flushes ---------------------------------

func TestCloseFlushesPendingRecords(t *testing.T) {
	path := testLogPath(t)
	// Use slow flusher so we know the flush is happening at Close,
	// not from the periodic timer.
	l, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	for i := 0; i < 4; i++ {
		if _, err := l.Append([]byte(fmt.Sprintf("r%d", i))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// File must be non-empty after Close.
	if sz := fileSize(t, path); sz == 0 {
		t.Fatalf("file empty after Close — final flush didn't run")
	}

	// Subsequent Append must report ErrLogClosed.
	if _, err := l.Append([]byte("x")); !errors.Is(err, ErrLogClosed) {
		t.Fatalf("Append after Close want ErrLogClosed got %v", err)
	}

	// Reopen and verify all records.
	l2, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	for i := int64(0); i < 4; i++ {
		got, err := l2.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		want := []byte(fmt.Sprintf("r%d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", i, got, want)
		}
	}
}

// ---- 9. Concurrent producers -------------------------------------

func TestConcurrentAppendOffsetsUniqueAndContiguous(t *testing.T) {
	const goroutines = 16
	const perGoroutine = 250

	path := testLogPath(t)
	l, err := NewLog(path, fastFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	offsets := make([]int64, 0, goroutines*perGoroutine)
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				off, err := l.Append([]byte(fmt.Sprintf("g%d-j%d", g, j)))
				if err != nil {
					t.Errorf("Append: %v", err)
					return
				}
				mu.Lock()
				offsets = append(offsets, off)
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// All offsets unique and form [0, N).
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	for i, o := range offsets {
		if int64(i) != o {
			t.Fatalf("offset[%d] = %d (want %d) — not contiguous", i, o, i)
		}
	}

	// All records readable after reopen.
	l2, err := NewLog(path, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if l2.NextOffset() != int64(goroutines*perGoroutine) {
		t.Fatalf("NextOffset want %d got %d", goroutines*perGoroutine, l2.NextOffset())
	}
	for _, o := range offsets {
		if _, err := l2.Read(o); err != nil {
			t.Fatalf("Read %d after reopen: %v", o, err)
		}
	}
}

// ---- segment-specific tests ---------------------------------------

// rollOpts is like slowFlushOpts but with a tiny SegmentBytes so that
// even small writes force segment rolls.
func rollOpts(t *testing.T, segmentBytes int64) Options {
	t.Helper()
	return Options{
		Codec:         codec.NewNoopCodec(),
		FlushBytes:    1 << 30,
		FlushRecords:  1 << 20,
		FlushInterval: 1 * time.Hour,
		SegmentBytes:  segmentBytes,
	}
}

// 1. Segment rollover — writes that cross SegmentBytes produce
// multiple segment files.
func TestSegmentRollover(t *testing.T) {
	dir := testLogPath(t)
	// Each frame is at least headerSize (27) + record bytes; with
	// 250-byte records and 256-byte segment threshold, every frame
	// after the first crosses the threshold.
	payload := bytes.Repeat([]byte{'x'}, 250)

	for i := range 5 {
		mustWriteAndClose(t, dir, rollOpts(t, 256), func(l *Log) {
			if _, err := l.Append(payload); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		})
	}

	paths := segmentPaths(t, dir)
	if len(paths) < 3 {
		t.Fatalf("expected ≥3 segments after rolls, got %d (%v)", len(paths), paths)
	}

	// All records should still be readable end-to-end.
	l, err := NewLog(dir, rollOpts(t, 256))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	for off := int64(0); off < 5; off++ {
		got, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read %d: %v", off, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("Read %d payload mismatch", off)
		}
	}
}

// 2. Multi-segment recovery — close, reopen, all offsets readable.
func TestMultiSegmentRecovery(t *testing.T) {
	dir := testLogPath(t)
	// Generate ≥3 segments with 50 records, each ~80 bytes → ~107 byte
	// frame; segment_bytes=300 → 3 frames per segment.
	const total = 50

	for i := range total {
		mustWriteAndClose(t, dir, rollOpts(t, 300), func(l *Log) {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%05d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		})
	}
	if got := len(segmentPaths(t, dir)); got < 3 {
		t.Fatalf("expected ≥3 segments, got %d", got)
	}

	l, err := NewLog(dir, rollOpts(t, 300))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	if l.NextOffset() != total {
		t.Fatalf("NextOffset want %d got %d", total, l.NextOffset())
	}
	for i := range int64(total) {
		want := fmt.Appendf(nil, "rec-%05d", i)
		got, err := l.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", i, got, want)
		}
	}
}

// 3. Mid-segment corruption in a sealed (non-last) segment skips that
// frame; later segments stay readable; no segment file is shorter.
func TestMidSegmentCorruptionInSealedSegmentSkipped(t *testing.T) {
	dir := testLogPath(t)
	// SegmentBytes=20 forces a roll after every frame (a single
	// minimum frame is ≥ headerSize=27 > 20), so each cycle produces
	// exactly one record per segment. With 6 cycles we get 6 sealed
	// segments + 1 empty active.
	for i := range 6 {
		mustWriteAndClose(t, dir, rollOpts(t, 20), func(l *Log) {
			if _, err := l.Append(fmt.Appendf(nil, "frame-%d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		})
	}
	paths := segmentPaths(t, dir)
	if len(paths) < 3 {
		t.Fatalf("expected ≥3 segments, got %d", len(paths))
	}

	sizesBefore := make([]int64, len(paths))
	for i, p := range paths {
		st, _ := os.Stat(p)
		sizesBefore[i] = st.Size()
	}

	// Corrupt 1 byte in the payload of segment[1] (offset 30 = past
	// 27-byte header).
	corruptByteAtPath(t, paths[1], 30)

	l, err := NewLog(dir, rollOpts(t, 20))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	// No segment file shorter than before — we never truncate on
	// mid-file corruption.
	for i, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if st.Size() < sizesBefore[i] {
			t.Fatalf("segment %d truncated: was %d now %d", i, sizesBefore[i], st.Size())
		}
	}

	// Record 1 (in segment[1]) is now a gap; the rest readable.
	for _, off := range []int64{0, 2, 3, 4, 5} {
		want := fmt.Appendf(nil, "frame-%d", off)
		got, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read %d: %v", off, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", off, got, want)
		}
	}
	if _, err := l.Read(1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(1) want ErrOffsetNotFound got %v", err)
	}
}

// 5. Torn tail on a sealed (non-last) segment is NOT truncated.
func TestTornTailOnSealedSegmentNotTruncated(t *testing.T) {
	dir := testLogPath(t)
	// SegmentBytes=20 → one record per segment (see comment in
	// TestMidSegmentCorruptionInSealedSegmentSkipped).
	for i := range 4 {
		mustWriteAndClose(t, dir, rollOpts(t, 20), func(l *Log) {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		})
	}
	paths := segmentPaths(t, dir)
	if len(paths) < 3 {
		t.Fatalf("expected ≥3 segments, got %d", len(paths))
	}

	// Append a partial frame to segment[0] (sealed).
	sealedPath := paths[0]
	stBefore, _ := os.Stat(sealedPath)
	f, err := os.OpenFile(sealedPath, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open sealed: %v", err)
	}
	if _, err := f.Write([]byte{magicByte0, magicByte1, 0, 0, 0, 0, 5}); err != nil {
		t.Fatalf("write: %v", err)
	}
	f.Close()
	stAfter, _ := os.Stat(sealedPath)

	l, err := NewLog(dir, rollOpts(t, 20))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	stNow, _ := os.Stat(sealedPath)
	if stNow.Size() != stAfter.Size() {
		t.Fatalf("sealed segment was truncated: was %d after corruption %d after recovery %d (original %d)",
			stAfter.Size(), stAfter.Size(), stNow.Size(), stBefore.Size())
	}

	// Records from later segments still readable.
	for _, off := range []int64{1, 2, 3} {
		if _, err := l.Read(off); err != nil {
			t.Fatalf("Read %d: %v", off, err)
		}
	}
}

// 6. Roll preserves offset continuity — no gaps, no repeats.
func TestRollPreservesOffsetContinuity(t *testing.T) {
	dir := testLogPath(t)
	const total = 30

	l, err := NewLog(dir, Options{
		Codec:         codec.NewNoopCodec(),
		FlushBytes:    1, // flush every record
		FlushRecords:  1,
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  100, // tiny, so we roll often
	})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	got := make([]int64, 0, total)
	for i := range total {
		off, err := l.Append(fmt.Appendf(nil, "rec-%d", i))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		got = append(got, off)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for i, o := range got {
		if int64(i) != o {
			t.Fatalf("offset[%d] = %d (want %d)", i, o, i)
		}
	}

	// Reopen and confirm the same offsets are readable across the
	// segment boundaries.
	l2, err := NewLog(dir, rollOpts(t, 100))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	for i := range int64(total) {
		got, err := l2.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		want := fmt.Appendf(nil, "rec-%d", i)
		if !bytes.Equal(got, want) {
			t.Fatalf("Read %d got %q want %q", i, got, want)
		}
	}
}

// ---- retention tests ----------------------------------------------

// retentionOpts builds Options for a retention test: writes flush
// immediately, segments are tiny (one frame each), and retention runs
// only when manually triggered (the reaper is started but its ticker
// never fires within the test's wall-clock lifetime — instead we
// invoke sweep directly via the test hook below). The clock is a
// pointer so tests can advance it.
func retentionOpts(t *testing.T, clock *atomicTime, cfg RetentionConfig) Options {
	t.Helper()
	cfg.Now = clock.Get
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 1 * time.Hour // never fires during a test run
	}
	return Options{
		Codec:         codec.NewNoopCodec(),
		FlushBytes:    1,
		FlushRecords:  1,
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  20,
		Retention:     cfg,
	}
}

// atomicTime is a tiny goroutine-safe clock for retention tests.
type atomicTime struct {
	mu sync.Mutex
	t  time.Time
}

func newAtomicTime(start time.Time) *atomicTime {
	return &atomicTime{t: start}
}

func (a *atomicTime) Get() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.t
}

func (a *atomicTime) Set(t time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.t = t
}

// produceN writes N single-record frames using mustWriteAndClose so
// each one ends up in its own segment (with SegmentBytes=20 forcing a
// roll after every record).
func produceN(t *testing.T, dir string, opts Options, n int) {
	t.Helper()
	for i := range n {
		mustWriteAndClose(t, dir, opts, func(l *Log) {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%d", i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
		})
	}
}

// 1. Age-based retention deletes old sealed segments. Active stays;
// the offsets in deleted segments become permanent gaps.
func TestRetentionDeletesOldSegments(t *testing.T) {
	clock := newAtomicTime(time.Now())
	opts := retentionOpts(t, clock, RetentionConfig{
		MaxAge:        10 * time.Minute,
		CheckInterval: 1 * time.Hour,
	})
	dir := testLogPath(t)

	produceN(t, dir, opts, 5) // 5 sealed + 1 empty active

	pathsBefore := segmentPaths(t, dir)
	if len(pathsBefore) < 5 {
		t.Fatalf("expected ≥5 segments, got %d", len(pathsBefore))
	}

	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	clock.Set(time.Now().Add(1 * time.Hour)) // every sealed segment is "old"
	l.reaper.sweep()

	// All sealed segments deleted; only the (empty) active remains.
	paths := segmentPaths(t, dir)
	if len(paths) != 1 {
		t.Fatalf("expected only the active segment after sweep, got %d (%v)", len(paths), paths)
	}

	// Reads of deleted offsets fail with ErrOffsetNotFound.
	for off := int64(0); off < 5; off++ {
		if _, err := l.Read(off); !errors.Is(err, ErrOffsetNotFound) {
			t.Fatalf("Read(%d) want ErrOffsetNotFound got %v", off, err)
		}
	}

	// New writes still work — active segment is intact.
	if _, err := l.Append([]byte("after-sweep")); err != nil {
		t.Fatalf("Append after sweep: %v", err)
	}
}

// 2. Active segment is never deleted, even if it's the only segment
// and the retention bound says everything should go.
func TestRetentionRespectsActiveSegment(t *testing.T) {
	clock := newAtomicTime(time.Now())
	dir := testLogPath(t)

	// Single record into a fresh log with default-ish (large)
	// segment bytes — no roll, so the only segment IS the active
	// one.
	noRollOpts := Options{
		Codec:         codec.NewNoopCodec(),
		FlushBytes:    1,
		FlushRecords:  1,
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  1 << 20, // big — no roll for a single small record
		Retention: RetentionConfig{
			MaxAge:        1 * time.Nanosecond, // everything is "old"
			CheckInterval: 1 * time.Hour,
			Now:           clock.Get,
		},
	}
	mustWriteAndClose(t, dir, noRollOpts, func(l *Log) {
		if _, err := l.Append([]byte("rec-0")); err != nil {
			t.Fatalf("Append: %v", err)
		}
	})

	if got := len(segmentPaths(t, dir)); got != 1 {
		t.Fatalf("expected exactly 1 segment, got %d", got)
	}

	l, err := NewLog(dir, noRollOpts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	clock.Set(clock.Get().Add(1 * time.Hour))
	l.reaper.sweep()

	if got := len(segmentPaths(t, dir)); got == 0 {
		t.Fatalf("active segment was deleted")
	}
	if _, err := l.Read(0); err != nil {
		t.Fatalf("Read(0) after sweep: %v", err)
	}
}

// 4. Retention disabled (both fields zero) → reaper does nothing.
func TestRetentionDisabledIsNoop(t *testing.T) {
	clock := newAtomicTime(time.Now())
	opts := retentionOpts(t, clock, RetentionConfig{})
	dir := testLogPath(t)

	produceN(t, dir, opts, 4)
	beforePaths := segmentPaths(t, dir)
	beforeSize := fileSize(t, dir)

	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	clock.Set(clock.Get().Add(1000 * time.Hour))
	l.reaper.sweep()

	afterPaths := segmentPaths(t, dir)
	afterSize := fileSize(t, dir)
	if len(afterPaths) != len(beforePaths) {
		t.Fatalf("retention disabled but segments changed: was %d, now %d", len(beforePaths), len(afterPaths))
	}
	if afterSize != beforeSize {
		t.Fatalf("retention disabled but size changed: was %d, now %d", beforeSize, afterSize)
	}
}

func TestDefaultOptionsAndAccessors(t *testing.T) {
	dir := testLogPath(t)
	l, err := NewLog(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	defer l.Close()

	if l.NotifyC() == nil {
		t.Fatal("NotifyC() = nil")
	}
	if got := l.OldestOffset(); got != 0 {
		t.Fatalf("OldestOffset() = %d, want 0", got)
	}
	if got := l.SizeBytes(); got != 0 {
		t.Fatalf("SizeBytes() = %d, want 0", got)
	}
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("SegmentCount() = %d, want 1", got)
	}
	if got := l.LatestOffset(); got != 0 {
		t.Fatalf("LatestOffset() = %d, want 0", got)
	}
	if mt, ok := l.OldestSegmentAt(); !ok || mt <= 0 {
		t.Fatalf("OldestSegmentAt() = (%d, %t), want valid mtime", mt, ok)
	}
	if mt, ok := l.SegmentMTimeForOffset(0); ok || mt != 0 {
		t.Fatalf("SegmentMTimeForOffset(0) = (%d, %t), want no match", mt, ok)
	}
	if l.findSegmentLocked(999) != nil {
		t.Fatal("findSegmentLocked() found unexpected segment")
	}

	off, err := l.Append([]byte("hello"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if off != 0 {
		t.Fatalf("Append() offset = %d, want 0", off)
	}
	if got := l.LatestOffset(); got != 0 {
		t.Fatalf("LatestOffset() = %d, want 0", got)
	}
	if mt, ok := l.SegmentMTimeForOffset(0); ok || mt != 0 {
		t.Fatalf("SegmentMTimeForOffset(0) = (%d, %t), want no flushed match", mt, ok)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	l, err = NewLog(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen NewLog() error = %v", err)
	}
	defer l.Close()
	if mt, ok := l.SegmentMTimeForOffset(0); !ok || mt <= 0 {
		t.Fatalf("SegmentMTimeForOffset(0) = (%d, %t), want valid mtime", mt, ok)
	}
	if mt, ok := l.SegmentMTimeForOffset(1); ok || mt != 0 {
		t.Fatalf("SegmentMTimeForOffset(1) = (%d, %t), want no match", mt, ok)
	}
	if seg := l.findSegmentLocked(0); seg == nil {
		t.Fatal("findSegmentLocked(0) = nil, want segment")
	}
}

func TestListSegmentFileNamesSortsAndFilters(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		segmentFileName(20),
		segmentFileName(5),
		"notes.txt",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", name, err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, segmentFileName(99)), 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	names, err := listSegmentFileNames(dir)
	if err != nil {
		t.Fatalf("listSegmentFileNames() error = %v", err)
	}
	want := []string{segmentFileName(5), segmentFileName(20)}
	if fmt.Sprint(names) != fmt.Sprint(want) {
		t.Fatalf("listSegmentFileNames() = %v, want %v", names, want)
	}
}

func TestSegmentHelpersCoverClosedPaths(t *testing.T) {
	dir := t.TempDir()
	seg, err := createSegment(dir, 7)
	if err != nil {
		t.Fatalf("createSegment() error = %v", err)
	}
	if seg.path != filepath.Join(dir, segmentFileName(7)) {
		t.Fatalf("segment path = %q, want %q", seg.path, filepath.Join(dir, segmentFileName(7)))
	}
	if seg.baseOffset != 7 || seg.nextOffset != 7 || seg.sizeBytes != 0 {
		t.Fatalf("segment state = %+v, want base=7 next=7 size=0", seg)
	}
	if err := seg.close(); err != nil {
		t.Fatalf("close() error = %v", err)
	}
	if mt, err := segmentMTime(seg); err == nil || mt != 0 {
		t.Fatalf("segmentMTime(closed) = (%d, %v), want error", mt, err)
	}
	if err := seg.close(); err != nil {
		t.Fatalf("second close() error = %v", err)
	}
}

// ---- file-poking helpers (test-only) ------------------------------
//
// Tests in this file treat a "log" as a directory of segment files
// under <tmpdir>/p.log/. Tests that originally targeted a single file
// now target the only segment in the directory; the helpers below
// abstract that.

// segmentPaths returns every *.log file inside dir, sorted by
// base-offset (== sorted by filename, given the zero-padded naming).
func segmentPaths(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := parseSegmentFileName(e.Name()); !ok {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	return out
}

// activeSegmentPath returns the path of the highest-baseOffset segment
// file in dir.
func activeSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	paths := segmentPaths(t, dir)
	if len(paths) == 0 {
		t.Fatalf("no segment files in %s", dir)
	}
	return paths[len(paths)-1]
}

// fileSize returns the total size of all segment files in dir. (For
// tests that compare on-disk size between codecs / before-after
// recovery.)
func fileSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	for _, p := range segmentPaths(t, dir) {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		total += st.Size()
	}
	return total
}

// scanFramePositions walks segment files in dir and returns absolute
// frame start positions inside the *first* segment. Used by tests
// that intentionally produce all their frames into one segment (slow
// flush opts + small payloads). Tests that span multiple segments
// should walk segmentPaths themselves.
func scanFramePositions(t *testing.T, dir string) []int64 {
	t.Helper()
	paths := segmentPaths(t, dir)
	if len(paths) == 0 {
		return nil
	}
	f, err := os.Open(paths[0])
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	var out []int64
	pos := int64(0)
	for {
		var hdr [headerSize]byte
		n, err := f.ReadAt(hdr[:], pos)
		if err == io.EOF || n < headerSize {
			return out
		}
		if err != nil {
			t.Fatalf("ReadAt: %v", err)
		}
		h, err := decodeHeader(hdr[:])
		if err != nil {
			t.Fatalf("decodeHeader at %d: %v", pos, err)
		}
		out = append(out, pos)
		pos += int64(headerSize) + int64(h.compressed)
	}
}

// corruptByteAt flips one bit in the *first* segment of dir at the
// given byte offset. (Tests that need to corrupt a different segment
// pass the segment path directly to corruptByteAtPath.)
func corruptByteAt(t *testing.T, dir string, offset int64) {
	t.Helper()
	corruptByteAtPath(t, segmentPaths(t, dir)[0], offset)
}

func corruptByteAtPath(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for corrupt: %v", err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], offset); err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	b[0] ^= 0x01
	if _, err := f.WriteAt(b[:], offset); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
}

// wipeMagic overwrites the 2-byte magic at pos in the first segment
// of dir with zeros.
func wipeMagic(t *testing.T, dir string, pos int64) {
	t.Helper()
	path := segmentPaths(t, dir)[0]
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open for wipe: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteAt([]byte{0, 0}, pos); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
}
