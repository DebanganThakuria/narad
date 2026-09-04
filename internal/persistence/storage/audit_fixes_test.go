package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// CommitDurable persists the advanced high-watermark in the same flusher
// pass that fsyncs the records: after it returns, the persisted value
// already covers the batch.
func TestCommitDurablePersistsHighWatermarkBeforeReturning(t *testing.T) {
	dir := testLogPath(t)
	l, err := NewLog(dir, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	for round := 1; round <= 3; round++ {
		first, last, err := l.AppendBatch([][]byte{[]byte("a"), []byte("b")})
		if err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		if err := l.CommitDurable(first, last); err != nil {
			t.Fatalf("CommitDurable: %v", err)
		}
		persisted, err := l.PersistedHighWatermark()
		if err != nil {
			t.Fatalf("PersistedHighWatermark: %v", err)
		}
		if want := last + 1; persisted != want || l.HighWatermark() != want {
			t.Fatalf("round %d: persisted=%d live=%d, want both %d", round, persisted, l.HighWatermark(), want)
		}
	}
	// Nothing left for the interval path: the hwm file already matches.
	if err := l.CommitDurable(5, 4); err != nil {
		t.Fatalf("empty CommitDurable: %v", err)
	}
}

// A CRC mismatch on the read-back surfaces as a VerifyError (so the
// caller classifies it as a verify failure) and leaves the records
// hidden.
func TestCommitDurableReportsVerifyErrorAndKeepsRecordsHidden(t *testing.T) {
	dir := testLogPath(t)
	l, err := NewLog(dir, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	first, last, err := l.AppendBatch([][]byte{[]byte("hello-world-payload")})
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	// Flush to disk without committing, then corrupt the frame's payload.
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	data, err := os.ReadFile(segs[0])
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	idx := bytes.Index(data, []byte("hello-world"))
	if idx < 0 {
		t.Fatal("payload not found on disk")
	}
	data[idx] ^= 0xff
	if err := os.WriteFile(segs[0], data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err = l.CommitDurable(first, last)
	if _, ok := errors.AsType[VerifyError](err); !ok {
		t.Fatalf("CommitDurable error = %v, want VerifyError", err)
	}
	if !IsCorrupt(err) {
		t.Fatalf("VerifyError must unwrap to a corruption error, got %v", err)
	}
	if l.HighWatermark() != 0 {
		t.Fatalf("HighWatermark() = %d after failed verify, want 0", l.HighWatermark())
	}
}

// DisableCommitVerify skips the read-back; the batch still becomes
// visible and durable.
func TestCommitDurableWithoutVerify(t *testing.T) {
	dir := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.DisableCommitVerify = true
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()
	first, last, err := l.AppendBatch([][]byte{[]byte("x")})
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := l.CommitDurable(first, last); err != nil {
		t.Fatalf("CommitDurable: %v", err)
	}
	if l.HighWatermark() != 1 {
		t.Fatalf("HighWatermark() = %d, want 1", l.HighWatermark())
	}
}

// The reusable frame encoder produces byte-identical frames to the
// allocating form, across repeated calls of different sizes and both
// codecs, so buffer reuse cannot leak bytes between frames.
func TestFrameEncoderReuseMatchesFreshEncoding(t *testing.T) {
	zc, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	for _, c := range []codec.Codec{codec.NewNoopCodec(), zc} {
		var enc frameEncoder
		for round := range 6 {
			n := 1 + (round*7)%5
			recs := make([][]byte, n)
			for i := range recs {
				recs[i] = bytes.Repeat([]byte{byte('a' + round), byte(i)}, 10+round*50)
			}
			want, err := encodeFrame(recs, int64(round*100), c)
			if err != nil {
				t.Fatalf("encodeFrame: %v", err)
			}
			got, err := enc.encodeFrame(recs, int64(round*100), c)
			if err != nil {
				t.Fatalf("frameEncoder.encodeFrame: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("codec %T round %d: reused encoder output differs from fresh encoding", c, round)
			}
			// The frame must decode back to the same records.
			h, decoded, _, err := readFrameAt(bytes.NewReader(got), 0, &Log{codec: c})
			if err != nil {
				t.Fatalf("readFrameAt: %v", err)
			}
			if int(h.recordCount) != n {
				t.Fatalf("recordCount = %d, want %d", h.recordCount, n)
			}
			for i := range recs {
				if !bytes.Equal(decoded[i], recs[i]) {
					t.Fatalf("record %d mismatch after decode", i)
				}
			}
		}
	}
}

// The buffered verifier must agree with the allocating one on healthy
// and corrupt frames, including frames larger than its chunk size.
func TestVerifyFrameAtBufferedMatchesUnbuffered(t *testing.T) {
	big := bytes.Repeat([]byte("0123456789abcdef"), verifyChunkBytes/16+3)
	frame, err := encodeFrame([][]byte{big, []byte("tail")}, 7, codec.NewNoopCodec())
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}
	var buf []byte
	h1, end1, err1 := verifyFrameAt(bytes.NewReader(frame), 0)
	h2, end2, err2 := verifyFrameAtBuffered(bytes.NewReader(frame), 0, &buf)
	if err1 != nil || err2 != nil || h1 != h2 || end1 != end2 {
		t.Fatalf("healthy: unbuffered=(%+v,%d,%v) buffered=(%+v,%d,%v)", h1, end1, err1, h2, end2, err2)
	}
	frame[len(frame)-1] ^= 0x01
	_, _, err1 = verifyFrameAt(bytes.NewReader(frame), 0)
	_, _, err2 = verifyFrameAtBuffered(bytes.NewReader(frame), 0, &buf)
	if !IsCorrupt(err1) || !IsCorrupt(err2) {
		t.Fatalf("corrupt: unbuffered=%v buffered=%v, want corruption from both", err1, err2)
	}
	_, _, err2 = verifyFrameAtBuffered(bytes.NewReader(frame[:len(frame)-10]), 0, &buf)
	if err2 == nil {
		t.Fatal("torn frame must fail buffered verification")
	}
}

// consumer.offset is overwritten in place: the file stays 8 bytes, no
// temp files are left behind, values round-trip, and an empty file
// (crash between create and first write) reads as "no offset".
func TestConsumerOffsetInPlaceWrite(t *testing.T) {
	dir := t.TempDir()
	for _, off := range []int64{0, 7, 1 << 40, 3} {
		if err := WriteConsumerOffsetIfPartitionDirExists(dir, off); err != nil {
			t.Fatalf("Write(%d): %v", off, err)
		}
		got, ok, err := ReadConsumerOffset(dir)
		if err != nil || !ok || got != off {
			t.Fatalf("Read after %d = (%d, %v, %v)", off, got, ok, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != consumerOffsetFileName {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("partition dir entries = %v, want only %s", names, consumerOffsetFileName)
	}
	info, _ := os.Stat(filepath.Join(dir, consumerOffsetFileName))
	if info.Size() != 8 {
		t.Fatalf("file size = %d, want 8", info.Size())
	}

	if err := os.WriteFile(filepath.Join(dir, consumerOffsetFileName), nil, 0o644); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if _, ok, err := ReadConsumerOffset(dir); err != nil || ok {
		t.Fatalf("empty file read = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	if err := WriteConsumerOffsetIfPartitionDirExists(dir, 9); err != nil {
		t.Fatalf("Write over empty file: %v", err)
	}
	if got, ok, _ := ReadConsumerOffset(dir); !ok || got != 9 {
		t.Fatalf("Read after rewrite = (%d, %v), want (9, true)", got, ok)
	}

	missing := filepath.Join(dir, "gone")
	if err := WriteConsumerOffsetIfPartitionDirExists(missing, 1); !errors.Is(err, ErrPartitionDirMissing) {
		t.Fatalf("missing dir error = %v, want ErrPartitionDirMissing", err)
	}
}

// Frames written with WriteAt at the tracked size stay contiguous and
// readable across a segment roll, and a reopen recovers every record.
func TestPositionalFrameWritesStayContiguousAcrossRoll(t *testing.T) {
	dir := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 512
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	const n = 40
	for i := range n {
		if _, err := l.Append(fmt.Appendf(nil, "record-%03d-%s", i, bytes.Repeat([]byte("p"), 40))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := l.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	if l.SegmentCount() < 2 {
		t.Fatalf("expected a roll, got %d segments", l.SegmentCount())
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l, err = NewLog(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	if l.NextOffset() != n {
		t.Fatalf("NextOffset after reopen = %d, want %d", l.NextOffset(), n)
	}
	for i := range n {
		got, err := l.Read(int64(i))
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if want := fmt.Sprintf("record-%03d-", i); !bytes.HasPrefix(got, []byte(want)) {
			t.Fatalf("Read %d = %q, want prefix %q", i, got, want)
		}
	}
}

// ReadShared hands out the cached record without copying; Read copies.
// Both agree on content, and after retention deletes the segment a read
// of a reaped offset reports ErrOffsetNotFound instead of a closed-file
// error, even though the read runs outside the log lock.
func TestReadSharedAliasesCacheAndSurvivesRetention(t *testing.T) {
	dir := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 256
	// Age-based retention that only fires when the test sweeps by hand:
	// every sealed segment is "old" but the reaper's own tick is an hour.
	opts.Retention = RetentionConfig{MaxAge: time.Nanosecond, CheckInterval: time.Hour, Now: func() time.Time { return time.Now().Add(time.Hour) }}
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	const n = 30
	for i := range n {
		if _, err := l.Append(fmt.Appendf(nil, "rec-%03d-%s", i, bytes.Repeat([]byte("x"), 40))); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := l.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	shared, err := l.ReadShared(3)
	if err != nil {
		t.Fatalf("ReadShared: %v", err)
	}
	copied, err := l.Read(3)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(shared, copied) {
		t.Fatalf("ReadShared = %q, Read = %q", shared, copied)
	}
	again, _ := l.ReadShared(3)
	if &again[0] != &shared[0] {
		t.Fatal("ReadShared must return the cached slice, not a copy")
	}
	if &copied[0] == &shared[0] {
		t.Fatal("Read must return a private copy")
	}

	// Reap every sealed segment: reads of reaped offsets are gone, not
	// broken, and reads of retained offsets still work.
	if l.SegmentCount() < 3 {
		t.Fatalf("expected rolls, got %d segments", l.SegmentCount())
	}
	l.reaper.sweep()
	oldest := l.OldestOffset()
	if oldest == 0 {
		t.Fatal("retention did not delete any segment")
	}
	if _, err := l.ReadShared(0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("read of reaped offset = %v, want ErrOffsetNotFound", err)
	}
	if _, err := l.ReadShared(oldest); err != nil {
		t.Fatalf("read of oldest retained offset %d: %v", oldest, err)
	}
	if _, err := l.ReadShared(n - 1); err != nil {
		t.Fatalf("read of newest offset: %v", err)
	}
}

// Reads race retention deleting segments underneath them without
// panicking or returning wrong data: every successful read returns the
// record that was written at that offset.
func TestReadSharedConcurrentWithRetention(t *testing.T) {
	dir := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 200
	opts.Retention = RetentionConfig{MaxAge: time.Nanosecond, CheckInterval: time.Hour, Now: func() time.Time { return time.Now().Add(time.Hour) }}
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()
	const n = 200
	for i := range n {
		if _, err := l.Append(fmt.Appendf(nil, "r%04d%s", i, bytes.Repeat([]byte("y"), 30))); err != nil {
			t.Fatalf("Append: %v", err)
		}
		if i%5 == 4 {
			if err := l.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}
		}
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				off := int64(rand.IntN(n))
				rec, err := l.ReadShared(off)
				if err != nil {
					if !errors.Is(err, ErrOffsetNotFound) && !errors.Is(err, os.ErrClosed) && !IsCorrupt(err) {
						t.Errorf("ReadShared(%d): unexpected error %v", off, err)
					}
					continue
				}
				if want := fmt.Sprintf("r%04d", off); !bytes.HasPrefix(rec, []byte(want)) {
					t.Errorf("ReadShared(%d) = %q, want prefix %q", off, rec, want)
				}
			}
		})
	}
	for range 20 {
		l.reaper.sweep()
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
}
