package storage

// The transfer primitives underpin partition rebalance: a partition's
// segments must copy to a new node byte-for-byte and recover into a
// log with identical offsets, high-watermark, and readable records.
// This is the durability audit of the copy path, in isolation.

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// buildMultiSegmentLog writes n records with a tiny segment size so
// several segments roll, then closes durably. Returns the dir + the
// final next offset + a map offset→payload for verification.
func buildMultiSegmentLog(t *testing.T, dir string, n int) (int64, map[int64][]byte) {
	t.Helper()
	log, err := NewLog(dir, Options{FlushInterval: time.Millisecond, SegmentBytes: 1})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	payloads := map[int64][]byte{}
	for i := range n {
		p := []byte{byte('a' + i%26), byte('0' + i%10)}
		off, err := log.Append(EncodeKeyedRecord("k", int64(i), p))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := log.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
		payloads[off] = p
	}
	next := log.NextOffset()
	if err := log.AdvanceHighWatermark(next); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return next, payloads
}

func TestListPartitionSegmentsMarksActiveUnsealed(t *testing.T) {
	dir := t.TempDir()
	buildMultiSegmentLog(t, dir, 5) // SegmentBytes:1 => one segment per record

	segs, err := ListPartitionSegments(dir)
	if err != nil {
		t.Fatalf("ListPartitionSegments: %v", err)
	}
	if len(segs) < 2 {
		t.Fatalf("want multiple segments, got %d", len(segs))
	}
	// Base offsets must be strictly increasing; exactly the last is active.
	for i := 1; i < len(segs); i++ {
		if segs[i].BaseOffset <= segs[i-1].BaseOffset {
			t.Fatalf("segments not offset-ordered: %+v", segs)
		}
	}
	for i, s := range segs {
		wantSealed := i < len(segs)-1
		if s.Sealed != wantSealed {
			t.Fatalf("segment %d sealed=%v, want %v", i, s.Sealed, wantSealed)
		}
		// Sealed segments always hold data; the active tail may be empty
		// (a fresh roll leaves a 0-byte active segment).
		if s.Sealed && s.SizeBytes <= 0 {
			t.Fatalf("sealed segment %d has non-positive size %d", i, s.SizeBytes)
		}
		if s.SizeBytes < 0 {
			t.Fatalf("segment %d has negative size %d", i, s.SizeBytes)
		}
	}
}

// The crown check: copy every segment byte-for-byte to a fresh dir via
// the transfer primitives, recover it, and confirm the copy is
// identical — same next offset, same HWM, same records at every offset.
func TestPartitionCopyRoundTripsIdentically(t *testing.T) {
	src := t.TempDir()
	wantNext, payloads := buildMultiSegmentLog(t, src, 12)

	segs, err := ListPartitionSegments(src)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// Ship each segment in bounded chunks (exercises ReadSegmentRange +
	// WriteSegmentFile the way the RPC transport will).
	dst := t.TempDir()
	const chunk = 7
	for _, s := range segs {
		for at := int64(0); at < s.SizeBytes; at += chunk {
			data, err := ReadSegmentRange(src, s.BaseOffset, at, chunk)
			if err != nil {
				t.Fatalf("ReadSegmentRange: %v", err)
			}
			if len(data) == 0 {
				break
			}
			if at == 0 {
				if err := WriteSegmentFile(dst, s.BaseOffset, data); err != nil {
					t.Fatalf("WriteSegmentFile: %v", err)
				}
			} else if err := AppendToSegmentFile(dst, s.BaseOffset, data); err != nil {
				t.Fatalf("AppendToSegmentFile: %v", err)
			}
		}
	}
	// Also copy the durable HWM file so visibility matches — in the real
	// transport it is one more file shipped from the partition dir.
	if data, err := os.ReadFile(hwmFilePath(src)); err == nil {
		if err := os.WriteFile(hwmFilePath(dst), data, 0o644); err != nil {
			t.Fatalf("copy hwm file: %v", err)
		}
	}

	// Recover the copy and audit it against the source.
	copyLog, err := NewLog(dst, Options{FlushInterval: time.Millisecond})
	if err != nil {
		t.Fatalf("recover copy: %v", err)
	}
	defer copyLog.Close()

	if copyLog.NextOffset() != wantNext {
		t.Fatalf("copy NextOffset = %d, want %d", copyLog.NextOffset(), wantNext)
	}
	if copyLog.HighWatermark() != wantNext {
		t.Fatalf("copy HWM = %d, want %d", copyLog.HighWatermark(), wantNext)
	}
	for off, want := range payloads {
		_, _, got, err := copyLog.ReadKeyed(off)
		if err != nil {
			t.Fatalf("copy ReadKeyed(%d): %v", off, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("copy offset %d = %q, want %q", off, got, want)
		}
	}
}

// The read length comes from the wire on OpFetchSegmentChunk. It must
// never size an allocation by itself: clamp to MaxSegmentReadBytes and
// to what the file holds past the offset, refuse negatives, and answer
// a read at or past EOF with no bytes.
func TestReadSegmentRangeBoundsAllocation(t *testing.T) {
	dir := t.TempDir()
	const size = 5 << 20
	if err := WriteSegmentFile(dir, 0, bytes.Repeat([]byte{7}, size)); err != nil {
		t.Fatal(err)
	}

	// A 1 TiB request is served the cap, not a 1 TiB buffer (the test
	// would be OOM-killed otherwise).
	data, err := ReadSegmentRange(dir, 0, 0, 1<<40)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != MaxSegmentReadBytes {
		t.Fatalf("len = %d, want cap %d", len(data), MaxSegmentReadBytes)
	}
	// Past the cap, the file size bounds the read.
	if data, err = ReadSegmentRange(dir, 0, size-100, 1<<40); err != nil || len(data) != 100 {
		t.Fatalf("tail read: len=%d err=%v, want 100", len(data), err)
	}
	if data, err = ReadSegmentRange(dir, 0, size, 1<<20); err != nil || len(data) != 0 {
		t.Fatalf("read at EOF: len=%d err=%v, want 0 bytes and no error", len(data), err)
	}
	if data, err = ReadSegmentRange(dir, 0, size+10, 1<<20); err != nil || len(data) != 0 {
		t.Fatalf("read past EOF: len=%d err=%v, want 0 bytes and no error", len(data), err)
	}
	if _, err = ReadSegmentRange(dir, 0, -1, 10); err == nil {
		t.Fatal("negative offset accepted")
	}
	if _, err = ReadSegmentRange(dir, 0, 0, -1); err == nil {
		t.Fatal("negative length accepted")
	}
}

// logWithHiddenFrames writes n committed records (one frame each) to a
// log in dir, then m records that are written and fsynced but never
// made visible.
func logWithHiddenFrames(t *testing.T, dir string, n, m int, opts Options) *Log {
	t.Helper()
	opts.FlushInterval = time.Millisecond
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if _, err := l.Append(EncodeKeyedRecord("k", int64(i), []byte("committed"))); err != nil {
			t.Fatal(err)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.AdvanceHighWatermark(l.NextOffset()); err != nil {
		t.Fatal(err)
	}
	for i := range m {
		if _, err := l.Append(EncodeKeyedRecord("k", int64(i), []byte("HIDDEN"))); err != nil {
			t.Fatal(err)
		}
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	return l
}

// The committed boundary is the first frame at or above the high
// watermark, the same from the open log and from its closed files, and
// never past the log's own high watermark.
func TestCommittedBoundaryExcludesAnUncommittedTail(t *testing.T) {
	dir := t.TempDir()
	l := logWithHiddenFrames(t, dir, 5, 3, Options{})
	st, err := os.Stat(filepath.Join(dir, segmentFileName(0)))
	if err != nil {
		t.Fatal(err)
	}
	open, ok := l.CommittedBoundary(0, 5)
	if !ok || open <= 0 || open >= st.Size() {
		t.Fatalf("open boundary %d (ok %v), file %d bytes: want strictly inside (5 committed, 3 hidden frames)", open, ok, st.Size())
	}
	if beyond, _ := l.CommittedBoundary(0, 100); beyond != open {
		t.Fatalf("boundary asked past the high watermark = %d, want the committed %d", beyond, open)
	}
	if _, ok := l.CommittedBoundary(99, 5); ok {
		t.Fatal("a boundary reported for a segment the log does not have")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := l.CommittedBoundary(0, 5); ok {
		t.Fatal("a closed log reported a boundary")
	}
	closed, err := CommittedSegmentBytes(dir, 0, 5)
	if err != nil || closed != open {
		t.Fatalf("closed-file boundary %d (err %v), open %d", closed, err, open)
	}
	if all, _ := CommittedSegmentBytes(dir, 0, 100); all != st.Size() {
		t.Fatalf("boundary past every frame = %d, want the file size %d", all, st.Size())
	}
	if none, _ := CommittedSegmentBytes(dir, 0, 0); none != 0 {
		t.Fatalf("boundary at hwm 0 = %d, want 0", none)
	}
}

// A torn last frame ends the closed-file walk before it, and a frame
// header that does not decode below the high watermark leaves the file
// as it is (recovery steps over such damage, so the walk cannot place
// the boundary).
func TestCommittedSegmentBytesStopsAtATornFrame(t *testing.T) {
	dir := t.TempDir()
	l := logWithHiddenFrames(t, dir, 4, 0, Options{})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, segmentFileName(0))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	third, err := CommittedSegmentBytes(dir, 0, 3)
	if err != nil || third <= 0 || third >= int64(len(data)) {
		t.Fatalf("boundary at offset 3 = %d (err %v), file %d", third, err, len(data))
	}
	if err := os.WriteFile(path, data[:len(data)-2], 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := CommittedSegmentBytes(dir, 0, 10); err != nil || got != third {
		t.Fatalf("torn last frame: boundary %d (err %v), want %d (before the torn frame)", got, err, third)
	}
	damaged := append([]byte(nil), data...)
	damaged[third] ^= 0xff // the magic of the 4th frame
	if err := os.WriteFile(path, damaged, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := CommittedSegmentBytes(dir, 0, 10); err != nil || got != int64(len(damaged)) {
		t.Fatalf("damaged header: boundary %d (err %v), want the file size %d", got, err, len(damaged))
	}
}

// A listing for a copy at a high watermark leaves out the segments that
// start above it and cuts the last one left to its committed records.
func TestListCommittedSegmentsEndsAtTheHighWatermark(t *testing.T) {
	dir := t.TempDir()
	buildMultiSegmentLog(t, dir, 6) // one record per segment, hwm 6
	segs, err := ListCommittedSegments(dir, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 4 || segs[3].BaseOffset != 3 || segs[3].SizeBytes != 0 || segs[3].Sealed {
		t.Fatalf("listing at hwm 3 = %+v; want segments 0..3 with segment 3 empty and unsealed", segs)
	}
	for _, s := range segs[:3] {
		if s.SizeBytes == 0 || !s.Sealed {
			t.Fatalf("listing at hwm 3 = %+v; want segments 0..2 whole and sealed", segs)
		}
	}
	all, err := ListCommittedSegments(dir, 6, func(int64) (int64, bool) { return 0, false })
	if err != nil {
		t.Fatal(err)
	}
	plain, _ := ListPartitionSegments(dir)
	if len(all) != len(plain) || all[len(all)-1].SizeBytes != plain[len(plain)-1].SizeBytes {
		t.Fatalf("listing at the log's high watermark = %+v, want %+v", all, plain)
	}
}

// A staged copy with records past the high watermark it is promoted at
// is cut back to it: segment files above it go, and the last one is
// truncated before its first frame at or above it.
func TestCutStagedCopyDropsRecordsPastTheHighWatermark(t *testing.T) {
	dir := t.TempDir()
	l := logWithHiddenFrames(t, dir, 5, 3, Options{})
	committed, _ := l.CommittedBoundary(0, 5)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WriteSegmentFile(dir, 9, []byte("stray")); err != nil {
		t.Fatal(err)
	}
	cut, err := CutStagedCopy(dir, 5)
	if err != nil || !cut {
		t.Fatalf("cut = %v (err %v), want a cut", cut, err)
	}
	segs, _ := ListPartitionSegments(dir)
	if len(segs) != 1 || segs[0].SizeBytes != committed {
		t.Fatalf("after the cut %+v, want one segment of %d bytes", segs, committed)
	}
	if again, err := CutStagedCopy(dir, 5); err != nil || again {
		t.Fatalf("a second cut = %v (err %v), want nothing left to cut", again, err)
	}
	reopened, err := NewLog(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.NextOffset() != 5 {
		t.Fatalf("the cut copy recovers next offset %d, want 5", reopened.NextOffset())
	}
}
