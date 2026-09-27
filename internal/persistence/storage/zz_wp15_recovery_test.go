package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// wp15Layout is a partition written straight to segment files, the way
// rolls leave one: every segment holds whole frames and each segment
// after the first is named by the offset its predecessor ended at.
type wp15Layout struct {
	dir    string
	paths  []string  // segment files in base-offset order
	bases  []int64   // base offset of each segment
	frames [][]int64 // frame start positions, per segment
	firsts [][]int64 // first offset of each frame, per segment
	per    int       // records per frame
	next   int64     // the offset after the last record
}

// wp15Committed is the commit time stamped in every layout record.
const wp15Committed = int64(1790424109845)

// wp15Record is the keyed record the layout stores at off: its payload
// names the offset, so a read that returns another offset's bytes shows.
func wp15Record(off int64, size int) []byte {
	p := make([]byte, size)
	for i := range p {
		p[i] = byte('a' + (off+int64(i))%26)
	}
	copy(p, fmt.Sprintf("rec-%d-", off))
	return EncodeKeyedRecord(fmt.Sprintf("k%d", off), wp15Committed, p)
}

// wp15WriteLayout writes sealed segments of framesPerSeg frames of per
// records of recSize bytes each, then an active segment holding
// activeFrames frames. No fsync: the files only feed opens in this
// process.
func wp15WriteLayout(tb testing.TB, dir string, sealed, framesPerSeg, activeFrames, per, recSize int) wp15Layout {
	tb.Helper()
	if err := os.MkdirAll(dir, dataDirMode); err != nil {
		tb.Fatal(err)
	}
	lay := wp15Layout{dir: dir, per: per}
	var off int64
	var buf bytes.Buffer
	for s := 0; s <= sealed; s++ {
		n := framesPerSeg
		if s == sealed {
			n = activeFrames
		}
		base := off
		buf.Reset()
		var positions, firsts []int64
		for range n {
			recs := make([][]byte, per)
			for i := range recs {
				recs[i] = wp15Record(off+int64(i), recSize)
			}
			frame, err := encodeFrame(recs, off, codec.NewNoopCodec())
			if err != nil {
				tb.Fatal(err)
			}
			positions = append(positions, int64(buf.Len()))
			firsts = append(firsts, off)
			buf.Write(frame)
			off += int64(per)
		}
		path := filepath.Join(dir, segmentFileName(base))
		if err := os.WriteFile(path, buf.Bytes(), dataFileMode); err != nil {
			tb.Fatal(err)
		}
		lay.paths = append(lay.paths, path)
		lay.bases = append(lay.bases, base)
		lay.frames = append(lay.frames, positions)
		lay.firsts = append(lay.firsts, firsts)
	}
	lay.next = off
	return lay
}

// wp15Open opens the layout's partition and closes it with the test.
func wp15Open(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := NewLog(dir, Options{})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// wp15ExpectRecord fails unless offset reads back as the layout wrote it,
// through the copying and the zero-copy read alike.
func wp15ExpectRecord(t *testing.T, l *Log, off int64, recSize int) {
	t.Helper()
	want := wp15Record(off, recSize)
	got, err := l.Read(off)
	if err != nil {
		t.Fatalf("Read(%d): %v", off, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Read(%d) = %.40q, want %.40q", off, got, want)
	}
	key, committed, payload, err := l.ReadKeyedShared(off)
	if err != nil {
		t.Fatalf("ReadKeyedShared(%d): %v", off, err)
	}
	wantKey, wantCommitted, wantPayload, _ := DecodeKeyedRecord(want)
	if key != wantKey || committed != wantCommitted || !bytes.Equal(payload, wantPayload) {
		t.Fatalf("ReadKeyedShared(%d) = (%q, %d, %.40q)", off, key, committed, payload)
	}
}

// wp15ExpectUnreadable fails unless offset is refused the way the read
// path refuses a record it cannot vouch for: corrupt or not found, which
// the consume path skips with its corrupt-skipped counter. It must never
// hand out bytes, from either read, on any attempt (a failed read must
// not leave something cached for the next one).
func wp15ExpectUnreadable(t *testing.T, l *Log, off int64) {
	t.Helper()
	for attempt := range 2 {
		if got, err := l.Read(off); err == nil {
			t.Fatalf("attempt %d: Read(%d) served %.40q from a damaged frame", attempt, off, got)
		} else if !IsCorrupt(err) && !errors.Is(err, ErrOffsetNotFound) {
			t.Fatalf("attempt %d: Read(%d) = %v, want corrupt or not found", attempt, off, err)
		}
		if _, _, got, err := l.ReadKeyedShared(off); err == nil {
			t.Fatalf("attempt %d: ReadKeyedShared(%d) served %.40q from a damaged frame", attempt, off, got)
		} else if !IsCorrupt(err) && !errors.Is(err, ErrOffsetNotFound) {
			t.Fatalf("attempt %d: ReadKeyedShared(%d) = %v, want corrupt or not found", attempt, off, err)
		}
	}
}

// wp15FileSizes returns the size of every segment file of the layout.
func wp15FileSizes(t *testing.T, lay wp15Layout) []int64 {
	t.Helper()
	sizes := make([]int64, len(lay.paths))
	for i, p := range lay.paths {
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		sizes[i] = st.Size()
	}
	return sizes
}

// wp15AppendAfterReopen commits one record on the reopened log and
// checks it takes offset want and reads back.
func wp15AppendAfterReopen(t *testing.T, l *Log, want int64) {
	t.Helper()
	rec := []byte("after-reopen")
	first, last := wp3CommitBatch(t, l, [][]byte{rec})
	if first != want || last != want {
		t.Fatalf("append after reopen took [%d,%d], want offset %d", first, last, want)
	}
	got, err := l.Read(want)
	if err != nil || !bytes.Equal(got, rec) {
		t.Fatalf("Read(%d) after reopen = %q, %v", want, got, err)
	}
}

// Opening a partition must not read the payload of its sealed segments:
// their bounds come from the segment names, and every record is still
// CRC-checked when it is read. Before, recovery read and CRC-checked
// every payload byte of every sealed segment, into a frame-sized buffer
// per frame, so an open cost O(retained bytes) of I/O and allocation.
// The allocation is what this test measures, because it is exact where
// time is not.
func TestWP15OpenDoesNotReadSealedPayloads(t *testing.T) {
	const (
		sealed       = 8
		framesPerSeg = 256
		per          = 64
		recSize      = 256
	)
	dir := filepath.Join(t.TempDir(), "p0")
	lay := wp15WriteLayout(t, dir, sealed, framesPerSeg, 2, per, recSize)
	var sealedBytes int64
	for _, size := range wp15FileSizes(t, lay)[:sealed] {
		sealedBytes += size
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	l, err := NewLog(dir, Options{})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	allocated := int64(after.TotalAlloc - before.TotalAlloc)
	if allocated > sealedBytes/8 {
		t.Fatalf("NewLog allocated %d bytes over %d bytes of sealed segments: recovery is reading sealed payloads", allocated, sealedBytes)
	}
	t.Logf("NewLog allocated %d bytes over %d bytes of sealed segments", allocated, sealedBytes)

	if got := l.NextOffset(); got != lay.next {
		t.Fatalf("NextOffset = %d, want %d", got, lay.next)
	}
	if got := l.HighWatermark(); got != lay.next {
		t.Fatalf("HighWatermark = %d, want %d", got, lay.next)
	}
	for i, seg := range l.segments[:sealed] {
		if seg.nextOffset != lay.bases[i+1] {
			t.Fatalf("sealed segment %d ends at %d, want its successor's base %d", i, seg.nextOffset, lay.bases[i+1])
		}
	}
	// Every sealed record is still reachable: the first and last of every
	// segment, and a stride through the rest.
	for s := range sealed {
		wp15ExpectRecord(t, l, lay.bases[s], recSize)
		wp15ExpectRecord(t, l, lay.bases[s+1]-1, recSize)
	}
	for off := int64(0); off < lay.next; off += 997 {
		wp15ExpectRecord(t, l, off, recSize)
	}
	wp15AppendAfterReopen(t, l, lay.next)
}

// The active segment is still read and CRC-checked in full at open, but
// through one reused buffer: reading each frame whole allocated the
// segment's size in garbage per open, on the path every restart and every
// reopen of an idle partition takes.
func TestWP15ActiveWalkStreamsFrames(t *testing.T) {
	const per, recSize = 64, 256
	dir := filepath.Join(t.TempDir(), "p0")
	frameBytes := per * (4 + len(wp15Record(0, recSize)))
	lay := wp15WriteLayout(t, dir, 0, 0, (16<<20)/frameBytes, per, recSize)
	activeBytes := wp15FileSizes(t, lay)[0]

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	l, err := NewLog(dir, Options{})
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	allocated := int64(after.TotalAlloc - before.TotalAlloc)
	if allocated > activeBytes/8 {
		t.Fatalf("NewLog allocated %d bytes to walk a %d-byte active segment: recovery reads each frame into its own buffer", allocated, activeBytes)
	}
	t.Logf("NewLog allocated %d bytes to walk a %d-byte active segment", allocated, activeBytes)
	if got := l.NextOffset(); got != lay.next {
		t.Fatalf("NextOffset = %d, want %d", got, lay.next)
	}
	for off := int64(0); off < lay.next; off += 331 {
		wp15ExpectRecord(t, l, off, recSize)
	}
	wp15AppendAfterReopen(t, l, lay.next)
}

// A torn tail in a sealed segment (the last sync before the roll lied,
// and the segment the roll created survived) is left on disk, bounds
// nothing, and is never served. The segment's range ends where its
// successor starts, the roll's own record of how far it got, so the
// lost offsets read as unreadable instead of belonging to no segment;
// either way the consume path skips them as recorded loss.
func TestWP15TornSealedTailIsBoundedBySuccessor(t *testing.T) {
	const per, recSize = 4, 64
	dir := filepath.Join(t.TempDir(), "p0")
	lay := wp15WriteLayout(t, dir, 3, 5, 2, per, recSize)

	// Cut segment 1 inside its last frame.
	lastFrame := lay.frames[1][len(lay.frames[1])-1]
	if err := os.Truncate(lay.paths[1], lastFrame+headerSize+10); err != nil {
		t.Fatal(err)
	}
	sizes := wp15FileSizes(t, lay)

	l := wp15Open(t, dir)
	if got := wp15FileSizes(t, lay); !slices.Equal(got, sizes) {
		t.Fatalf("recovery changed segment sizes %v -> %v; a sealed segment is never truncated", sizes, got)
	}
	if got := l.segments[1].nextOffset; got != lay.bases[2] {
		t.Fatalf("torn sealed segment ends at %d, want its successor's base %d", got, lay.bases[2])
	}
	if got := l.NextOffset(); got != lay.next {
		t.Fatalf("NextOffset = %d, want %d (the torn sealed tail must not move the log tail)", got, lay.next)
	}

	tornFirst := lay.firsts[1][len(lay.firsts[1])-1]
	for off := int64(0); off < lay.next; off++ {
		if off >= tornFirst && off < tornFirst+per {
			wp15ExpectUnreadable(t, l, off)
			continue
		}
		wp15ExpectRecord(t, l, off, recSize)
	}
	wp15AppendAfterReopen(t, l, lay.next)
}

// A torn tail in the active segment is still cut at the last whole frame
// and the cut fsynced, exactly as before: the next commit reuses the
// offsets of the torn frame. The sealed segments before it are not
// touched. Both shapes of tear: a short last frame, and a last frame
// whose bytes are all there but zero-filled.
func TestWP15TornActiveTailStillTruncated(t *testing.T) {
	const per, recSize = 4, 64
	for _, tc := range []struct {
		name string
		tear func(t *testing.T, path string, frame, end int64)
	}{
		{"short", func(t *testing.T, path string, frame, _ int64) {
			if err := os.Truncate(path, frame+headerSize+7); err != nil {
				t.Fatal(err)
			}
		}},
		{"zero-filled", func(t *testing.T, path string, frame, end int64) {
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := f.WriteAt(make([]byte, end-frame-headerSize), frame+headerSize); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			lay := wp15WriteLayout(t, dir, 2, 3, 3, per, recSize)
			active := len(lay.paths) - 1
			sizes := wp15FileSizes(t, lay)
			lastFrame := lay.frames[active][len(lay.frames[active])-1]
			tc.tear(t, lay.paths[active], lastFrame, sizes[active])

			l := wp15Open(t, dir)
			got := wp15FileSizes(t, lay)
			if !slices.Equal(got[:active], sizes[:active]) {
				t.Fatalf("sealed segment sizes %v -> %v", sizes[:active], got[:active])
			}
			if got[active] != lastFrame {
				t.Fatalf("active segment is %d bytes after recovery, want it cut at the torn frame (%d)", got[active], lastFrame)
			}
			torn := lay.firsts[active][len(lay.firsts[active])-1]
			if next := l.NextOffset(); next != torn {
				t.Fatalf("NextOffset = %d, want %d (the torn frame's offsets are free again)", next, torn)
			}
			for off := range torn {
				wp15ExpectRecord(t, l, off, recSize)
			}
			wp15AppendAfterReopen(t, l, torn)
		})
	}
}

// A payload that fails its CRC in the middle of a sealed segment is never
// delivered: the read CRC-checks the frame it lands on and reports it
// corrupt, which the consume path skips with its corrupt-skipped counter.
// That holds whether the bad frame sits mid-segment or last in it, and
// whatever recovery made of it: recovery no longer reads sealed payloads,
// so the read is the check. The files are not modified, and every other
// record stays readable.
func TestWP15CorruptSealedPayloadNeverServed(t *testing.T) {
	const per, recSize = 4, 64
	for _, tc := range []struct {
		name  string
		frame func(lay wp15Layout) int
	}{
		{"middle-frame", func(lay wp15Layout) int { return 2 }},
		{"last-frame", func(lay wp15Layout) int { return len(lay.frames[1]) - 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			lay := wp15WriteLayout(t, dir, 3, 5, 2, per, recSize)
			bad := tc.frame(lay)
			corruptByteAtPath(t, lay.paths[1], lay.frames[1][bad]+headerSize+20)
			sizes := wp15FileSizes(t, lay)

			l := wp15Open(t, dir)
			if got := wp15FileSizes(t, lay); !slices.Equal(got, sizes) {
				t.Fatalf("recovery changed segment sizes %v -> %v", sizes, got)
			}
			if got := l.NextOffset(); got != lay.next {
				t.Fatalf("NextOffset = %d, want %d", got, lay.next)
			}
			badFirst := lay.firsts[1][bad]
			for off := int64(0); off < lay.next; off++ {
				if off >= badFirst && off < badFirst+per {
					wp15ExpectUnreadable(t, l, off)
					continue
				}
				wp15ExpectRecord(t, l, off, recSize)
			}
			if err := l.VerifyDurable(badFirst, badFirst+per-1); err == nil {
				t.Fatal("VerifyDurable passed over a frame whose payload fails its CRC")
			}
			wp15AppendAfterReopen(t, l, lay.next)
		})
	}
}

// A damaged frame header in a sealed segment (a wiped magic, a length
// field that runs past the end of the file, or one that lands inside the
// next frame) costs that frame's records and nothing else: navigation
// resynchronises on the next frame magic, as it did when recovery walked
// the segment too.
func TestWP15BadHeaderInSealedSegment(t *testing.T) {
	const per, recSize = 4, 64
	for _, tc := range []struct {
		name string
		hurt func(t *testing.T, path string, pos int64)
	}{
		{"wiped-magic", func(t *testing.T, path string, pos int64) {
			wp15WriteAt(t, path, pos, []byte{0, 0})
		}},
		{"length-past-eof", func(t *testing.T, path string, pos int64) {
			wp15WriteAt(t, path, pos+19, []byte{0, 0x10, 0, 0})
		}},
		{"length-into-next-frame", func(t *testing.T, path string, pos int64) {
			wp15WriteAt(t, path, pos+19, []byte{0, 0, 0, 40})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			lay := wp15WriteLayout(t, dir, 3, 5, 2, per, recSize)
			const bad = 2
			tc.hurt(t, lay.paths[1], lay.frames[1][bad])

			l := wp15Open(t, dir)
			if got := l.NextOffset(); got != lay.next {
				t.Fatalf("NextOffset = %d, want %d", got, lay.next)
			}
			badFirst := lay.firsts[1][bad]
			for off := int64(0); off < lay.next; off++ {
				if off >= badFirst && off < badFirst+per {
					wp15ExpectUnreadable(t, l, off)
					continue
				}
				wp15ExpectRecord(t, l, off, recSize)
			}
		})
	}
}

// For segments the flusher rolled, the end recovery now takes from the
// successor's name is exactly where a full CRC walk of the segment ends,
// the bound recovery used to compute by reading it.
func TestWP15SealedBoundsMatchAFullWalk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	opts := Options{SegmentBytes: 8 << 10}
	wp3NoFsync(t)
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 120 {
		wp3CommitBatch(t, l, wp3Records(1+i%7, 300))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2 := wp15Open(t, dir)
	if len(l2.segments) < 4 {
		t.Fatalf("want several sealed segments, got %d segments", len(l2.segments))
	}
	for i, seg := range l2.segments[:len(l2.segments)-1] {
		walked := wp15WalkedEnd(t, seg.path, seg.baseOffset)
		if seg.nextOffset != walked {
			t.Fatalf("sealed segment %d: recovered end %d, a full CRC walk ends at %d", i, seg.nextOffset, walked)
		}
	}
}

// wp15WalkedEnd CRC-walks a segment file and returns the offset after
// its last valid frame (its base when it holds none).
func wp15WalkedEnd(t *testing.T, path string, base int64) int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	end := base
	for pos := int64(0); pos < st.Size(); {
		h, next, err := verifyFrameAt(f, pos)
		if err != nil {
			t.Fatalf("%s@%d: %v", filepath.Base(path), pos, err)
		}
		end = h.baseOffset + int64(h.recordCount)
		pos = next
	}
	return end
}

func wp15WriteAt(t *testing.T, path string, pos int64, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteAt(b, pos); err != nil {
		t.Fatal(err)
	}
}
