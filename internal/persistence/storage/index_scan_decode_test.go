package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/klauspost/compress/zstd"
)

// countingCodec wraps a real codec and counts Decode calls. It reports the
// same Flag as the inner codec so codecForFlag resolves frames back to it on
// read, capturing every decode the storage layer performs.
type countingCodec struct {
	inner   codec.Codec
	decodes atomic.Int64
}

func newCountingZstd(t *testing.T) *countingCodec {
	t.Helper()
	z, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	return &countingCodec{inner: z}
}

func (c *countingCodec) Flag() uint8                   { return c.inner.Flag() }
func (c *countingCodec) Encode(dst, src []byte) []byte { return c.inner.Encode(dst, src) }
func (c *countingCodec) Decode(dst, src []byte, hint int) ([]byte, error) {
	c.decodes.Add(1)
	return c.inner.Decode(dst, src, hint)
}

// Locating an offset between sparse index anchors must walk frame *headers*
// only — never decode their payloads. Before the fix, the anchor scan called
// readFrameAt (full zstd decode) for every frame it stepped over, so a single
// consume read of a deep offset decoded hundreds of frames. This is the
// regression guard: deep reads must cost ~one decode each (the target frame's
// own record read), not one per frame scanned.
func TestIndexScanDoesNotDecodeWhileNavigating(t *testing.T) {
	cc := newCountingZstd(t)
	opts := slowFlushOpts(t, cc)
	opts.SegmentBytes = 8 << 20 // one segment for the whole run

	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	const total = 3000
	for i := range total {
		appendSingleRecordFrame(t, l, fmt.Appendf(nil, "record-%04d-%s", i, bytes.Repeat([]byte("x"), 96)))
	}

	entries := segmentIndexEntries(t, l)
	if entries <= 1 || entries >= total/8 {
		t.Fatalf("index entries = %d, want a sparse index (multiple anchors, well below %d frames)", entries, total)
	}

	// Deep offsets, each far from its anchor, in distinct single-record frames.
	offsets := []int64{255, 800, 1024, 2048, total - 1}
	cc.decodes.Store(0)
	for _, off := range offsets {
		got, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		want := fmt.Appendf(nil, "record-%04d-%s", off, bytes.Repeat([]byte("x"), 96))
		if !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) = %q, want %q", off, got, want)
		}
	}

	// One decode per distinct target frame (the actual record read), and never
	// proportional to the number of frames the navigation stepped over.
	got := cc.decodes.Load()
	if got > int64(len(offsets)) {
		t.Fatalf("decodes during %d deep reads = %d, want <= %d (scan must not decode)",
			len(offsets), got, len(offsets))
	}
}

// Reading across sparse anchors must return correct records under zstd, both
// on the live (active-segment, incrementally indexed) path and after reopen
// (the index is rebuilt by scanning frame headers on recovery). This is the
// zstd analogue of TestSparseActiveSegmentIndexReadsAcrossAnchors, which only
// covered the noop codec and so never exercised the decode-free scan.
func TestSparseIndexReadsAcrossAnchorsZstd(t *testing.T) {
	path := testLogPath(t)
	cc := newCountingZstd(t)
	opts := slowFlushOpts(t, cc)
	opts.SegmentBytes = 8 << 20

	l, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	const total = 3000
	for i := range total {
		appendSingleRecordFrame(t, l, fmt.Appendf(nil, "record-%04d-%s", i, bytes.Repeat([]byte("x"), 96)))
	}
	entries := segmentIndexEntries(t, l)
	if entries <= 1 {
		t.Fatalf("index entries = %d, want multiple sparse anchors", entries)
	}
	assertSparseReads(t, l, total)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen: recovery rebuilds the index by walking frame headers (no decode).
	reopened := newCountingZstd(t)
	opts2 := slowFlushOpts(t, reopened)
	opts2.SegmentBytes = 8 << 20
	l2, err := NewLog(path, opts2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if got := segmentIndexEntries(t, l2); got != entries {
		t.Fatalf("recovered index entries = %d, want %d", got, entries)
	}
	// The index rebuild on reopen must not have decoded frames either.
	if d := reopened.decodes.Load(); d != 0 {
		t.Fatalf("decodes during index rebuild on reopen = %d, want 0", d)
	}
	assertSparseReads(t, l2, total)
}

// Corruption must never be navigated into silently. With a frame's payload
// bit-flipped, the decode-free scan still validates each frame's CRC, so a
// read whose lookup reaches the corrupted frame surfaces an error (or resyncs
// past it) — it never returns wrong bytes — while other offsets stay readable.
func TestIndexScanDetectsCorruptionWhileNavigating(t *testing.T) {
	path := testLogPath(t)
	cc := newCountingZstd(t)
	opts := slowFlushOpts(t, cc)
	opts.SegmentBytes = 8 << 20

	l, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	const total = 1500
	for i := range total {
		appendSingleRecordFrame(t, l, fmt.Appendf(nil, "record-%04d-%s", i, bytes.Repeat([]byte("x"), 96)))
	}

	// Find the on-disk frame position for a mid-segment, non-anchor offset.
	const target = 900
	entry, _, unlock, ok, err := l.indexEntryForRead(target)
	if err != nil || !ok {
		t.Fatalf("indexEntryForRead(%d): ok=%v err=%v", target, ok, err)
	}
	framePos := entry.framePos
	unlock()
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Flip a byte inside that frame's payload (past the 27-byte header).
	segs, _ := filepath.Glob(filepath.Join(path, "*.log"))
	if len(segs) == 0 {
		t.Fatal("no segment file")
	}
	f, err := os.OpenFile(segs[0], os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	corruptAt := framePos + headerSize + 1
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, corruptAt); err != nil {
		t.Fatalf("read byte: %v", err)
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b, corruptAt); err != nil {
		t.Fatalf("write byte: %v", err)
	}
	f.Close()

	reopened := newCountingZstd(t)
	opts2 := slowFlushOpts(t, reopened)
	opts2.SegmentBytes = 8 << 20
	l2, err := NewLog(path, opts2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	// The corrupted offset must not return valid-looking wrong data: either a
	// CRC/corruption error, or not-found if the scan resynced past it.
	want := fmt.Appendf(nil, "record-%04d-%s", target, bytes.Repeat([]byte("x"), 96))
	got, rerr := l2.Read(target)
	if rerr == nil && bytes.Equal(got, want) {
		t.Fatalf("Read(%d) returned the original bytes from a corrupted frame — corruption not detected", target)
	}

	// A clean offset before the corruption is still served correctly.
	clean := int64(100)
	cgot, cerr := l2.Read(clean)
	if cerr != nil {
		t.Fatalf("Read(clean %d): %v", clean, cerr)
	}
	cwant := fmt.Appendf(nil, "record-%04d-%s", clean, bytes.Repeat([]byte("x"), 96))
	if !bytes.Equal(cgot, cwant) {
		t.Fatalf("Read(clean %d) = %q, want %q", clean, cgot, cwant)
	}
}
