package storage

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

func TestSparseActiveSegmentIndexReadsAcrossAnchors(t *testing.T) {
	path := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
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
	if entries >= total/8 {
		t.Fatalf("index entries = %d, want sparse index well below %d frames", entries, total)
	}

	assertSparseReads(t, l, total)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if got := segmentIndexEntries(t, l2); got != entries {
		t.Fatalf("recovered index entries = %d, want %d", got, entries)
	}
	assertSparseReads(t, l2, total)
}

func TestSparseSealedSegmentIndexLoadsLazily(t *testing.T) {
	path := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 4 << 10

	l, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	const total = 140
	for i := range total {
		appendSingleRecordFrame(t, l, fmt.Appendf(nil, "sealed-%04d-%s", i, bytes.Repeat([]byte("y"), 64)))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	const offsetInFirstSealedSegment = int64(30)
	l2.rwmu.RLock()
	seg := l2.findSegmentForOffsetLocked(offsetInFirstSealedSegment)
	l2.rwmu.RUnlock()
	if seg == nil {
		t.Fatalf("no segment for offset %d", offsetInFirstSealedSegment)
	}

	got, err := l2.Read(offsetInFirstSealedSegment)
	if err != nil {
		t.Fatalf("Read(%d): %v", offsetInFirstSealedSegment, err)
	}
	want := fmt.Appendf(nil, "sealed-%04d-%s", offsetInFirstSealedSegment, bytes.Repeat([]byte("y"), 64))
	if !bytes.Equal(got, want) {
		t.Fatalf("Read(%d) got %q want %q", offsetInFirstSealedSegment, got, want)
	}

	l2.rwmu.RLock()
	idx := l2.segmentIndexes[seg.baseOffset]
	entries := 0
	if idx != nil {
		entries = len(idx.entries)
	}
	indexes := len(l2.segmentIndexes)
	l2.rwmu.RUnlock()
	if idx == nil {
		t.Fatalf("sealed segment %d was not indexed after read", seg.baseOffset)
	}
	if got := entries; got != 1 {
		t.Fatalf("sealed segment index entries = %d, want 1 sparse anchor", got)
	}
	if indexes > maxHotSegmentIndexes {
		t.Fatalf("hot segment indexes = %d, want <= %d", indexes, maxHotSegmentIndexes)
	}
}

func TestSparseIndexScansPastCorruptGap(t *testing.T) {
	path := testLogPath(t)
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 1 << 20

	l, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	for i := range 3 {
		appendSingleRecordFrame(t, l, fmt.Appendf(nil, "frame-%d", i))
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	frames := scanFramePositions(t, path)
	if len(frames) != 3 {
		t.Fatalf("expected 3 frames, got %d", len(frames))
	}
	corruptByteAt(t, path, frames[1]+headerSize+2)

	l2, err := NewLog(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if got := segmentIndexEntries(t, l2); got != 1 {
		t.Fatalf("recovered sparse index entries = %d, want 1 anchor", got)
	}
	if got, err := l2.Read(2); err != nil || !bytes.Equal(got, []byte("frame-2")) {
		t.Fatalf("Read(2) got=%q err=%v", got, err)
	}
	if _, err := l2.Read(1); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(1) want ErrOffsetNotFound got %v", err)
	}
}

func appendSingleRecordFrame(t *testing.T, l *Log, record []byte) {
	t.Helper()
	if _, err := l.Append(record); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.flusher.drainOnce(false, true); err != nil {
		t.Fatalf("drainOnce: %v", err)
	}
}

func segmentIndexEntries(t *testing.T, l *Log) int {
	t.Helper()
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	total := 0
	for _, idx := range l.segmentIndexes {
		total += len(idx.entries)
	}
	return total
}

func assertSparseReads(t *testing.T, l *Log, total int) {
	t.Helper()
	for _, off := range []int64{0, 1, 255, 1024, int64(total - 1)} {
		got, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		want := fmt.Appendf(nil, "record-%04d-%s", off, bytes.Repeat([]byte("x"), 96))
		if !bytes.Equal(got, want) {
			t.Fatalf("Read(%d) got %q want %q", off, got, want)
		}
	}
}

func TestIndexStrideWidensForLargeSegments(t *testing.T) {
	l := &Log{opts: Options{SegmentBytes: int64(targetMaxSegmentIndexEntries+1) * segmentIndexStrideBytes}}
	if got := l.indexStrideBytes(); got <= segmentIndexStrideBytes {
		t.Fatalf("indexStrideBytes() = %d, want wider than %d", got, segmentIndexStrideBytes)
	}

	l = &Log{opts: Options{SegmentBytes: segmentIndexStrideBytes}}
	if got := l.indexStrideBytes(); got != segmentIndexStrideBytes {
		t.Fatalf("indexStrideBytes() = %d, want default %d", got, segmentIndexStrideBytes)
	}
}

func TestSparseIndexHelpersDoNotRequireClock(t *testing.T) {
	l := &Log{opts: Options{SegmentBytes: 1 << 20}}
	entries := []indexEntry{
		{baseOffset: 0, framePos: 0},
		{baseOffset: 1, framePos: int64(segmentIndexStrideBytes) - 1},
		{baseOffset: 2, framePos: int64(segmentIndexStrideBytes)},
		{baseOffset: 3, framePos: int64(2 * segmentIndexStrideBytes)},
	}
	sparse := l.sparseIndexEntries(entries)
	if len(sparse) != 3 {
		t.Fatalf("sparse entries = %d, want 3", len(sparse))
	}
	if sparse[0].baseOffset != 0 || sparse[1].baseOffset != 2 || sparse[2].baseOffset != 3 {
		t.Fatalf("unexpected sparse anchors: %+v", sparse)
	}
}
