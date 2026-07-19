package storage

// The transfer primitives underpin partition rebalance: a partition's
// segments must copy to a new node byte-for-byte and recover into a
// log with identical offsets, high-watermark, and readable records.
// This is the durability audit of the copy path, in isolation.

import (
	"bytes"
	"os"
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
