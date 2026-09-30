package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// wp3ColdReadAllocs is the allocations of one ReadShared that decodes a
// frame from the file (a one-frame cache, a different frame each call).
func wp3ColdReadAllocs(t *testing.T, c codec.Codec, perFrame int) float64 {
	t.Helper()
	wp3NoFsync(t)
	const frames = 64
	dir := filepath.Join(t.TempDir(), "p0")
	w, err := NewLog(dir, Options{Codec: c})
	if err != nil {
		t.Fatal(err)
	}
	for range frames {
		wp3CommitBatch(t, w, wp3Records(perFrame, 190))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := NewLog(dir, Options{Codec: c})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.frameCache = newFrameCache(1, maxDecodeCacheBytes)
	// Warm the index and the navigation cache so only the frame decode
	// is measured.
	for f := range frames {
		if _, err := l.ReadShared(int64(f * perFrame)); err != nil {
			t.Fatal(err)
		}
	}
	i := 0
	return testing.AllocsPerRun(frames-1, func() {
		i++
		if _, err := l.ReadShared(int64(i%frames) * int64(perFrame)); err != nil {
			t.Fatal(err)
		}
	})
}

// A cold frame read caches the decoded records as slices of the buffer
// it read into: no copy of the payload through the noop codec and no
// allocation per record, so the cost does not grow with the record
// count.
func TestWP3ColdReadAllocationsIndependentOfRecordCount(t *testing.T) {
	if wp3RaceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	zc, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		c     codec.Codec
		limit float64
	}{
		{"none", nil, 5},
		{"zstd", zc, 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			small := wp3ColdReadAllocs(t, tc.c, 7)
			large := wp3ColdReadAllocs(t, tc.c, 33)
			t.Logf("allocations per cold read: 7 records %.1f, 33 records %.1f", small, large)
			if small > tc.limit {
				t.Fatalf("cold read of a 7-record frame: %.1f allocations, want <= %.0f", small, tc.limit)
			}
			if large != small {
				t.Fatalf("cold read allocations grow with the record count: 7 records %.1f, 33 records %.1f", small, large)
			}
		})
	}
}

// Records read cold are the bytes written, for both codecs, and stay
// intact while later reads decode other frames.
func TestWP3ColdReadRecordsIntact(t *testing.T) {
	zc, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []codec.Codec{nil, zc} {
		dir := filepath.Join(t.TempDir(), "p0")
		w, err := NewLog(dir, Options{Codec: c})
		if err != nil {
			t.Fatal(err)
		}
		var want [][]byte
		for f := range 20 {
			batch := wp3Records(5, 100+f)
			for _, r := range batch {
				want = append(want, append([]byte(nil), r...))
			}
			wp3CommitBatch(t, w, batch)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		l, err := NewLog(dir, Options{Codec: c})
		if err != nil {
			t.Fatal(err)
		}
		l.frameCache = newFrameCache(2, maxDecodeCacheBytes)
		held := make([][]byte, len(want))
		for off := range want {
			rec, err := l.ReadShared(int64(off))
			if err != nil {
				t.Fatalf("ReadShared(%d): %v", off, err)
			}
			held[off] = rec
		}
		for off := range want {
			if !bytes.Equal(held[off], want[off]) {
				t.Fatalf("record %d changed after later reads", off)
			}
		}
		_ = l.Close()
	}
}

// An uncompressed frame whose header claims a different uncompressed
// size than its payload is corrupt, although its CRC checks out.
func TestWP3UncompressedFrameSizeMismatchIsCorrupt(t *testing.T) {
	frame, err := encodeFrame([][]byte{[]byte("abc"), []byte("defg")}, 0, codec.NewNoopCodec())
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(frame[15:19], binary.BigEndian.Uint32(frame[15:19])+1)
	binary.BigEndian.PutUint32(frame[23:27], crc32cOf(frame[2:23], frame[headerSize:]))
	if _, _, _, err := readFrameAt(bytes.NewReader(frame), 0, &Log{codec: codec.NewNoopCodec()}); !errors.Is(err, errCorrupt) {
		t.Fatalf("readFrameAt = %v, want errCorrupt", err)
	}
}
