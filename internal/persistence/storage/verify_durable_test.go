package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/klauspost/compress/zstd"
)

// VerifyDurable must pass for a healthy committed batch and FAIL when the
// on-disk frame is corrupted — proving CRC-based detection without any decode,
// for both the passthrough and zstd codecs.
func TestVerifyDurableDetectsCorruption(t *testing.T) {
	zc, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatalf("NewZstdCodec: %v", err)
	}
	for _, tc := range []struct {
		name string
		c    codec.Codec
	}{
		{"noop", codec.NewNoopCodec()},
		{"zstd", zc},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testLogPath(t)
			l, err := NewLog(dir, slowFlushOpts(t, tc.c))
			if err != nil {
				t.Fatalf("NewLog: %v", err)
			}
			defer l.Close()

			recs := make([][]byte, 8)
			for i := range recs {
				recs[i] = fmt.Appendf(nil, `{"n":%d,"pad":"aaaaaaaaaaaaaaaa"}`, i)
			}
			first, last, err := l.AppendBatch(recs)
			if err != nil {
				t.Fatalf("AppendBatch: %v", err)
			}
			if err := l.Sync(); err != nil {
				t.Fatalf("Sync: %v", err)
			}

			if err := l.VerifyDurable(first, last); err != nil {
				t.Fatalf("healthy VerifyDurable: %v", err)
			}

			// Corrupt the last byte of the on-disk segment (in the frame
			// payload) via a separate fd; the open Log sees it through the
			// shared page cache. VerifyDurable re-reads the file (not the
			// cache), so its CRC check must now fail.
			segs, _ := filepath.Glob(filepath.Join(dir, "*.log"))
			if len(segs) == 0 {
				t.Fatal("no segment file found")
			}
			f, err := os.OpenFile(segs[len(segs)-1], os.O_RDWR, 0)
			if err != nil {
				t.Fatalf("open segment: %v", err)
			}
			info, _ := f.Stat()
			b := make([]byte, 1)
			if _, err := f.ReadAt(b, info.Size()-1); err != nil {
				t.Fatalf("read last byte: %v", err)
			}
			b[0] ^= 0xFF
			if _, err := f.WriteAt(b, info.Size()-1); err != nil {
				t.Fatalf("write last byte: %v", err)
			}
			f.Close()

			if err := l.VerifyDurable(first, last); err == nil {
				t.Fatal("VerifyDurable on a corrupted frame returned nil, want CRC error")
			}
		})
	}
}

// VerifyDurable walks multiple frames (two commit batches → two frames).
func TestVerifyDurableMultiFrame(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	first1, _, _ := l.AppendBatch([][]byte{[]byte("a"), []byte("b"), []byte("c")})
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 1: %v", err)
	}
	_, last2, _ := l.AppendBatch([][]byte{[]byte("d"), []byte("e")})
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 2: %v", err)
	}
	if err := l.VerifyDurable(first1, last2); err != nil {
		t.Fatalf("multi-frame VerifyDurable: %v", err)
	}
}
