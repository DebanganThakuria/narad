package storage

import (
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// A zstd frame read by a log whose codec is not zstd resolves to one
// shared codec, not a freshly built encoder and decoder per frame.
func TestWP3ForeignZstdCodecIsShared(t *testing.T) {
	first, err := codecForFlag(codec.FlagZstd, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := codecForFlag(codec.FlagZstd, codec.NewNoopCodec())
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("each foreign zstd frame got its own codec")
	}
	if first.Flag() != codec.FlagZstd {
		t.Fatalf("foreign codec flag = %d, want zstd", first.Flag())
	}
}

// wp3ForeignColdReadAllocs is the allocations of one cold read of a zstd
// frame by a log opened with readCodec.
func wp3ForeignColdReadAllocs(t *testing.T, readCodec codec.Codec) float64 {
	t.Helper()
	wp3NoFsync(t)
	zc, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatal(err)
	}
	const frames, perFrame = 64, 7
	dir := filepath.Join(t.TempDir(), "p0")
	w, err := NewLog(dir, Options{Codec: zc})
	if err != nil {
		t.Fatal(err)
	}
	for range frames {
		wp3CommitBatch(t, w, wp3Records(perFrame, 190))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := NewLog(dir, Options{Codec: readCodec})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.frameCache = newFrameCache(1, maxDecodeCacheBytes)
	for f := range frames {
		if _, err := l.ReadShared(int64(f * perFrame)); err != nil {
			t.Fatal(err)
		}
	}
	i := 0
	return testing.AllocsPerRun(frames-1, func() {
		i++
		if _, err := l.ReadShared(int64(i%frames) * perFrame); err != nil {
			t.Fatal(err)
		}
	})
}

// Reading zstd frames with a noop log costs what reading them with a
// zstd log does.
func TestWP3ForeignZstdFrameReadCostsNoCodecBuild(t *testing.T) {
	if wp3RaceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	zc, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		t.Fatal(err)
	}
	own := wp3ForeignColdReadAllocs(t, zc)
	foreign := wp3ForeignColdReadAllocs(t, nil)
	t.Logf("allocations per cold zstd frame read: zstd log %.1f, noop log %.1f", own, foreign)
	if foreign > own {
		t.Fatalf("a noop log reading a zstd frame allocates %.1f objects, a zstd log %.1f", foreign, own)
	}
}
