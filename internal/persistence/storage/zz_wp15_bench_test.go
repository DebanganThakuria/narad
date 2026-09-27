package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkWP15OpenSealed is NewLog on a partition of four full 64 MiB
// sealed segments (256 MiB) and a 1 MiB active segment, warm page cache,
// at three frame sizes: 16, 64 and 1024 records of 256 bytes. Recovery
// used to read and CRC-check every sealed frame, so small frames cost
// the most; the active segment is still walked in full.
func BenchmarkWP15OpenSealed(b *testing.B) {
	for _, per := range []int{16, 64, 1024} {
		frameBytes := per * (4 + len(wp15Record(0, 256)))
		b.Run(fmt.Sprintf("frame=%dKiB", frameBytes>>10), func(b *testing.B) {
			wp3NoFsync(b)
			dir := filepath.Join(b.TempDir(), "p0")
			wp15WriteLayout(b, dir, 4, (64<<20)/frameBytes, (1<<20)/frameBytes, per, 256)
			// The first open writes the hwm file at Close; open once so
			// every timed open finds the same directory.
			l, err := NewLog(dir, Options{})
			if err != nil {
				b.Fatal(err)
			}
			if err := l.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l, err := NewLog(dir, Options{})
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := l.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkWP15ReadSealed reads records of sealed segments on a freshly
// opened log, as a consumer draining a backlog after a restart does:
// seq walks every offset in order (the frame cache serves most reads),
// miss reads a different frame each time through a one-frame cache, so
// every read is a file read, a CRC check and a split.
func BenchmarkWP15ReadSealed(b *testing.B) {
	const per = 16
	for _, mode := range []string{"seq", "miss"} {
		b.Run(mode, func(b *testing.B) {
			wp3NoFsync(b)
			dir := filepath.Join(b.TempDir(), "p0")
			frameBytes := per * (4 + len(wp15Record(0, 256)))
			lay := wp15WriteLayout(b, dir, 4, (4<<20)/frameBytes, 4, per, 256)
			l, err := NewLog(dir, Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			sealedEnd := lay.bases[len(lay.bases)-1]
			step := int64(1)
			if mode == "miss" {
				l.frameCache = newFrameCache(1, maxDecodeCacheBytes)
				step = per + 1
			}
			b.ReportAllocs()
			b.ResetTimer()
			off := int64(0)
			for i := 0; i < b.N; i++ {
				if _, err := l.ReadShared(off); err != nil {
					b.Fatal(err)
				}
				off += step
				if off >= sealedEnd {
					off = 0
				}
			}
		})
	}
}

// BenchmarkWP15OpenActive is NewLog on a partition whose only segment is
// a nearly full 60 MiB active one, the part of an open that recovery
// still reads and CRC-checks in full, at the same three frame sizes.
func BenchmarkWP15OpenActive(b *testing.B) {
	for _, per := range []int{16, 64, 1024} {
		frameBytes := per * (4 + len(wp15Record(0, 256)))
		b.Run(fmt.Sprintf("frame=%dKiB", frameBytes>>10), func(b *testing.B) {
			wp3NoFsync(b)
			dir := filepath.Join(b.TempDir(), "p0")
			wp15WriteLayout(b, dir, 0, 0, (60<<20)/frameBytes, per, 256)
			l, err := NewLog(dir, Options{})
			if err != nil {
				b.Fatal(err)
			}
			if err := l.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l, err := NewLog(dir, Options{})
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := l.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}
