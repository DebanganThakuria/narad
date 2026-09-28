package wal

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Group-commit benchmarks inside prepared segments, where every batch
// overwrites zero-filled space instead of growing the file. The small
// record case is the produce path's steady state; the 1 MiB case (the
// largest produce body the HTTP API accepts) builds batches of many MiB,
// which is where the size of each write and sync matters.

// zzWP5BenchPrime appends until the log has rolled into a prepared
// segment, so the timed appends land in one. It fills the first segment
// with large records: one at a time, small ones would take minutes of
// syncs on macOS.
func zzWP5BenchPrime(b *testing.B, l *Log) {
	b.Helper()
	payload := bytes.Repeat([]byte("f"), 256<<10)
	deadline := time.Now().Add(time.Minute)
	for {
		l.mu.Lock()
		prepared := l.activePrepared
		nearFull := l.segmentSize+int64(frameHeaderSize+len(payload)) > l.opts.SegmentBytes
		ready := l.spareReady
		l.mu.Unlock()
		if prepared {
			return
		}
		if time.Now().After(deadline) {
			b.Fatal("the log never rolled into a prepared segment")
		}
		if nearFull && !ready {
			time.Sleep(time.Millisecond)
			continue
		}
		if _, err := l.Append(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
	}
}

// zzWP5BenchPreparedParallel runs 8*GOMAXPROCS appenders of
// payloadSize-byte records into prepared segments.
func zzWP5BenchPreparedParallel(b *testing.B, opts Options, payloadSize int) {
	l, err := Open(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("p"), payloadSize)
	if opts.Prealloc == PreallocOn {
		zzWP5BenchPrime(b, l)
	}
	fill := func(dst []byte) []byte { return append(dst, payload...) }
	b.SetParallelism(8)
	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := l.AppendWith(context.Background(), len(payload), fill); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	if err := l.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkZZWP5PreparedAppendSmall is 300-byte records: batches of
// tens of KiB.
func BenchmarkZZWP5PreparedAppendSmall(b *testing.B) {
	zzWP5BenchPreparedParallel(b, Options{SegmentBytes: 8 << 20, Prealloc: PreallocOn}, 300)
}

// BenchmarkZZWP5PreparedAppendLarge is 1 MiB records from 8*GOMAXPROCS
// appenders: batches of tens of MiB.
func BenchmarkZZWP5PreparedAppendLarge(b *testing.B) {
	zzWP5BenchPreparedParallel(b, Options{SegmentBytes: 64 << 20, Prealloc: PreallocOn}, 1<<20)
}

// BenchmarkZZWP5UnpreparedAppendLarge is the large case with
// preparation off (the default outside Linux): segments grow by
// appending, as before preparation existed.
func BenchmarkZZWP5UnpreparedAppendLarge(b *testing.B) {
	zzWP5BenchPreparedParallel(b, Options{SegmentBytes: 64 << 20, Prealloc: PreallocOff}, 1<<20)
}

// BenchmarkZZWP5PreparedWriteRuns isolates what the write limit costs a
// large group commit: a batch of 1 MiB frames overwritten in place in a
// zero-filled file, as one write and one sync (no limit, how every batch
// was written before the limit) or in runs of whole frames each written
// and synced before the next.
func BenchmarkZZWP5PreparedWriteRuns(b *testing.B) {
	for _, batchMiB := range []int{8, 32} {
		var buffer []byte
		payload := bytes.Repeat([]byte("w"), 1<<20-frameHeaderSize)
		for i := range batchMiB {
			buffer = appendFrame(buffer, uint64(i), payload)
		}
		for _, limit := range []int64{0, 4 << 20, 16 << 20} {
			name := "batch=" + strconv.Itoa(batchMiB) + "MiB/limit=" + strconv.FormatInt(limit>>20, 10) + "MiB"
			b.Run(name, func(b *testing.B) {
				file, err := os.OpenFile(filepath.Join(b.TempDir(), "00000000000000000000.wal"), os.O_CREATE|os.O_RDWR, 0o600)
				if err != nil {
					b.Fatal(err)
				}
				defer file.Close()
				if err := writeFull(file, make([]byte, len(buffer))); err != nil {
					b.Fatal(err)
				}
				if err := syncfile.SyncData(file); err != nil {
					b.Fatal(err)
				}
				var l Log
				b.SetBytes(int64(len(buffer)))
				b.ResetTimer()
				for b.Loop() {
					if _, err := file.Seek(0, io.SeekStart); err != nil {
						b.Fatal(err)
					}
					if err := l.writeAndSyncFileOps(file, buffer, limit); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
