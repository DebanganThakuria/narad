package wal

import (
	"bytes"
	"context"
	"testing"
)

// Produce-path benchmarks for the ingress WAL. Every append waits for
// its group commit, so ns/op tracks the write+sync cost per batch
// divided by the batch size, and B/op tracks the staging buffer.

// zzWP5BenchAppendParallel runs 8*GOMAXPROCS goroutines appending
// payloadSize-byte records through AppendWith.
func zzWP5BenchAppendParallel(b *testing.B, opts Options, payloadSize int) {
	l, err := Open(b.TempDir(), opts)
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("p"), payloadSize)
	fill := func(dst []byte) []byte { return append(dst, payload...) }
	b.SetParallelism(8)
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

// BenchmarkZZWP5AppendWithParallel is the steady state: one segment, no
// rolls.
func BenchmarkZZWP5AppendWithParallel(b *testing.B) {
	zzWP5BenchAppendParallel(b, Options{SegmentBytes: 1 << 30}, 300)
}

// BenchmarkZZWP5AppendWithParallelRolls rolls every MiB, so the roll
// path (and the segment preparation behind it) is part of the cost.
func BenchmarkZZWP5AppendWithParallelRolls(b *testing.B) {
	zzWP5BenchAppendParallel(b, Options{SegmentBytes: 1 << 20}, 300)
}

// BenchmarkZZWP5AppendSerial is one appender: every append is its own
// write plus data sync, so ns/op is the per-batch sync latency the 202
// waits on. 4 MiB segments roll every 512 appends.
func BenchmarkZZWP5AppendSerial(b *testing.B) {
	l, err := Open(b.TempDir(), Options{SegmentBytes: 4 << 20})
	if err != nil {
		b.Fatal(err)
	}
	payload := bytes.Repeat([]byte("s"), 8<<10-frameHeaderSize)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := l.Append(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := l.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkZZWP5ReplayFromCursor replays 4096 records of 512 bytes from
// the start of a closed log.
func BenchmarkZZWP5ReplayFromCursor(b *testing.B) {
	dir := b.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	const n = 4096
	payload := bytes.Repeat([]byte("r"), 512)
	done := make(chan struct{}, 64)
	for range 64 {
		go func() {
			for range n / 64 {
				if _, err := l.Append(context.Background(), payload); err != nil {
					panic(err)
				}
			}
			done <- struct{}{}
		}()
	}
	for range 64 {
		<-done
	}
	if err := l.Close(); err != nil {
		b.Fatal(err)
	}
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count := 0
		err := ReplayFromCursor(dir, Cursor{}, 0, func(r Record, _ Cursor) error {
			count++
			sink += len(r.Payload)
			return nil
		})
		if err != nil || count != n {
			b.Fatalf("replay: %v count=%d", err, count)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/record")
	_ = sink
}
