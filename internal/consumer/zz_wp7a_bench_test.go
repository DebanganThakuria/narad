package consumer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkZZWP7aReserveCommitParallel8: each goroutine reserves and
// acks on its own partition, so the shard mutex never contends and only
// the process-wide words (the shard map lock, the clock) are shared.
func BenchmarkZZWP7aReserveCommitParallel8(b *testing.B) {
	f := NewInFlight(fixedCaps(1024, 1024), nil)
	ctx := context.Background()
	tail := int64(1) << 60
	for p := 0; p < 8; p++ {
		if _, err := f.ReserveNext(ctx, testTopic, p, testVT, 0); err != nil {
			b.Fatal(err)
		}
	}
	var next atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		p := int(next.Add(1)-1) % 8
		for pb.Next() {
			r, err := f.ReserveNext(ctx, testTopic, p, testVT, tail)
			if err != nil || !r.Reserved {
				panic(err)
			}
			if err := f.CommitHandle(testTopic, p, r.Offset, r.Nonce); err != nil {
				panic(err)
			}
		}
	})
}

// BenchmarkZZWP7aReserveEmpty is the probe of a partition with nothing
// past the frontier, which every empty consume scan pays per partition.
func BenchmarkZZWP7aReserveEmpty(b *testing.B) {
	f := NewInFlight(fixedCaps(1024, 1024), nil)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		r, err := f.ReserveNext(ctx, testTopic, 0, 30*time.Second, 0)
		if err != nil || r.Reserved {
			b.Fatal(err)
		}
	}
}

// BenchmarkZZWP7aReserveExtendRelease is a lease that is extended once
// and then nacked, the two other heap-touching operations.
func BenchmarkZZWP7aReserveExtendRelease(b *testing.B) {
	f := newClockedInFlight(1024, 1024)
	ctx := context.Background()
	tail := int64(1) << 60
	b.ReportAllocs()
	for b.Loop() {
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
		if err != nil || !r.Reserved {
			b.Fatalf("ReserveNext: %+v %v", r, err)
		}
		if _, err := f.ExtendHandle(testTopic, testPart, r.Offset, r.Nonce, testVT); err != nil {
			b.Fatal(err)
		}
		if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
			b.Fatal(err)
		}
	}
}
