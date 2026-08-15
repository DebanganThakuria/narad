package consumer

import (
	"context"
	"fmt"
	"testing"
)

// The existing reserve benchmarks (BenchmarkReserveCommitCycle,
// BenchmarkReserveExpirePurge) both keep the in-flight set at depth ~1:
// every reserved offset is resolved before the next reserve. That is the
// single-consumer case.
//
// The multi-consumer case is different. ReserveNext scans upward from
// committed+1 and skips every offset that is already reserved, acked
// ahead, or corrupt:
//
//	for off := next; off < logTail; off++ {
//	    if sh.resolvedOrReservedLocked(off) { continue }
//	    return sh.reserveLocked(...)
//	}
//
// With D outstanding reservations the scan walks D offsets, each costing
// up to three map lookups, and it does so holding the partition's
// exclusive mutex — so every consumer on that partition serialises behind
// it. These benchmarks measure the cost as a function of D.
//
// Each measured iteration reserves the (D+1)th offset and immediately
// releases it, so the depth stays pinned at D for the whole run rather
// than growing with b.N. The release is O(1) and common to every depth,
// so it shows up as a constant offset — read the SLOPE across depths, not
// the absolute ns/op.

// benchDepths are outstanding-reservation depths to measure. 1024 is the
// MaxInFlight the existing benches use.
var benchDepths = []int{1, 16, 128, 512, 1024}

// BenchmarkReserveNextAtDepth measures reserve+release with D offsets
// already reserved and unresolved — the steady state when D consumers
// hold messages concurrently on one partition.
func BenchmarkReserveNextAtDepth(b *testing.B) {
	ctx := context.Background()
	tail := int64(1) << 60

	for _, depth := range benchDepths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			f := newClockedInFlight(depth+128, depth+128)
			for range depth {
				r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
				if err != nil || !r.Reserved {
					b.Fatalf("seed ReserveNext: %+v %v", r, err)
				}
			}
			b.ResetTimer()

			for range b.N {
				r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
				if err != nil || !r.Reserved {
					b.Fatalf("ReserveNext: %+v %v", r, err)
				}
				if err := f.ReleaseHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
					b.Fatalf("ReleaseHandle: %v", err)
				}
			}
		})
	}
}

// BenchmarkReserveNextAckedAhead measures the out-of-order-ack shape: D
// offsets acked ahead of a frontier blocked on one hole. Those offsets
// stay in the ackedAhead set, so the scan walks them on every reserve.
// This is the state one slow or lost message produces while its
// neighbours keep being consumed and acked.
func BenchmarkReserveNextAckedAhead(b *testing.B) {
	ctx := context.Background()
	tail := int64(1) << 60

	for _, depth := range benchDepths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			f := newClockedInFlight(depth+128, depth+128)

			// Offset 0 is reserved and never acked: the frontier hole.
			hole, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
			if err != nil || !hole.Reserved {
				b.Fatalf("seed hole: %+v %v", hole, err)
			}
			// Reserve and ack the next depth offsets: they land in ackedAhead
			// because the frontier cannot advance past the hole.
			for range depth {
				r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
				if err != nil || !r.Reserved {
					b.Fatalf("seed ReserveNext: %+v %v", r, err)
				}
				if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
					b.Fatalf("seed CommitHandle: %v", err)
				}
			}
			b.ResetTimer()

			for range b.N {
				r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
				if err != nil || !r.Reserved {
					b.Fatalf("ReserveNext: %+v %v", r, err)
				}
				if err := f.ReleaseHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
					b.Fatalf("ReleaseHandle: %v", err)
				}
			}
		})
	}
}

// BenchmarkReserveNextParallel measures what the depth costs in the
// situation that actually matters: many consumers hitting one partition
// at once, all serialising on sh.mu while each holds it for a scan
// proportional to the depth.
func BenchmarkReserveNextParallel(b *testing.B) {
	ctx := context.Background()
	tail := int64(1) << 60

	for _, depth := range benchDepths {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			f := newClockedInFlight(depth+128, depth+128)
			for range depth {
				r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
				if err != nil || !r.Reserved {
					b.Fatalf("seed ReserveNext: %+v %v", r, err)
				}
			}
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
					if err != nil || !r.Reserved {
						b.Fatalf("ReserveNext: %+v %v", r, err)
					}
					if err := f.ReleaseHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
						b.Fatalf("ReleaseHandle: %v", err)
					}
				}
			})
		})
	}
}
