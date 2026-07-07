package consumer

import (
	"context"
	"testing"
)

// BenchmarkReserveCommitCycle measures the queue-mode hot loop: reserve
// the next offset, then ack it. Exercises the per-call expiry purge and
// heap maintenance on both operations.
func BenchmarkReserveCommitCycle(b *testing.B) {
	f := newClockedInFlight(1024, 1024)
	ctx := context.Background()
	tail := int64(1) << 60
	for b.Loop() {
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
		if err != nil || !r.Reserved {
			b.Fatalf("ReserveNext: %+v %v", r, err)
		}
		if err := f.CommitHandle(testTopic, testPart, r.Offset, r.Nonce); err != nil {
			b.Fatalf("CommitHandle: %v", err)
		}
	}
}

// BenchmarkReserveExpirePurge measures the redelivery path: every
// reservation expires before the next reserve, so each ReserveNext pops
// one expired heap entry — the exact code path the expiry-match check
// sits on.
func BenchmarkReserveExpirePurge(b *testing.B) {
	f := newClockedInFlight(1024, 1024)
	ctx := context.Background()
	tail := int64(1) << 60
	now := int64(1000)
	for b.Loop() {
		now += testVT.Milliseconds() + 1
		withClock(f, now)
		r, err := f.ReserveNext(ctx, testTopic, testPart, testVT, tail)
		if err != nil || !r.Reserved {
			b.Fatalf("ReserveNext: %+v %v", r, err)
		}
	}
}
