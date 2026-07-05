package consumer

import (
	"context"
	"errors"
	"testing"
	"time"
)

const (
	testTopic    = "t"
	testPart     = 0
	testVT       = 30 * time.Second
	testDeepTail = 1_000_000
)

// fixedCaps returns a resolver that always yields the same caps.
func fixedCaps(maxIF, maxAA int) CapsResolver {
	return func(context.Context, string) (Caps, error) {
		return Caps{MaxInFlight: maxIF, MaxAckedAhead: maxAA}, nil
	}
}

func newTestInFlight(maxIF, maxAA int) *InFlight {
	return NewInFlight(fixedCaps(maxIF, maxAA), nil) // nil onCommit — pure in-memory for tests
}

// newClockedInFlight is newTestInFlight with the fake clock pinned to
// 1000 ms, the epoch most tests advance from.
func newClockedInFlight(maxIF, maxAA int) *InFlight {
	f := newTestInFlight(maxIF, maxAA)
	withClock(f, 1000)
	return f
}

func withClock(f *InFlight, now int64) {
	f.setTimeNow(func() int64 { return now })
}

// committedOffset returns the last committed offset (Next - 1).
// Returns -1 when no messages have been committed yet.
func committedOffset(f *InFlight, topic string, partition int) int64 {
	return f.Next(topic, partition) - 1
}

func mustReserve(t *testing.T, f *InFlight, logTail int64) ReserveResult {
	t.Helper()
	r, err := f.ReserveNext(context.Background(), testTopic, testPart, testVT, logTail)
	if err != nil {
		t.Fatalf("ReserveNext: %v", err)
	}
	if !r.Reserved {
		t.Fatalf("ReserveNext: expected reservation, got skip=%q", r.SkipReason)
	}
	return r
}

// mustReserveOn reserves on an arbitrary (topic, partition) with the
// default visibility timeout and a deep tail.
func mustReserveOn(t *testing.T, f *InFlight, topic string, partition int) ReserveResult {
	t.Helper()
	r, err := f.ReserveNext(context.Background(), topic, partition, testVT, testDeepTail)
	if err != nil {
		t.Fatalf("ReserveNext(%s, %d): %v", topic, partition, err)
	}
	if !r.Reserved {
		t.Fatalf("ReserveNext(%s, %d): expected reservation, got skip=%q", topic, partition, r.SkipReason)
	}
	return r
}

func mustCommit(t *testing.T, f *InFlight, offset, nonce int64) {
	t.Helper()
	if err := f.CommitHandle(testTopic, testPart, offset, nonce); err != nil {
		t.Fatalf("CommitHandle(offset=%d): %v", offset, err)
	}
}

func mustInit(t *testing.T, f *InFlight, committed int64) {
	t.Helper()
	if err := f.Init(context.Background(), testTopic, testPart, committed); err != nil {
		t.Fatalf("Init(committed=%d): %v", committed, err)
	}
}

func mustRefresh(t *testing.T, f *InFlight) {
	t.Helper()
	if err := f.RefreshCaps(context.Background(), testTopic); err != nil {
		t.Fatalf("RefreshCaps: %v", err)
	}
}

// reserveN reserves n messages in order and returns their nonces indexed by offset.
func reserveN(t *testing.T, f *InFlight, n int) map[int64]int64 {
	t.Helper()
	nonces := make(map[int64]int64, n)
	for range n {
		r := mustReserve(t, f, testDeepTail)
		nonces[r.Offset] = r.Nonce
	}
	return nonces
}

// wantReserved asserts a successful reservation of the given offset.
func wantReserved(t *testing.T, r ReserveResult, err error, offset int64) {
	t.Helper()
	if err != nil || !r.Reserved || r.Offset != offset {
		t.Fatalf("ReserveNext() = %+v, err=%v, want reserved offset %d", r, err, offset)
	}
}

// wantSkip asserts a skipped reservation with the given reason.
func wantSkip(t *testing.T, r ReserveResult, err error, reason string) {
	t.Helper()
	if err != nil || r.Reserved || r.SkipReason != reason {
		t.Fatalf("ReserveNext() = %+v, err=%v, want %q skip", r, err, reason)
	}
}

// wantErr asserts errors.Is(err, want).
func wantErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func wantNext(t *testing.T, f *InFlight, topic string, partition int, want int64) {
	t.Helper()
	if got := f.Next(topic, partition); got != want {
		t.Fatalf("Next(%s, %d) = %d, want %d", topic, partition, got, want)
	}
}

func wantCommitted(t *testing.T, f *InFlight, want int64) {
	t.Helper()
	if got := committedOffset(f, testTopic, testPart); got != want {
		t.Fatalf("committedOffset() = %d, want %d", got, want)
	}
}

func wantSnapshot(t *testing.T, f *InFlight, topic string, partition, inFlight, ackedAhead int) {
	t.Helper()
	gotIF, gotAA := f.Snapshot(topic, partition)
	if gotIF != inFlight || gotAA != ackedAhead {
		t.Fatalf("Snapshot(%s, %d) = (%d, %d), want (%d, %d)", topic, partition, gotIF, gotAA, inFlight, ackedAhead)
	}
}

// waitInFlightZero polls until the default shard reports zero in-flight
// reservations, failing after a second. Used by purger tests that race a
// background RunPurger goroutine against an advanced fake clock.
func waitInFlightZero(t *testing.T, f *InFlight) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if inFlight, _ := f.Snapshot(testTopic, testPart); inFlight == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	inFlight, _ := f.Snapshot(testTopic, testPart)
	t.Fatalf("Snapshot() inFlight = %d, want 0", inFlight)
}
