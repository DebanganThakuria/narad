package consumer

import (
	"context"
	"testing"
	"time"
)

func TestRunPurgerRemovesExpiredEntries(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	mustReserve(t, f, testDeepTail)
	wantSnapshot(t, f, testTopic, testPart, 1, 0)

	go f.RunPurger(t.Context(), 10*time.Millisecond)

	withClock(f, 1000+60_000)
	waitInFlightZero(t, f)
}

func TestPurgeExpiredSkipsStaleHeapEntry(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	r1 := mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	r2 := mustReserve(t, f, testDeepTail)
	if r2.Offset != r1.Offset {
		t.Fatalf("re-reserved offset = %d, want %d", r2.Offset, r1.Offset)
	}

	withClock(f, 1000+120_000)
	f.purgeAll()
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestRunPurgerReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.RunPurger(ctx, time.Hour)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunPurger() did not stop after context cancellation")
	}
}

func TestRunPurgerWithNoShardsIsSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.purgeAll()
}

func TestPurgeAllWithMultipleShardsIsSafe(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserveOn(t, f, testTopic, 0)
	mustReserveOn(t, f, testTopic, 1)
	withClock(f, 1000+60_000)
	f.purgeAll()
	wantSnapshot(t, f, testTopic, 0, 0, 0)
	wantSnapshot(t, f, testTopic, 1, 0, 0)
}

func TestRunPurgerCanSweepAfterReReservation(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	mustReserve(t, f, testDeepTail)

	go f.RunPurger(t.Context(), 10*time.Millisecond)

	withClock(f, 1000+120_000)
	waitInFlightZero(t, f)
}

func TestRunPurgerContextAlreadyCanceledReturnsImmediately(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	go func() {
		f.RunPurger(ctx, time.Hour)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunPurger() did not return")
	}
}

func TestSnapshotAfterExpiredPurgeReturnsZero(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	withClock(f, 1000+60_000)
	f.purgeAll()
	wantSnapshot(t, f, testTopic, testPart, 0, 0)
}

func TestRunPurgerSweepsWithoutPanickingAfterDrop(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	f.DropTopic(testTopic)
	ctx, cancel := context.WithCancel(context.Background())
	go f.RunPurger(ctx, 10*time.Millisecond)
	cancel()
}

func TestRunPurgerAfterOtherTopicDropStillSweepsCurrentTopic(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	f.DropTopic("other")

	go f.RunPurger(t.Context(), 10*time.Millisecond)

	withClock(f, 1000+60_000)
	waitInFlightZero(t, f)
}

func TestRunPurgerNoShardsContextCanceledSafe(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.RunPurger(ctx, time.Hour)
}

func TestRunPurgerWithEmptyIntervalContextCancelStillReturns(t *testing.T) {
	t.Parallel()
	f := newTestInFlight(10, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.RunPurger(ctx, time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunPurger() did not return")
	}
}

func TestRunPurgerAfterCurrentAndOtherTopicStateSafe(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)
	mustReserve(t, f, testDeepTail)
	mustReserveOn(t, f, "other", testPart)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.RunPurger(ctx, time.Hour)
}

// TestPurgeAllNotifiesReleaseOnExpiry pins the release-notifier
// contract: the background purger must invoke the notifier exactly for
// the partitions where expired reservations were actually released —
// that is what wakes long-poll consumers blocked while every visible
// message was in-flight.
func TestPurgeAllNotifiesReleaseOnExpiry(t *testing.T) {
	t.Parallel()
	f := newClockedInFlight(10, 10)

	type release struct {
		topic     string
		partition int
	}
	var releases []release
	f.SetReleaseNotifier(func(topic string, partition int) {
		releases = append(releases, release{topic, partition})
	})

	mustReserve(t, f, testDeepTail)

	// Not yet expired: no notification.
	f.purgeAll()
	if len(releases) != 0 {
		t.Fatalf("releases before expiry = %v, want none", releases)
	}

	// Past the visibility timeout: exactly one notification for the shard.
	withClock(f, 1000+testVT.Milliseconds()+1)
	f.purgeAll()
	if len(releases) != 1 || releases[0] != (release{testTopic, testPart}) {
		t.Fatalf("releases after expiry = %v, want [{%s %d}]", releases, testTopic, testPart)
	}

	// Nothing left in flight: sweeping again must not re-notify.
	f.purgeAll()
	if len(releases) != 1 {
		t.Fatalf("releases after empty sweep = %v, want exactly 1", releases)
	}
}
