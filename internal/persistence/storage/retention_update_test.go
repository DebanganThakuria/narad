package storage

import (
	"slices"
	"sync"
	"testing"
	"time"
)

// agedRetentionLog opens a log under maxAge whose first two records sit
// in a sealed segment last written two hours before the log's clock,
// with a third record in the active segment written at the same time.
func agedRetentionLog(t *testing.T, maxAge time.Duration) *Log {
	t.Helper()
	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	l, err := NewLog(testLogPath(t), timedRetentionOpts(t, clock, RetentionConfig{MaxAge: maxAge, MaxSegmentAge: -1}))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	appendCommit(t, l, "a")
	appendCommit(t, l, "b")
	if err := l.submitCommit(commitRequest{hwm: -1, rotate: true}); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	appendCommit(t, l, "c")
	clock.Set(t0.Add(2 * time.Hour))
	if got := l.SegmentCount(); got != 2 {
		t.Fatalf("setup: %d segments, want a sealed one and the active one", got)
	}
	return l
}

func enrolledWithReaper(l *Log) bool { return slices.Contains(sharedReaper.snapshot(), l) }

// dueAtNextTick reports whether the shared loop sweeps l on its next
// tick.
func dueAtNextTick(l *Log) bool {
	sharedReaper.mu.Lock()
	defer sharedReaper.mu.Unlock()
	e := sharedReaper.logs[l]
	return e != nil && !l.reaper.cfg.Now().Before(e.next)
}

// A keep-forever log is not enrolled with the shared reaper. Gaining a
// bound enrols it, due at the loop's next tick; losing the bound takes
// it out again, and a closed log is never enrolled.
func TestSetRetentionReRegistersWithTheReaper(t *testing.T) {
	l := agedRetentionLog(t, 0)
	if enrolledWithReaper(l) {
		t.Fatal("setup: a keep-forever log is enrolled")
	}
	if !l.SetRetentionMaxAge(time.Hour) {
		t.Fatal("SetRetentionMaxAge(1h) on a keep-forever log reported no change")
	}
	if !enrolledWithReaper(l) {
		t.Fatal("a log that gained a bound is not enrolled with the shared reaper")
	}
	if !dueAtNextTick(l) {
		t.Fatal("a log that gained a bound is not due at the loop's next tick")
	}
	if got := l.RetentionMaxAge(); got != time.Hour {
		t.Fatalf("RetentionMaxAge = %v, want the new 1h", got)
	}
	if !l.SweepRetentionNow() {
		t.Fatal("the new 1h bound did not reap the 2h-old sealed segment")
	}
	if !l.SetRetentionMaxAge(0) {
		t.Fatal("SetRetentionMaxAge(0) reported no change")
	}
	if enrolledWithReaper(l) {
		t.Fatal("a log that lost its bound is still enrolled")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l.SetRetentionMaxAge(time.Hour)
	if enrolledWithReaper(l) {
		t.Fatal("a closed log was enrolled")
	}
}

// Close and SetRetentionMaxAge race from separate goroutines; whichever
// order they land in, the closed log is never left in the shared loop.
func TestSetRetentionRacingCloseLeavesNoReaperEntry(t *testing.T) {
	for i := range 200 {
		l, err := NewLog(t.TempDir(), Options{
			Retention: RetentionConfig{CheckInterval: time.Minute},
		})
		if err != nil {
			t.Fatalf("NewLog: %v", err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Go(func() {
			<-start
			for d := range 5 {
				l.SetRetentionMaxAge(time.Duration(d+1) * time.Hour)
			}
		})
		wg.Go(func() {
			<-start
			_ = l.Close()
		})
		close(start)
		wg.Wait()
		if enrolledWithReaper(l) {
			t.Fatalf("iteration %d: a closed log was left enrolled with the shared reaper", i)
		}
	}
}

// Raising the bound of an open log protects what the old bound would
// have reaped, and setting the same bound again reports no change.
func TestSetRetentionRaiseKeepsRecordsTheOldBoundWouldReap(t *testing.T) {
	l := agedRetentionLog(t, time.Hour)
	if !l.SetRetentionMaxAge(30 * 24 * time.Hour) {
		t.Fatal("SetRetentionMaxAge(30d) reported no change")
	}
	if l.SetRetentionMaxAge(30 * 24 * time.Hour) {
		t.Fatal("a second identical SetRetentionMaxAge reported a change")
	}
	if l.SweepRetentionNow() {
		t.Fatal("a sweep under the raised bound changed the segment set")
	}
	if got := l.OldestOffset(); got != 0 {
		t.Fatalf("records reaped at the old 1h bound after a raise to 30d: oldest offset %d", got)
	}
}

// Lowering the bound reaps what the new bound expires, the active
// segment's expired records included, and the shared loop sweeps the
// log at its next tick rather than a check interval later.
func TestSetRetentionLowerReapsAtTheNextTick(t *testing.T) {
	l := agedRetentionLog(t, 30*24*time.Hour)
	if l.SweepRetentionNow() {
		t.Fatal("setup: a sweep under 30d reaped 2h-old records")
	}
	if !l.SetRetentionMaxAge(time.Hour) {
		t.Fatal("SetRetentionMaxAge(1h) reported no change")
	}
	if !dueAtNextTick(l) {
		t.Fatal("a lowered bound is not due at the loop's next tick")
	}
	l.SweepRetentionNow()
	if got := l.OldestOffset(); got != 3 {
		t.Fatalf("oldest offset %d after lowering to 1h, want 3 (every 2h-old record reaped)", got)
	}
}

// The age-based roll of the active segment follows the live bound.
func TestSetRetentionMovesTheActiveSegmentRollAge(t *testing.T) {
	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	l, err := NewLog(testLogPath(t), timedRetentionOpts(t, clock, RetentionConfig{MaxAge: 30 * 24 * time.Hour}))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	appendCommit(t, l, "a")
	clock.Set(t0.Add(2 * time.Hour))
	appendCommit(t, l, "b")
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("setup: %d segments under 30d, want 1", got)
	}
	l.SetRetentionMaxAge(time.Hour)
	clock.Set(t0.Add(3 * time.Hour))
	appendCommit(t, l, "c")
	if got := l.SegmentCount(); got != 2 {
		t.Fatalf("%d segments after a write past the new 1h roll age, want 2", got)
	}
}
