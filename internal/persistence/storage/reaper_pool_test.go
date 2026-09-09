package storage

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestReaperGoroutinesDoNotScaleWithLogs is the point of the shared
// reaper. Retention used to cost a goroutine and a timer per open
// partition log, so a node holding 150k logs carried 150k of each for
// work that runs about once a minute. Opening many logs must now add
// no retention goroutines at all beyond the single shared loop.
func TestReaperGoroutinesDoNotScaleWithLogs(t *testing.T) {
	const logs = 40
	dir := t.TempDir()

	settle := func() {
		for range 5 {
			runtime.GC()
			time.Sleep(20 * time.Millisecond)
		}
	}

	settle()
	before := runtime.NumGoroutine()

	opened := make([]*Log, 0, logs)
	for i := range logs {
		l, err := NewLog(filepath.Join(dir, "p"+string(rune('a'+i%26))+string(rune('a'+i/26))), Options{
			Retention: RetentionConfig{MaxAge: time.Hour, CheckInterval: time.Minute},
		})
		if err != nil {
			t.Fatalf("NewLog(%d) error = %v", i, err)
		}
		opened = append(opened, l)
	}
	t.Cleanup(func() {
		for _, l := range opened {
			_ = l.Close()
		}
	})

	settle()
	after := runtime.NumGoroutine()
	added := after - before

	// One flusher per log is expected and untouched by this change; the
	// reaper's used to double it. Allow the flushers plus a small
	// constant for the shared loop and any runtime noise.
	ceiling := logs + 8
	if added > ceiling {
		t.Fatalf("opening %d logs added %d goroutines (want <= %d): retention is still per-log",
			logs, added, ceiling)
	}
	t.Logf("%d logs added %d goroutines (~%.1f per log)", logs, added, float64(added)/float64(logs))
}

// A log with no age bound has nothing to reap, so it must not be
// enrolled at all. Previously it still parked a goroutine forever on
// its stop channel doing nothing.
func TestReaperSkipsLogsWithoutRetention(t *testing.T) {
	before := len(sharedReaper.snapshot())
	l, err := NewLog(t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	defer l.Close()
	if got := len(sharedReaper.snapshot()); got != before {
		t.Fatalf("registered logs = %d, want %d: a log with no age bound was enrolled", got, before)
	}
}

// Closing a log must take it out of the shared loop, or a long-lived
// process would sweep an ever-growing set of dead logs.
func TestReaperUnregistersOnClose(t *testing.T) {
	before := len(sharedReaper.snapshot())
	l, err := NewLog(t.TempDir(), Options{
		Retention: RetentionConfig{MaxAge: time.Hour, CheckInterval: time.Minute},
	})
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	if got := len(sharedReaper.snapshot()); got != before+1 {
		t.Fatalf("registered logs = %d, want %d after opening one with retention", got, before+1)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := len(sharedReaper.snapshot()); got != before {
		t.Fatalf("registered logs = %d, want %d after close: the log was left enrolled", got, before)
	}
}

// The goroutine-count tests above prove retention stopped costing a
// goroutine per log. These prove it still WORKS: a shared loop that
// never sweeps anything would pass every count assertion while quietly
// letting disks fill.

// TestSharedReaperActuallySweeps is the one that matters. A log
// registered with the pool, holding a sealed segment past its age
// bound, must have it deleted by the shared loop with nothing calling
// sweep() directly.
//
// The pool ticks in real time but each entry's due check uses that
// log's own clock, so advancing the fake clock is what makes the sweep
// due; the loop then has to notice within a tick.
func TestSharedReaperActuallySweeps(t *testing.T) {
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	const maxAge = 10 * time.Minute
	opts := timedRetentionOpts(t, clock, RetentionConfig{MaxAge: maxAge, CheckInterval: time.Millisecond})
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	defer l.Close()

	appendCommit(t, l, "first")
	// Past the age bound: the next write rolls the aged active segment,
	// leaving a sealed one for the reaper to take.
	clock.Set(t0.Add(2 * maxAge))
	appendCommit(t, l, "second")

	l.rwmu.RLock()
	before := len(l.segments)
	l.rwmu.RUnlock()
	if before < 2 {
		t.Fatalf("expected a sealed segment after the time-based roll, got %d segment(s)", before)
	}

	// Nothing here calls sweep(): the shared loop has to do it.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		l.rwmu.RLock()
		n := len(l.segments)
		l.rwmu.RUnlock()
		if n < before {
			t.Logf("shared reaper reduced %d segments to %d", before, n)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("segments still %d after 20s: the shared reaper never swept this log", before)
}

// A log registered LATER with a short check interval must not inherit
// the period of one registered earlier with a long one.
//
// This is a regression test for a real bug: the pool used to derive its
// loop period from the shortest interval seen at registration, but the
// ticker is created once, so the slow log below would have pinned the
// loop at an hour and the fast log would never have been swept.
func TestSharedReaperHonoursPerLogIntervals(t *testing.T) {
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	const maxAge = 10 * time.Minute

	// Registered FIRST, asking for hourly sweeps.
	slowClock := newAtomicTime(t0)
	slow, err := NewLog(testLogPath(t), timedRetentionOpts(t, slowClock,
		RetentionConfig{MaxAge: maxAge, CheckInterval: time.Hour}))
	if err != nil {
		t.Fatalf("NewLog(slow) error = %v", err)
	}
	defer slow.Close()

	// Registered SECOND, wanting them promptly.
	fastClock := newAtomicTime(t0)
	fast, err := NewLog(testLogPath(t), timedRetentionOpts(t, fastClock,
		RetentionConfig{MaxAge: maxAge, CheckInterval: time.Millisecond}))
	if err != nil {
		t.Fatalf("NewLog(fast) error = %v", err)
	}
	defer fast.Close()

	appendCommit(t, fast, "first")
	fastClock.Set(t0.Add(2 * maxAge))
	appendCommit(t, fast, "second")

	fast.rwmu.RLock()
	before := len(fast.segments)
	fast.rwmu.RUnlock()
	if before < 2 {
		t.Fatalf("expected a sealed segment on the fast log, got %d", before)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		fast.rwmu.RLock()
		n := len(fast.segments)
		fast.rwmu.RUnlock()
		if n < before {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("the fast log was never swept: a slow log registered earlier is holding back the loop period")
}

// A log closed while the pool is running must neither be swept nor
// panic. sweepDue collects entries under the lock and sweeps outside it,
// so a close can land in between; that has to be harmless.
func TestSharedReaperToleratesACloseMidFlight(t *testing.T) {
	future := func() time.Time { return time.Now().Add(time.Hour) }
	for range 12 {
		l, err := NewLog(t.TempDir(), Options{
			Retention: RetentionConfig{MaxAge: time.Minute, CheckInterval: time.Millisecond, Now: future},
		})
		if err != nil {
			t.Fatalf("NewLog() error = %v", err)
		}
		if _, err := l.Append(EncodeKeyedRecord("", 1, []byte(`{"a":1}`))); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
		// Close immediately, racing whatever the loop is doing.
		if err := l.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}
	// Give the loop time to run over the churn; a panic would fail here.
	time.Sleep(2500 * time.Millisecond)
	for _, l := range sharedReaper.snapshot() {
		if l.closed.Load() {
			t.Fatal("a closed log is still enrolled in the shared reaper")
		}
	}
}
