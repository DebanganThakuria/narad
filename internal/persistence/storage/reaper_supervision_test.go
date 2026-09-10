package storage

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// reaperLoops counts the shared retention loops alive right now by
// scanning goroutine stacks for the loop's frame.
func reaperLoops() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "(*reaperPool).run(")
}

func withSweepHook(t *testing.T, hook func(*Log)) {
	t.Helper()
	sharedReaper.mu.Lock()
	prev := sharedReaper.sweepHook
	sharedReaper.sweepHook = hook
	sharedReaper.mu.Unlock()
	t.Cleanup(func() {
		sharedReaper.mu.Lock()
		sharedReaper.sweepHook = prev
		sharedReaper.mu.Unlock()
	})
}

func openRetainedLog(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := NewLog(dir, Options{
		// The shared loop ticks every reaperSweepFloor; a tiny check
		// interval makes every tick sweep this log.
		Retention: RetentionConfig{MaxAge: time.Hour, CheckInterval: time.Millisecond},
	})
	if err != nil {
		t.Fatalf("NewLog(%s) error = %v", dir, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// TestSharedReaperSurvivesAPanickingSweep pins the containment: a sweep
// that panics for one log is logged and skipped, the other logs in the
// same pass are still swept, and the loop keeps ticking afterwards with
// no replacement needed.
func TestSharedReaperSurvivesAPanickingSweep(t *testing.T) {
	dir := t.TempDir()
	bad := openRetainedLog(t, filepath.Join(dir, "bad"))
	good := openRetainedLog(t, filepath.Join(dir, "good"))

	var badSweeps, goodSweeps atomic.Int32
	withSweepHook(t, func(l *Log) {
		switch l {
		case bad:
			if badSweeps.Add(1) == 1 {
				panic("retention: injected failure")
			}
		case good:
			goodSweeps.Add(1)
		}
	})

	restartsBefore := sharedReaper.restarts.Load()
	waitFor(t, 6*time.Second, "after one sweep panicked the loop did not keep sweeping both logs", func() bool {
		return badSweeps.Load() >= 2 && goodSweeps.Load() >= 2
	})
	if got := sharedReaper.restarts.Load(); got != restartsBefore {
		t.Fatalf("restarts = %d, want %d: a contained panic must not need a replacement loop", got, restartsBefore)
	}
	if n := reaperLoops(); n != 1 {
		t.Fatalf("shared retention loops alive = %d, want exactly 1", n)
	}
}

// TestSharedReaperReplacesAStalledLoop pins the resurrection: when the
// loop stops ticking, the next registration starts a replacement, the
// old loop exits on its next tick, and exactly one loop is left
// sweeping.
func TestSharedReaperReplacesAStalledLoop(t *testing.T) {
	// Replacements are capped process-wide (maxReaperRestarts); this test
	// spends one per run, so a repeated run (-count) could exhaust it.
	if sharedReaper.restarts.Load() >= maxReaperRestarts-1 {
		t.Skip("shared reaper restart budget exhausted in this process")
	}
	dir := t.TempDir()
	_ = openRetainedLog(t, filepath.Join(dir, "seed")) // guarantees the loop exists
	restartsBefore := sharedReaper.restarts.Load()

	// Pretend the loop has not ticked for a long time. The live loop
	// re-stamps lastTick on every tick, so a tick landing between the
	// backdate and the register would hide the stall; a few attempts
	// make the test deterministic without stopping the loop.
	var watched *Log
	for attempt := 0; attempt < 5 && sharedReaper.restarts.Load() == restartsBefore; attempt++ {
		sharedReaper.lastTick.Store(reaperNow() - int64(2*reaperStallAfter))
		watched = openRetainedLog(t, filepath.Join(dir, fmt.Sprintf("watched-%d", attempt))) // register -> replacement
	}
	if got := sharedReaper.restarts.Load(); got != restartsBefore+1 {
		t.Fatalf("restarts = %d, want %d: a stalled loop was not replaced on register", got, restartsBefore+1)
	}

	var sweeps atomic.Int32
	withSweepHook(t, func(l *Log) {
		if l == watched {
			sweeps.Add(1)
		}
	})
	waitFor(t, 6*time.Second, "the replacement loop is not sweeping", func() bool { return sweeps.Load() >= 2 })
	// The superseded loop exits on its next tick; the replacement is the
	// only one left.
	waitFor(t, 4*time.Second, "more than one retention loop stayed alive after the replacement", func() bool { return reaperLoops() == 1 })
	if got := ReaperRestarts(); got < 1 {
		t.Fatalf("ReaperRestarts() = %d, want >= 1", got)
	}
}

// TestSweepRetentionNowIgnoresLogsWithoutAgeBound pins the guard on the
// synchronous sweep the cold walk calls: a nil log and a log with no
// age bound (nothing to reap) both return without touching anything, so
// the walk can call it unconditionally on whatever it opened.
func TestSweepRetentionNowIgnoresLogsWithoutAgeBound(t *testing.T) {
	var nilLog *Log
	nilLog.SweepRetentionNow() // must not panic

	l, err := NewLog(t.TempDir(), Options{Retention: RetentionConfig{MaxAge: 0, CheckInterval: time.Millisecond}})
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	if _, err := l.Append([]byte("keep")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	l.SweepRetentionNow()
	if got := l.NextOffset(); got != 1 {
		t.Fatalf("NextOffset after a sweep on a keep-forever log = %d, want 1 (nothing rolled or removed)", got)
	}
}

// TestSharedReaperSurvivesAPanickingPass covers the outer containment: a
// panic in the collect phase (here a log whose clock panics once) is
// recovered by sweepDueSafe with the pool lock released, and the loop
// keeps ticking without a replacement.
func TestSharedReaperSurvivesAPanickingPass(t *testing.T) {
	dir := t.TempDir()
	var calls atomic.Int32
	l, err := NewLog(filepath.Join(dir, "clock"), Options{
		Retention: RetentionConfig{MaxAge: time.Hour, CheckInterval: time.Millisecond, Now: func() time.Time {
			if calls.Add(1) == 3 {
				panic("retention: injected clock failure")
			}
			return time.Now()
		}},
	})
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	restartsBefore := sharedReaper.restarts.Load()
	waitFor(t, 8*time.Second, "the loop stopped after a pass panicked", func() bool { return calls.Load() >= 6 })
	// The pool lock must not have been left held by the panicking pass.
	done := make(chan struct{})
	go func() { _ = len(sharedReaper.snapshot()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the pool lock stayed held after the panicking pass")
	}
	if got := sharedReaper.restarts.Load(); got != restartsBefore {
		t.Fatalf("restarts = %d, want %d: a contained pass panic must not need a replacement", got, restartsBefore)
	}
	if n := reaperLoops(); n != 1 {
		t.Fatalf("retention loops alive = %d, want 1", n)
	}
}
