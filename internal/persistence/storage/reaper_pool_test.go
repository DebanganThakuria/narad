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
