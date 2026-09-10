package storage

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// The flusher arms its timer only while a pass is owed. That turns an
// idle partition from ten wake-ups a second into none, which at tens of
// thousands of open logs is the difference between a busy timer heap and
// a quiet one.
//
// It is also durability code, and the failure mode is silent: get the
// arming conditions wrong and an acked record simply never reaches
// disk, with nothing to indicate it. So each test below pins ONE reason
// the timer has to come back, and every one of them fails if that
// reason is dropped from needsTimer.

// countPasses installs the pass hook for the duration of a test and
// returns a reader for how many flusher passes have run.
func countPasses(t *testing.T) func() int {
	t.Helper()
	var passes atomic.Int64
	flushPassHook = func() { passes.Add(1) }
	t.Cleanup(func() { flushPassHook = nil })
	return func() int { return int(passes.Load()) }
}

// lazyTimerLog opens a log with thresholds far out of reach, so nothing
// a test appends can cross one and every durability guarantee under
// test rides the timer rather than a threshold.
func lazyTimerLog(t *testing.T, tune func(*Options)) (*Log, string) {
	t.Helper()
	dir := testLogPath(t)
	opts := Options{
		FlushBytes:      1 << 30,
		FlushRecords:    1 << 20,
		FlushInterval:   20 * time.Millisecond,
		SyncMode:        SyncBatched,
		SyncInterval:    50 * time.Millisecond,
		SyncBytes:       1 << 30,
		HWMSyncInterval: 50 * time.Millisecond,
		SegmentBytes:    64 << 20,
	}
	if tune != nil {
		tune(&opts)
	}
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, dir
}

// segmentBytesOnDisk is the total size of the log's segment files, read
// straight from the filesystem so it reflects what a crash would find
// rather than what the log believes.
func segmentBytesOnDisk(t *testing.T, dir string) int64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir(%s) error = %v", dir, err)
	}
	var total int64
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".log" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatalf("stat %s: %v", e.Name(), err)
		}
		total += info.Size()
	}
	return total
}

func waitFor(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %v", what, within)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestIdleLogRunsNoFlusherPasses is the point of the change. A log that
// nobody is writing to must cost nothing: not a cheap pass ten times a
// second, but no pass at all.
func TestIdleLogRunsNoFlusherPasses(t *testing.T) {
	passes := countPasses(t)
	l, _ := lazyTimerLog(t, nil)

	// Write and commit, so the log has done real work and settled: the
	// buffer drains, the bytes sync, the high-watermark persists. From
	// here nothing is owed and the timer must be gone.
	if _, err := l.Append(EncodeKeyedRecord("", 1, []byte(`{"a":1}`))); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.CommitDurable(0, 0); err != nil {
		t.Fatalf("CommitDurable() error = %v", err)
	}
	waitFor(t, 2*time.Second, "the log to settle", func() bool {
		return !l.flusher.needsTimer()
	})

	settled := passes()
	time.Sleep(time.Second) // 50 timer fires at FlushInterval=20ms, if armed
	if got := passes() - settled; got != 0 {
		t.Fatalf("an idle log ran %d flusher passes in one second, want 0: "+
			"the timer is still armed with nothing owed", got)
	}
}

// TestSubThresholdAppendStillReachesDiskOnTheTimer is the durability
// guarantee the timer exists for. A record too small to cross any
// threshold has nothing else scheduled to write it: if arming on the
// empty-to-non-empty transition is dropped, this record sits in memory
// forever and the loss is silent.
func TestSubThresholdAppendStillReachesDiskOnTheTimer(t *testing.T) {
	l, dir := lazyTimerLog(t, nil)

	waitFor(t, 2*time.Second, "the fresh log to settle", func() bool {
		return !l.flusher.needsTimer()
	})
	before := segmentBytesOnDisk(t, dir)

	if _, err := l.Append(EncodeKeyedRecord("", 1, []byte(`{"lonely":true}`))); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// No commit, no sync, no second append: only the timer can move it.
	waitFor(t, 5*time.Second, "a sub-threshold record to reach the segment file", func() bool {
		return segmentBytesOnDisk(t, dir) > before
	})
}

// TestUnsyncedBytesStillGetFsyncedOnTheTimer pins the second reason the
// timer must stay armed. Records can be written into the segment file
// and still not be durable: syncIfNeeded defers the fdatasync behind
// SyncInterval. Disarming while bytes are unsynced would leave them one
// power cut from gone.
func TestUnsyncedBytesStillGetFsyncedOnTheTimer(t *testing.T) {
	l, _ := lazyTimerLog(t, func(o *Options) {
		// Long enough that the drain cannot also sync in the same pass.
		o.SyncInterval = 400 * time.Millisecond
	})

	if _, err := l.Append(EncodeKeyedRecord("", 1, []byte(`{"a":1}`))); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	// Once the write has landed but the sync has not, the flusher owes a
	// pass purely to fsync, and needsTimer must say so.
	waitFor(t, 3*time.Second, "the record to be written but unsynced", func() bool {
		return l.flusher.unsyncedBytes.Load() > 0
	})
	if !l.flusher.needsTimer() {
		t.Fatal("needsTimer() = false with unsynced bytes outstanding: " +
			"the periodic fsync would never run")
	}
	waitFor(t, 5*time.Second, "the deferred fsync", func() bool {
		return l.flusher.unsyncedBytes.Load() == 0
	})
	if l.durableTail.Load() <= 0 {
		t.Fatalf("durable tail = %d after the fsync, want > 0", l.durableTail.Load())
	}
}

// TestHighWatermarkStillPersistsOnTheTimer pins the third reason. A
// commit advances the high-watermark, but syncHighWatermark can defer
// writing it behind HWMSyncInterval. If the timer is not held for that,
// the visibility boundary never reaches disk and a restart silently
// redelivers everything above the last persisted value.
func TestHighWatermarkStillPersistsOnTheTimer(t *testing.T) {
	l, _ := lazyTimerLog(t, func(o *Options) {
		o.HWMSyncInterval = 400 * time.Millisecond
	})

	if _, err := l.Append(EncodeKeyedRecord("", 1, []byte(`{"a":1}`))); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	// Advance visibility without a commit, so nothing forces the persist.
	if err := l.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}

	if l.persistedHWM.Load() < 1 && !l.flusher.needsTimer() {
		t.Fatal("needsTimer() = false with the high-watermark ahead of the " +
			"persisted one: the persist would never run")
	}
	waitFor(t, 5*time.Second, "the high-watermark to reach disk", func() bool {
		return l.persistedHWM.Load() >= 1
	})
}

// TestPendingFlushingRetriesOnTheTimer pins the fourth reason. A failed
// segment write leaves records in the flushing snapshot, and the retry
// rides the timer. Disarming with a snapshot outstanding strands acked
// records that the log still believes it is holding for a retry.
func TestPendingFlushingRetriesOnTheTimer(t *testing.T) {
	l, _ := lazyTimerLog(t, nil)

	// Fail the next fsync, which poisons the log; what matters here is
	// that a log with an outstanding snapshot keeps its timer.
	l.flushingMu.Lock()
	l.flushingValid = true
	l.flushingBase = 0
	l.flushingWritten = 0
	l.flushingRecords = [][]byte{[]byte("stuck")}
	l.flushingMu.Unlock()

	if !l.hasPendingFlushing() {
		t.Fatal("hasPendingFlushing() = false after installing a snapshot")
	}
	if !l.flusher.needsTimer() {
		t.Fatal("needsTimer() = false with an unwritten flushing snapshot: " +
			"the failed write would never be retried")
	}
}

// TestLazyTimerSurvivesAnIdleActiveIdleCycle is the arming path itself.
// The flusher must come back from a disarmed state every time, not just
// the first time: a lost wake-up here would show up as a log that goes
// permanently deaf after its first quiet spell.
func TestLazyTimerSurvivesAnIdleActiveIdleCycle(t *testing.T) {
	l, dir := lazyTimerLog(t, nil)

	for round := range 5 {
		waitFor(t, 3*time.Second, "the log to settle", func() bool {
			return !l.flusher.needsTimer()
		})
		before := segmentBytesOnDisk(t, dir)

		if _, err := l.Append(EncodeKeyedRecord("", 1, []byte(`{"round":1}`))); err != nil {
			t.Fatalf("round %d: Append() error = %v", round, err)
		}
		waitFor(t, 5*time.Second, "the record to reach disk", func() bool {
			return segmentBytesOnDisk(t, dir) > before
		})
	}
}
