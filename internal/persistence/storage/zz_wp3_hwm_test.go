package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// wp3HWMOpts is a log whose deferred high-watermark persist waits for
// interval, with an eager flusher so passes are cheap to count.
func wp3HWMOpts(interval time.Duration, m MetricsRecorder) Options {
	return Options{
		FlushInterval:   5 * time.Millisecond,
		HWMSyncInterval: interval,
		Metrics:         m,
	}
}

// wp3Batch is n distinct records tagged with tag.
func wp3Batch(tag string, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = fmt.Appendf(nil, "%s-%03d", tag, i)
	}
	return out
}

// wp3AssertVisibleOnce checks every record in want is readable below the
// high-watermark at its offset, and that nothing else is.
func wp3AssertVisibleOnce(t *testing.T, l *Log, want map[int64]string) {
	t.Helper()
	hwm := l.HighWatermark()
	if hwm != int64(len(want)) {
		t.Fatalf("high-watermark %d, want %d (one per committed record)", hwm, len(want))
	}
	for off := range hwm {
		rec, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read(%d) below the high-watermark: %v", off, err)
		}
		if string(rec) != want[off] {
			t.Fatalf("Read(%d) = %q, want %q", off, rec, want[off])
		}
	}
}

// A commit fsyncs its segment and nothing else: the high-watermark file
// is persisted later, off the commit path.
func TestWP3CommitDoesNotPersistHighWatermark(t *testing.T) {
	m := &wp3Metrics{}
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), wp3HWMOpts(time.Hour, m))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const commits = 20
	for i := range commits {
		wp3CommitBatch(t, l, wp3Batch(fmt.Sprint(i), 3))
	}
	if got := m.hwmN.Load(); got != 0 {
		t.Fatalf("%d high-watermark persists over %d commits, want 0", got, commits)
	}
	if got := m.fsyncN.Load(); got != commits {
		t.Fatalf("%d segment fsyncs over %d commits, want one each", got, commits)
	}
	// What a restart would recover still covers every commit.
	if got, err := l.PersistedHighWatermark(); err != nil || got != commits*3 {
		t.Fatalf("PersistedHighWatermark = (%d, %v), want %d", got, err, commits*3)
	}
}

// Under a steady stream of commits the deferred persist still happens
// every HWMSyncInterval: a timer pushed out by each commit would never
// fire while the partition stays busy.
func TestWP3LazyPersistKeepsUpUnderSteadyCommits(t *testing.T) {
	m := &wp3Metrics{}
	const interval = 40 * time.Millisecond
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), wp3HWMOpts(interval, m))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	wp3NoFsync(t)

	deadline := time.Now().Add(10 * interval)
	var seenAt int64
	for time.Now().Before(deadline) {
		wp3CommitBatch(t, l, wp3Batch("s", 2))
		if time.Until(deadline) > 5*interval && seenAt == 0 {
			seenAt = l.HighWatermark()
		}
		time.Sleep(time.Millisecond)
	}
	persisted, ok, err := ReadPersistedHighWatermark(l.dir)
	if err != nil || !ok || persisted < seenAt {
		t.Fatalf("persisted high-watermark = (%d, %v, %v) after %v of commits, want at least %d, reached %v earlier",
			persisted, ok, err, 10*interval, seenAt, 5*interval)
	}
}

// Once commits stop, the deferred persist costs one timer pass, not a
// pass every FlushInterval until HWMSyncInterval has elapsed.
func TestWP3DeferredPersistTakesOneTimerPass(t *testing.T) {
	var passes atomic.Int64
	flushPassHook = func() { passes.Add(1) }
	t.Cleanup(func() { flushPassHook = nil })
	const interval = 150 * time.Millisecond
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), wp3HWMOpts(interval, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	wp3CommitBatch(t, l, wp3Batch("a", 3))
	passes.Store(0)
	waitFor(t, 5*time.Second, "the deferred high-watermark persist", func() bool {
		return l.persistedHWM.Load() == l.HighWatermark() && !l.flusher.needsTimer()
	})
	if got := passes.Load(); got > 2 {
		t.Fatalf("%d flusher passes for one deferred persist (FlushInterval %v, HWMSyncInterval %v), want at most 2",
			got, 5*time.Millisecond, interval)
	}
}

// Close writes the exact high-watermark, whether the file lagged or had
// never been written, so readers of the closed log see every record.
func TestWP3CloseWritesExactHighWatermark(t *testing.T) {
	for _, seed := range []bool{false, true} {
		t.Run(fmt.Sprintf("file-existed=%v", seed), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			l, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
			if err != nil {
				t.Fatal(err)
			}
			wp3CommitBatch(t, l, wp3Batch("a", 3))
			if seed {
				if err := l.Sync(); err != nil { // forced: writes the file
					t.Fatal(err)
				}
			}
			wp3CommitBatch(t, l, wp3Batch("b", 4))
			if got, ok, _ := ReadPersistedHighWatermark(dir); ok && got == l.HighWatermark() {
				t.Fatalf("file already at %d before Close: the test proves nothing", got)
			}
			hwm := l.HighWatermark()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if got, ok, err := ReadPersistedHighWatermark(dir); err != nil || !ok || got != hwm {
				t.Fatalf("after Close = (%d, %v, %v), want exactly %d", got, ok, err, hwm)
			}
		})
	}
}

// wp3PowerLoss takes l down the way a power loss would: from here on
// every file sync lies, so nothing the clean shutdown writes (the final
// high-watermark persist included) reaches the disk, and after it the
// files are cut back to their last honest sync. Directory syncs stay
// honest (a journaled metadata model), so a file created after the last
// honest data sync survives empty.
func wp3PowerLoss(t *testing.T, inj *faulttest.Injector, l *Log) {
	t.Helper()
	inj.LieSyncs(l.dir, 0, 1, true)
	_ = l.Close()
	if err := inj.Crash(l.dir); err != nil {
		t.Fatal(err)
	}
	inj.StopLying()
}

// A power loss at any point hides no record a commit made visible: the
// boundary file lags (it was never written, or written long before),
// and recovery takes the boundary from the record tail. The reopen then
// rewrites the file, which bounds how long readers of the closed log
// see a stale one.
func TestWP3CrashNeverHidesVisibleRecord(t *testing.T) {
	for _, seed := range []bool{false, true} {
		t.Run(fmt.Sprintf("file-existed=%v", seed), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			inj := faulttest.New(t)
			l, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
			if err != nil {
				t.Fatal(err)
			}
			inj.LieSyncs(dir, 1<<30, 1, true) // every sync honest, and recorded

			want := make(map[int64]string)
			commit := func(tag string) {
				first, last := wp3CommitBatch(t, l, wp3Batch(tag, 5))
				for off := first; off <= last; off++ {
					want[off] = fmt.Sprintf("%s-%03d", tag, off-first)
				}
			}
			commit("a")
			if seed {
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			for i := range 6 {
				commit(fmt.Sprint("b", i))
			}
			// Appended, never committed: not acked, may or may not survive.
			if _, err := l.Append([]byte("uncommitted")); err != nil {
				t.Fatal(err)
			}

			wp3PowerLoss(t, inj, l)
			if got, ok, _ := ReadPersistedHighWatermark(dir); ok && got >= int64(len(want)) {
				t.Fatalf("file holds %d after the crash, want it lagging: the test proves nothing", got)
			}

			l2, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
			if err != nil {
				t.Fatal(err)
			}
			defer l2.Close()
			wp3AssertVisibleOnce(t, l2, want)
			if got, ok, err := ReadPersistedHighWatermark(dir); err != nil || !ok || got != l2.HighWatermark() {
				t.Fatalf("file after reopen = (%d, %v, %v), want the recovered %d", got, ok, err, l2.HighWatermark())
			}
		})
	}
}

// A commit that fails in process still discards its tail, and the
// discard survives a power loss: after the retry commits, a crash and a
// reopen show the retry once and nothing of the failed attempt.
func TestWP3FailedCommitDiscardSurvivesCrash(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	inj := faulttest.New(t)
	l, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
	if err != nil {
		t.Fatal(err)
	}
	inj.LieSyncs(dir, 1<<30, 1, true)

	want := make(map[int64]string)
	first, last := wp3CommitBatch(t, l, wp3Batch("ok", 4))
	for off := first; off <= last; off++ {
		want[off] = fmt.Sprintf("ok-%03d", off-first)
	}

	wp3FailReadBackOnce(t, l)
	f, la, err := l.AppendBatch(wp3Batch("failed", 4))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(f, la); err == nil {
		t.Fatal("CommitDurable succeeded although its read-back failed")
	}
	if got := l.NextOffset(); got != f {
		t.Fatalf("NextOffset after the failed commit = %d, want %d (tail discarded)", got, f)
	}

	rf, rl := wp3CommitBatch(t, l, wp3Batch("retry", 4))
	if rf != f {
		t.Fatalf("retry landed at %d, want %d", rf, f)
	}
	for off := rf; off <= rl; off++ {
		want[off] = fmt.Sprintf("retry-%03d", off-rf)
	}

	wp3PowerLoss(t, inj, l)
	l2, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	wp3AssertVisibleOnce(t, l2, want)
	if got := l2.NextOffset(); got != l2.HighWatermark() {
		t.Fatalf("NextOffset %d past the recovered high-watermark %d: a discarded frame came back", got, l2.HighWatermark())
	}
}

// A broken high-watermark file no longer fails commits: they neither
// write nor need it. Close reports it, and a reopen once it is fixed
// recovers every record.
func TestWP3CommitSucceedsWithBrokenHighWatermarkFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	l, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(hwmFilePath(dir), 0o700); err != nil { // not a file
		t.Fatal(err)
	}
	want := make(map[int64]string)
	for i := range 3 {
		first, last := wp3CommitBatch(t, l, wp3Batch(fmt.Sprint(i), 2))
		for off := first; off <= last; off++ {
			want[off] = fmt.Sprintf("%d-%03d", i, off-first)
		}
	}
	wp3AssertVisibleOnce(t, l, want)
	if err := l.Close(); err == nil {
		t.Fatal("Close = nil, want the failed high-watermark persist")
	}

	if err := os.Remove(hwmFilePath(dir)); err != nil {
		t.Fatal(err)
	}
	l2, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	wp3AssertVisibleOnce(t, l2, want)
}

// A failed rewrite of the file at open does not fail the open: the
// recovered boundary is right in memory, and Close writes it once the
// disk is back.
func TestWP3OpenPersistFailureKeepsLogUsable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	l, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
	if err != nil {
		t.Fatal(err)
	}
	wp3CommitBatch(t, l, wp3Batch("a", 5))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WritePersistedHighWatermark(dir, 2); err != nil { // stale
		t.Fatal(err)
	}

	inj := faulttest.New(t)
	rule := inj.FailFrom(syncfile.OpOpen, hwmFileName, 1, syscall.EIO)
	l2, err := NewLog(dir, wp3HWMOpts(time.Hour, nil))
	if err != nil {
		t.Fatalf("NewLog with an unwritable high-watermark file: %v", err)
	}
	if rule.Fired() == 0 {
		t.Fatal("the open never tried to rewrite the stale file")
	}
	if got := l2.HighWatermark(); got != 5 {
		t.Fatalf("high-watermark %d, want the recovered 5", got)
	}
	inj.Remove(rule)
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadPersistedHighWatermark(dir); err != nil || !ok || got != 5 {
		t.Fatalf("after Close = (%d, %v, %v), want 5", got, ok, err)
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Fatal("unreachable")
	}
}
