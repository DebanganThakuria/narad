package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// timedRetentionOpts: no size-based roll, explicit commits, and a fake
// clock shared by the flusher (segment write times) and the reaper.
func timedRetentionOpts(t *testing.T, clock *atomicTime, cfg RetentionConfig) Options {
	t.Helper()
	cfg.Now = clock.Get
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = time.Hour
	}
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.SegmentBytes = 1 << 30
	opts.Retention = cfg
	return opts
}

func appendCommit(t *testing.T, l *Log, payload string) int64 {
	t.Helper()
	off, err := l.Append([]byte(payload))
	if err != nil {
		t.Fatalf("Append(%q): %v", payload, err)
	}
	if err := l.CommitDurable(off, off); err != nil {
		t.Fatalf("CommitDurable(%d): %v", off, err)
	}
	return off
}

// A partition that never fills a segment used to keep its oldest record
// until the segment filled, then for one more retention period (audit
// finding 2.3). Now the active segment rolls before the first write that
// finds its oldest record older than MaxSegmentAge (MaxAge by default),
// so the sealed segment is reaped on the next sweep after that.
func TestTimeBasedRollBoundsRecordLifetime(t *testing.T) {
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	const maxAge = 10 * time.Minute
	opts := timedRetentionOpts(t, clock, RetentionConfig{MaxAge: maxAge})
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	off0 := appendCommit(t, l, "old")
	clock.Set(t0.Add(5 * time.Minute))
	off1 := appendCommit(t, l, "still-young")
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after 5m = %d, want 1 (roll age not reached)", got)
	}

	// 11 minutes after the oldest record: the next write rolls first.
	clock.Set(t0.Add(11 * time.Minute))
	off2 := appendCommit(t, l, "new")
	if got := l.SegmentCount(); got != 2 {
		t.Fatalf("segments after aged write = %d, want 2 (time-based roll)", got)
	}
	l.rwmu.RLock()
	sealed, active := l.segments[0], l.segments[1]
	l.rwmu.RUnlock()
	if sealed.nextOffset != off2 || active.baseOffset != off2 {
		t.Fatalf("roll boundary: sealed ends at %d, active starts at %d, want %d", sealed.nextOffset, active.baseOffset, off2)
	}
	if !sealed.lastWriteAt.Equal(t0.Add(5 * time.Minute)) {
		t.Fatalf("sealed lastWriteAt = %v, want the last write at t0+5m", sealed.lastWriteAt)
	}

	// The sealed segment's last write (t0+5m) is 6 minutes old: not yet
	// reapable.
	l.reaper.sweep()
	if got := l.SegmentCount(); got != 2 {
		t.Fatalf("segments after early sweep = %d, want 2", got)
	}

	// At t0+16m the sealed segment's last write is older than MaxAge.
	clock.Set(t0.Add(16 * time.Minute))
	l.reaper.sweep()
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after sweep = %d, want 1", got)
	}
	for _, off := range []int64{off0, off1} {
		if _, err := l.Read(off); !errors.Is(err, ErrOffsetNotFound) {
			t.Fatalf("Read(%d) after retention: err=%v, want ErrOffsetNotFound", off, err)
		}
	}
	if got, err := l.Read(off2); err != nil || string(got) != "new" {
		t.Fatalf("Read(%d) = (%q, %v)", off2, got, err)
	}
}

// MaxSegmentAge can be set below MaxAge to tighten the bound, and a
// negative value disables the age-based roll.
func TestTimeBasedRollHonoursMaxSegmentAge(t *testing.T) {
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name          string
		maxSegmentAge time.Duration
		advance       time.Duration
		wantSegments  int
	}{
		{"tighter than max age", 2 * time.Minute, 3 * time.Minute, 2},
		{"default is max age", 0, 3 * time.Minute, 1},
		{"disabled", -1, 24 * time.Hour, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newAtomicTime(t0)
			opts := timedRetentionOpts(t, clock, RetentionConfig{MaxAge: 10 * time.Minute, MaxSegmentAge: tc.maxSegmentAge})
			l, err := NewLog(testLogPath(t), opts)
			if err != nil {
				t.Fatalf("NewLog: %v", err)
			}
			defer l.Close()
			appendCommit(t, l, "first")
			clock.Set(t0.Add(tc.advance))
			appendCommit(t, l, "second")
			if got := l.SegmentCount(); got != tc.wantSegments {
				t.Fatalf("segments = %d, want %d", got, tc.wantSegments)
			}
		})
	}
}

// A partition that stops writing never triggers the write-time roll, so
// the reaper rotates an active segment whose last write is older than
// MaxAge (all its records have expired) and reaps it in the same sweep.
func TestReaperRotatesIdleActiveSegment(t *testing.T) {
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	opts := timedRetentionOpts(t, clock, RetentionConfig{MaxAge: 10 * time.Minute})
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	off0 := appendCommit(t, l, "r0")
	off1 := appendCommit(t, l, "r1")

	clock.Set(t0.Add(9 * time.Minute))
	l.reaper.sweep()
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after 9m = %d, want 1 (nothing expired)", got)
	}
	if _, err := l.Read(off0); err != nil {
		t.Fatalf("Read(%d) before expiry: %v", off0, err)
	}

	clock.Set(t0.Add(11 * time.Minute))
	l.reaper.sweep()
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after rotation sweep = %d, want 1 (rotated segment reaped, fresh active)", got)
	}
	if got := l.OldestOffset(); got != off1+1 {
		t.Fatalf("OldestOffset after rotation = %d, want %d", got, off1+1)
	}
	for _, off := range []int64{off0, off1} {
		if _, err := l.Read(off); !errors.Is(err, ErrOffsetNotFound) {
			t.Fatalf("Read(%d) after rotation: err=%v, want ErrOffsetNotFound", off, err)
		}
	}
	// Writable, offsets continue.
	if off := appendCommit(t, l, "r2"); off != off1+1 {
		t.Fatalf("offset after rotation = %d, want %d", off, off1+1)
	}
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after write into the fresh active = %d, want 1", got)
	}
}

// The reaper never rotates an active segment that still holds records
// above the high-watermark: sealing them would put them out of reach of
// a failed commit's discard.
func TestReaperDoesNotRotateUncommittedTail(t *testing.T) {
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	opts := timedRetentionOpts(t, clock, RetentionConfig{MaxAge: 10 * time.Minute})
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	if _, err := l.Append([]byte("durable-but-hidden")); err != nil {
		t.Fatal(err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	clock.Set(t0.Add(time.Hour))
	l.reaper.sweep()
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments = %d, want 1", got)
	}
	if got := l.OldestOffset(); got != 0 {
		t.Fatalf("OldestOffset = %d, want 0 (hidden tail not rotated away)", got)
	}
	if got, err := l.Read(0); err != nil || string(got) != "durable-but-hidden" {
		t.Fatalf("Read(0) = (%q, %v)", got, err)
	}
}

// The reaper's age check uses the write time cached on the segment, so
// it costs no stat per sealed segment per sweep and is not fooled by a
// file whose mtime moved (a copy, a touch).
func TestReaperUsesCachedWriteTime(t *testing.T) {
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	opts := timedRetentionOpts(t, clock, RetentionConfig{MaxAge: 10 * time.Minute})
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	appendCommit(t, l, "r0")
	clock.Set(t0.Add(11 * time.Minute))
	appendCommit(t, l, "r1") // rolls: segment 0 sealed with lastWriteAt=t0
	if got := l.SegmentCount(); got != 2 {
		t.Fatalf("segments = %d, want 2", got)
	}
	l.rwmu.RLock()
	sealedPath := l.segments[0].path
	l.rwmu.RUnlock()
	// Bump the file's mtime far into the future: a stat-based reaper
	// would now keep the segment forever.
	future := time.Now().Add(365 * 24 * time.Hour)
	if err := os.Chtimes(sealedPath, future, future); err != nil {
		t.Fatal(err)
	}
	clock.Set(t0.Add(30 * time.Minute))
	l.reaper.sweep()
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after sweep = %d, want 1 (cached write time, not mtime)", got)
	}
	if _, err := os.Stat(sealedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sealed segment still on disk: %v", err)
	}
}

// countingRecorder is a MetricsRecorder that also counts storage errors.
type countingRecorder struct {
	mu     sync.Mutex
	errors map[string]int
	dels   int
}

func (c *countingRecorder) ObserveFlush(time.Duration, int64)                 {}
func (c *countingRecorder) ObserveFsync(time.Duration)                        {}
func (c *countingRecorder) ObserveHighWatermarkPersist(time.Duration, string) {}
func (c *countingRecorder) ObserveRetentionRun(time.Duration)                 {}
func (c *countingRecorder) IncRetentionDeletion(string, int64, int64) {
	c.mu.Lock()
	c.dels++
	c.mu.Unlock()
}

func (c *countingRecorder) IncStorageError(kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.errors == nil {
		c.errors = map[string]int{}
	}
	c.errors[kind]++
}

func (c *countingRecorder) count(kind string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errors[kind]
}

// A segment file the reaper cannot unlink is logged and counted instead
// of silently dropped from memory while it keeps consuming disk.
func TestReaperCountsUnlinkFailures(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	t0 := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	clock := newAtomicTime(t0)
	opts := timedRetentionOpts(t, clock, RetentionConfig{MaxAge: 10 * time.Minute})
	rec := &countingRecorder{}
	opts.Metrics = rec
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	appendCommit(t, l, "r0")
	clock.Set(t0.Add(11 * time.Minute))
	appendCommit(t, l, "r1") // rolls
	l.rwmu.RLock()
	sealedPath := l.segments[0].path
	l.rwmu.RUnlock()

	if err := os.Chmod(l.dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(l.dir, 0o700) })

	clock.Set(t0.Add(30 * time.Minute))
	l.reaper.sweep()
	if got := l.SegmentCount(); got != 1 {
		t.Fatalf("segments after sweep = %d, want 1 (detached from memory)", got)
	}
	if _, err := os.Stat(sealedPath); err != nil {
		t.Fatalf("segment file should still exist after the failed unlink: %v", err)
	}
	if got := rec.count("retention_unlink"); got != 1 {
		t.Fatalf("retention_unlink errors = %d, want 1", got)
	}
	if rec.dels != 0 {
		t.Fatalf("deletions counted = %d, want 0 for a failed unlink", rec.dels)
	}
}

// A failed commit's discard and an fsync poison are counted too.
func TestStorageErrorCounters(t *testing.T) {
	rec := &countingRecorder{}
	opts := slowFlushOpts(t, codec.NewNoopCodec())
	opts.Metrics = rec
	l, err := NewLog(testLogPath(t), opts)
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	if err := l.closeHWMFile(); err != nil {
		t.Fatal(err)
	}
	goodPath := l.hwmPath
	l.hwmPath = l.dir
	off, err := l.Append([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off, off); err == nil {
		t.Fatal("CommitDurable succeeded with an unwritable hwm path")
	}
	l.hwmPath = goodPath
	if got := rec.count("commit_discard"); got != 1 {
		t.Fatalf("commit_discard = %d, want 1", got)
	}

	fsyncHook = func(*segment) error { return errors.New("eio") }
	t.Cleanup(func() { fsyncHook = nil })
	off, err = l.Append([]byte("y"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off, off); !errors.Is(err, ErrLogPoisoned) {
		t.Fatalf("CommitDurable = %v, want ErrLogPoisoned", err)
	}
	if got := rec.count("fsync_poisoned"); got != 1 {
		t.Fatalf("fsync_poisoned = %d, want 1", got)
	}
}

func openSealedHandles(t *testing.T, l *Log) int {
	t.Helper()
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	n := 0
	for _, s := range l.segments[:len(l.segments)-1] {
		s.fmu.Lock()
		if s.file != nil {
			n++
		}
		s.fmu.Unlock()
	}
	return n
}

// Sealed segments do not pin a file descriptor for the life of the Log:
// recovery releases them after the scan, a read opens one lazily, and
// the handle goes away with the segment's index when it leaves the hot
// set (audit finding 2.4).
func TestSealedSegmentHandlesAreLazy(t *testing.T) {
	clock := newAtomicTime(time.Now())
	opts := retentionOpts(t, clock, RetentionConfig{})
	dir := testLogPath(t)
	const sealed = 6
	produceN(t, dir, opts, sealed) // sealed segments + 1 empty active

	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	if got := l.SegmentCount(); got != sealed+1 {
		t.Fatalf("segments = %d, want %d", got, sealed+1)
	}
	if got := openSealedHandles(t, l); got != 0 {
		t.Fatalf("open sealed handles after recovery = %d, want 0", got)
	}

	// Reading every sealed segment opens each lazily, but the hot set
	// bounds how many stay open.
	for i := range int64(sealed) {
		got, err := l.Read(i)
		if err != nil || string(got) != fmt.Sprintf("rec-%d", i) {
			t.Fatalf("Read(%d) = (%q, %v)", i, got, err)
		}
	}
	if got := openSealedHandles(t, l); got == 0 || got > maxHotSegmentIndexes {
		t.Fatalf("open sealed handles after reading all segments = %d, want 1..%d", got, maxHotSegmentIndexes)
	}
	// Reads of released segments still work (the handle is reopened).
	for i := range int64(sealed) {
		if _, err := l.Read(i); err != nil {
			t.Fatalf("second Read(%d): %v", i, err)
		}
	}
}

// A read racing the release of the segment handle it captured retries
// and succeeds instead of surfacing os.ErrClosed.
func TestReadRetriesAfterHandleRelease(t *testing.T) {
	clock := newAtomicTime(time.Now())
	opts := retentionOpts(t, clock, RetentionConfig{})
	dir := testLogPath(t)
	produceN(t, dir, opts, 3)
	l, err := NewLog(dir, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()

	if _, err := l.Read(0); err != nil {
		t.Fatal(err)
	}
	l.rwmu.RLock()
	seg := l.segments[0]
	l.rwmu.RUnlock()
	if err := seg.release(); err != nil {
		t.Fatal(err)
	}
	// The frame cache would satisfy the read; drop it so the read must
	// go to the (released) file.
	l.frameCache.invalidateSegment(seg.baseOffset)
	if got, err := l.Read(0); err != nil || string(got) != "rec-0" {
		t.Fatalf("Read(0) after release = (%q, %v)", got, err)
	}
}
