package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// wp3HWMOpts is a log with an eager flusher, so passes are cheap.
func wp3HWMOpts(m MetricsRecorder) Options {
	return Options{
		FlushInterval: 5 * time.Millisecond,
		Metrics:       m,
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

// wp3Committer commits tagged batches to a log and remembers what each
// committed offset holds.
type wp3Committer struct {
	t    *testing.T
	want map[int64]string
}

func newWP3Committer(t *testing.T) *wp3Committer {
	return &wp3Committer{t: t, want: make(map[int64]string)}
}

func (c *wp3Committer) commit(l *Log, tag string, n int) {
	c.t.Helper()
	first, last := wp3CommitBatch(c.t, l, wp3Batch(tag, n))
	for off := first; off <= last; off++ {
		c.want[off] = fmt.Sprintf("%s-%03d", tag, off-first)
	}
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

// wp3PreviousBinaryHighWatermark is the boundary the binaries before
// this change recover from dir, given the recovered record tail: a
// missing or empty hwm file means the tail, 8 bytes mean min(file,
// tail). It reads the raw file, as that binary would, so it must run
// before anything reopens the directory.
func wp3PreviousBinaryHighWatermark(t *testing.T, dir string, tail int64) int64 {
	t.Helper()
	data, err := os.ReadFile(hwmFilePath(dir))
	if errors.Is(err, os.ErrNotExist) || len(data) == 0 {
		return tail
	}
	if err != nil || len(data) != 8 {
		t.Fatalf("hwm file = (%d bytes, %v): the previous binary would refuse to open the log", len(data), err)
	}
	return min(max(int64(binary.BigEndian.Uint64(data)), 0), tail)
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

// wp3CleanLife gives dir a previous, cleanly closed life with committed
// records, so the next open finds an exact hwm file.
func wp3CleanLife(t *testing.T, dir string, c *wp3Committer) {
	t.Helper()
	l, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		c.commit(l, fmt.Sprint("clean", i), 5)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadPersistedHighWatermark(dir); err != nil || !ok || got != int64(len(c.want)) {
		t.Fatalf("after the clean life the file holds (%d, %v, %v), want %d", got, ok, err, len(c.want))
	}
}

// A commit fsyncs its segment and nothing else: the high-watermark file
// is emptied once, before the log's first advance, and not touched by
// any commit after that.
func TestWP3CommitDoesNotPersistHighWatermark(t *testing.T) {
	m := &wp3Metrics{}
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), wp3HWMOpts(m))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const commits = 20
	for i := range commits {
		wp3CommitBatch(t, l, wp3Batch(fmt.Sprint(i), 3))
	}
	if got := m.hwmN.Load(); got != 1 {
		t.Fatalf("%d high-watermark file operations over %d commits, want 1 (the release)", got, commits)
	}
	if got := m.fsyncN.Load(); got != commits {
		t.Fatalf("%d segment fsyncs over %d commits, want one each", got, commits)
	}
	// What a restart would recover still covers every commit.
	if got, err := l.PersistedHighWatermark(); err != nil || got != commits*3 {
		t.Fatalf("PersistedHighWatermark = (%d, %v), want %d", got, err, commits*3)
	}
}

// The regression the release exists for. A crash of this binary must
// leave a directory that the binaries before it recover without hiding
// an acked record, since a rollback after a crash is a normal operation.
// Those binaries trust an 8-byte hwm file (min(file, tail)), and their
// failed-commit discard truncates everything above that boundary, so a
// file that lagged the committed records would make them hide and then
// destroy acked records. The previous clean life leaves such a file
// behind (its exact boundary); the commits after the reopen must not
// leave it standing, under a process kill or a power loss.
func TestWP3PreviousBinaryRecoversEveryCommitAfterCrash(t *testing.T) {
	for _, crash := range []string{"kill", "power-loss"} {
		t.Run(crash, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			c := newWP3Committer(t)
			wp3CleanLife(t, dir, c)

			inj := faulttest.New(t)
			l, err := NewLog(dir, wp3HWMOpts(nil))
			if err != nil {
				t.Fatal(err)
			}
			inj.LieSyncs(dir, 1<<30, 1, true) // every sync honest, and recorded
			for i := range 5 {
				c.commit(l, fmt.Sprint("after", i), 5)
			}
			committed := int64(len(c.want))

			switch crash {
			case "kill":
				// The page cache survives a killed process: the files as
				// they are now are what the next binary finds.
				if got := wp3PreviousBinaryHighWatermark(t, dir, committed); got < committed {
					t.Fatalf("the previous binary would recover the boundary %d after a kill, hiding acked offsets %d..%d",
						got, got, committed-1)
				}
				_ = l.Close()
			case "power-loss":
				wp3PowerLoss(t, inj, l)
				if got := wp3PreviousBinaryHighWatermark(t, dir, committed); got < committed {
					t.Fatalf("the previous binary would recover the boundary %d after a power loss, hiding acked offsets %d..%d",
						got, got, committed-1)
				}
				l2, err := NewLog(dir, wp3HWMOpts(nil))
				if err != nil {
					t.Fatal(err)
				}
				defer l2.Close()
				wp3AssertVisibleOnce(t, l2, c.want)
			}
		})
	}
}

// A power loss at any point hides no record a commit made visible,
// whether the log started empty or from a clean previous life whose
// exact boundary the file held. Nothing rewrites the file at the reopen
// (an open log answers from memory), and Close then writes the exact
// boundary for readers of the closed log.
func TestWP3CrashNeverHidesVisibleRecord(t *testing.T) {
	for _, seed := range []bool{false, true} {
		t.Run(fmt.Sprintf("clean-previous-life=%v", seed), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			c := newWP3Committer(t)
			if seed {
				wp3CleanLife(t, dir, c)
			}
			inj := faulttest.New(t)
			l, err := NewLog(dir, wp3HWMOpts(nil))
			if err != nil {
				t.Fatal(err)
			}
			inj.LieSyncs(dir, 1<<30, 1, true)
			for i := range 6 {
				c.commit(l, fmt.Sprint("b", i), 5)
			}
			// Appended, never committed: not acked, may or may not survive.
			if _, err := l.Append([]byte("uncommitted")); err != nil {
				t.Fatal(err)
			}

			wp3PowerLoss(t, inj, l)
			if got, ok, _ := ReadPersistedHighWatermark(dir); ok {
				t.Fatalf("file holds %d after the crash, want it empty: the test proves nothing", got)
			}

			l2, err := NewLog(dir, wp3HWMOpts(nil))
			if err != nil {
				t.Fatal(err)
			}
			if l2.HighWatermark() < int64(len(c.want)) {
				t.Fatalf("recovered high-watermark %d hides committed records (%d committed)", l2.HighWatermark(), len(c.want))
			}
			if got, ok, err := ReadPersistedHighWatermark(dir); err != nil || ok {
				t.Fatalf("file after reopen = (%d, %v, %v), want it untouched (empty)", got, ok, err)
			}
			hwm := l2.HighWatermark()
			if err := l2.Close(); err != nil {
				t.Fatal(err)
			}
			if got, ok, err := ReadPersistedHighWatermark(dir); err != nil || !ok || got != hwm {
				t.Fatalf("file after Close = (%d, %v, %v), want %d", got, ok, err, hwm)
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
	l, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	inj.LieSyncs(dir, 1<<30, 1, true)

	c := newWP3Committer(t)
	c.commit(l, "ok", 4)

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

	c.commit(l, "retry", 4)
	if c.want[f] != "retry-000" {
		t.Fatalf("retry did not land at %d: %q", f, c.want[f])
	}

	wp3PowerLoss(t, inj, l)
	l2, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	wp3AssertVisibleOnce(t, l2, c.want)
	if got := l2.NextOffset(); got != l2.HighWatermark() {
		t.Fatalf("NextOffset %d past the recovered high-watermark %d: a discarded frame came back", got, l2.HighWatermark())
	}
}

// Close writes the exact high-watermark: for a log that started empty,
// for one that started from a clean previous life, and for one opened
// after a crash that commits nothing before it is closed again (an idle
// eviction), so readers of the closed log see every visible record.
func TestWP3CloseWritesExactHighWatermark(t *testing.T) {
	for _, start := range []string{"empty", "clean", "crashed"} {
		t.Run(start, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			c := newWP3Committer(t)
			switch start {
			case "clean":
				wp3CleanLife(t, dir, c)
			case "crashed":
				inj := faulttest.New(t)
				l, err := NewLog(dir, wp3HWMOpts(nil))
				if err != nil {
					t.Fatal(err)
				}
				inj.LieSyncs(dir, 1<<30, 1, true)
				c.commit(l, "before-crash", 5)
				wp3PowerLoss(t, inj, l)
			}
			l, err := NewLog(dir, wp3HWMOpts(nil))
			if err != nil {
				t.Fatal(err)
			}
			if start != "crashed" {
				c.commit(l, "a", 3)
				c.commit(l, "b", 4)
			}
			hwm := l.HighWatermark()
			if hwm != int64(len(c.want)) {
				t.Fatalf("high-watermark %d, want %d", hwm, len(c.want))
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if got, ok, err := ReadPersistedHighWatermark(dir); err != nil || !ok || got != hwm {
				t.Fatalf("after Close = (%d, %v, %v), want exactly %d", got, ok, err, hwm)
			}
		})
	}
}

// A clean restart keeps a hidden tail hidden: records appended and
// flushed by Close but never committed stay invisible behind the exact
// boundary Close wrote.
func TestWP3HiddenTailStaysHiddenAcrossCleanRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	l, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	c := newWP3Committer(t)
	c.commit(l, "a", 2)
	if _, _, err := l.AppendBatch(wp3Batch("hidden", 3)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if got := l2.NextOffset(); got != 5 {
		t.Fatalf("NextOffset %d, want 5 (the hidden tail was flushed)", got)
	}
	wp3AssertVisibleOnce(t, l2, c.want)
}

// A rebalance copy carries the source's boundary in the hwm file
// (WritePersistedHighWatermark), and opening the copy keeps the records
// past it hidden, across a later close and reopen too.
func TestWP3StagedCopyKeepsSourceBoundary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	l, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		wp3CommitBatch(t, l, wp3Batch(fmt.Sprint(i), 5))
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := WritePersistedHighWatermark(dir, 7); err != nil {
		t.Fatal(err)
	}
	for life := range 2 {
		l, err := NewLog(dir, wp3HWMOpts(nil))
		if err != nil {
			t.Fatal(err)
		}
		if got := l.HighWatermark(); got != 7 {
			t.Fatalf("life %d: high-watermark %d, want the carried 7", life, got)
		}
		if got := l.NextOffset(); got != 10 {
			t.Fatalf("life %d: NextOffset %d, want 10 (every record copied)", life, got)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// An unusable hwm path fails the log's first commit (the release cannot
// empty it), which discards the batch so the ingress WAL's retry lands
// one copy, and leaves Close with nothing to write. Once the path is
// usable again the retry commits.
func TestWP3UnusableHighWatermarkPathFailsFirstCommit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	l, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(hwmFilePath(dir), 0o700); err != nil { // not a file
		t.Fatal(err)
	}
	first, last, err := l.AppendBatch(wp3Batch("a", 3))
	if err != nil {
		t.Fatal(err)
	}
	err = l.CommitDurable(first, last)
	if err == nil || !strings.Contains(err.Error(), "hwm") {
		t.Fatalf("CommitDurable = %v, want the hwm release failure", err)
	}
	if got := l.HighWatermark(); got != 0 {
		t.Fatalf("high-watermark %d, want 0 (nothing exposed)", got)
	}
	if got := l.NextOffset(); got != 0 {
		t.Fatalf("NextOffset %d, want 0 (the batch discarded)", got)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrOffsetNotFound) {
		t.Fatalf("Read(0) = %v, want ErrOffsetNotFound", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close = %v, want nil: nothing was exposed, so nothing is owed", err)
	}

	if err := os.Remove(hwmFilePath(dir)); err != nil {
		t.Fatal(err)
	}
	l2, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	c := newWP3Committer(t)
	c.commit(l2, "a", 3)
	wp3AssertVisibleOnce(t, l2, c.want)
}

// Once released, the hwm file is not touched until Close: breaking it
// fails no commit. Close reports it, and a reopen once it is fixed
// recovers every record from the tail.
func TestWP3BrokenHighWatermarkFileAfterReleaseFailsNoCommit(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p0")
	l, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	c := newWP3Committer(t)
	c.commit(l, "first", 2)
	if err := os.Remove(hwmFilePath(dir)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(hwmFilePath(dir), 0o700); err != nil { // not a file
		t.Fatal(err)
	}
	for i := range 3 {
		c.commit(l, fmt.Sprint(i), 2)
	}
	wp3AssertVisibleOnce(t, l, c.want)
	if err := l.Close(); err == nil {
		t.Fatal("Close = nil, want the failed high-watermark persist")
	}

	if err := os.Remove(hwmFilePath(dir)); err != nil {
		t.Fatal(err)
	}
	l2, err := NewLog(dir, wp3HWMOpts(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	wp3AssertVisibleOnce(t, l2, c.want)
}

// Opening a log writes nothing to the hwm file, whatever it holds: the
// open path of an idle-evicted partition's next reader stays free of
// syncs, and a read-only open leaves the closed log's boundary in place.
func TestWP3OpenWritesNothing(t *testing.T) {
	for _, start := range []string{"clean", "crashed"} {
		t.Run(start, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "p0")
			c := newWP3Committer(t)
			inj := faulttest.New(t)
			switch start {
			case "clean":
				wp3CleanLife(t, dir, c)
			case "crashed":
				l, err := NewLog(dir, wp3HWMOpts(nil))
				if err != nil {
					t.Fatal(err)
				}
				inj.LieSyncs(dir, 1<<30, 1, true)
				c.commit(l, "a", 5)
				wp3PowerLoss(t, inj, l)
			}
			var rules []*faulttest.Rule
			for _, op := range []syncfile.Op{syncfile.OpOpen, syncfile.OpWrite, syncfile.OpTruncate, syncfile.OpSync, syncfile.OpSyncData} {
				rules = append(rules, inj.FailFrom(op, hwmFileName, 1, syscall.EIO))
			}
			l, err := NewLog(dir, wp3HWMOpts(nil))
			if err != nil {
				t.Fatalf("NewLog: %v", err)
			}
			for _, r := range rules {
				if r.Fired() != 0 {
					t.Fatalf("the open touched the hwm file: %s", faulttest.Describe(inj.Events()))
				}
				inj.Remove(r)
			}
			wp3AssertVisibleOnce(t, l, c.want)
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
