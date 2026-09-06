package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// Disk-fault tests for the partition log, driven through the syncfile
// fault-injection seam under a concurrent append+commit load that
// mirrors the ingress dispatcher: a batch is appended, CommitDurable is
// called under a per-partition lock, and a failed commit is retried by
// appending the same records again (the WAL still owns them).
//
// Invariants asserted after every scenario and after every reopen:
//
//   - every record whose commit returned success is readable at the
//     offset it was committed at, and lies below the high-watermark;
//   - every record at an offset below the high-watermark is one of the
//     records handed to Append (never garbage, never a torn frame);
//   - the hidden tail above the high-watermark, if any, holds only
//     submitted records too, or reads as not-found;
//   - the in-memory index agrees with the segment files: walking the
//     frames on disk yields exactly the records Read returns.

// faultMetrics counts storage error kinds so a test can assert the node
// reported a poisoned log.
type faultMetrics struct {
	mu    sync.Mutex
	kinds map[string]int
}

func (m *faultMetrics) ObserveFlush(time.Duration, int64)                 {}
func (m *faultMetrics) ObserveFsync(time.Duration)                        {}
func (m *faultMetrics) ObserveHighWatermarkPersist(time.Duration, string) {}
func (m *faultMetrics) IncRetentionDeletion(string, int64, int64)         {}
func (m *faultMetrics) ObserveRetentionRun(time.Duration)                 {}
func (m *faultMetrics) IncStorageError(kind string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.kinds == nil {
		m.kinds = make(map[string]int)
	}
	m.kinds[kind]++
}

func (m *faultMetrics) count(kind string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.kinds[kind]
}

// faultOptions: small segments so rolls happen under load, a real codec
// so torn frames exercise the decoder, batched sync so the background
// flusher also syncs on its own.
func faultOptions(m MetricsRecorder) Options {
	c, err := codec.NewZstdCodec(1)
	if err != nil {
		panic(err)
	}
	return Options{
		Codec:         c,
		FlushBytes:    64 << 10,
		FlushRecords:  64,
		FlushInterval: 5 * time.Millisecond,
		SyncMode:      SyncBatched,
		SyncInterval:  20 * time.Millisecond,
		SegmentBytes:  24 << 10,
		Metrics:       m,
	}
}

// commitLoad is the ledger of a load run.
type commitLoad struct {
	tag       string
	mu        sync.Mutex
	submitted map[string]bool
	committed map[int64]string // offset -> payload, successful commits only
	// honest records, per successful commit, whether no sync lied while
	// it ran (only meaningful under LieSyncs).
	honest      map[int64]bool
	failures    int
	poisoned    int
	lastCommitE error
	errs        []error
	// allowHoles accepts an offset below the high-watermark that reads
	// as not-found or corrupt: the recorded loss a lying disk leaves
	// (see TestFaultLyingFsyncCrash). Never set for an honest disk.
	allowHoles bool
	holes      int
}

func newCommitLoad(tag string) *commitLoad {
	return &commitLoad{
		tag:       tag,
		submitted: make(map[string]bool),
		committed: make(map[int64]string),
		honest:    make(map[int64]bool),
	}
}

// inherit adds other's submitted records to c, for a load that runs on
// a log other loads have already filled: verifyLog must recognise the
// earlier records as legitimate.
func (c *commitLoad) inherit(other *commitLoad) *commitLoad {
	other.mu.Lock()
	defer other.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := range other.submitted {
		c.submitted[p] = true
	}
	return c
}

// faultPayload is one record: a readable tag and 96 pseudo-random bytes
// so zstd cannot fold a whole load into a single frame (rolls must
// happen under the load) and a torn frame is not accidentally valid.
func faultPayload(tag string, w, r, i int) []byte {
	rng := rand.New(rand.NewPCG(uint64(w)<<32|uint64(r), uint64(i)+uint64(len(tag))))
	out := fmt.Appendf(nil, "%s-w%02d-r%03d-i%02d-", tag, w, r, i)
	for range 96 {
		out = append(out, byte(rng.IntN(256)))
	}
	return out
}

// run drives workers goroutines, each appending rounds batches of
// batchSize records and committing every batch under commitMu. A
// failed commit is retried up to 3 times with the same records; a
// poisoned log stops the worker (the node refuses writes, the ingress
// WAL keeps the records). lies, when non-nil, is consulted around each
// commit to classify it as honest.
func (c *commitLoad) run(t *testing.T, l *Log, workers, rounds, batchSize int, lies func() int64) {
	t.Helper()
	var commitMu sync.Mutex
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for r := range rounds {
				batch := make([][]byte, batchSize)
				for i := range batch {
					batch[i] = faultPayload(c.tag, w, r, i)
				}
				c.mu.Lock()
				for _, b := range batch {
					c.submitted[string(b)] = true
				}
				c.mu.Unlock()
				for attempt := 0; attempt < 4; attempt++ {
					commitMu.Lock()
					owned := make([][]byte, len(batch))
					for i, b := range batch {
						owned[i] = append([]byte(nil), b...)
					}
					var liesBefore int64
					if lies != nil {
						liesBefore = lies()
					}
					first, last, err := l.AppendBatchOwned(owned)
					if err == nil {
						err = l.CommitDurable(first, last)
					}
					var liesAfter int64
					if lies != nil {
						liesAfter = lies()
					}
					commitMu.Unlock()
					c.mu.Lock()
					if err == nil {
						for i, b := range batch {
							c.committed[first+int64(i)] = string(b)
							c.honest[first+int64(i)] = liesAfter == liesBefore
						}
						c.mu.Unlock()
						break
					}
					c.failures++
					c.lastCommitE = err
					c.errs = append(c.errs, err)
					if errors.Is(err, ErrLogPoisoned) {
						c.poisoned++
						c.mu.Unlock()
						return
					}
					c.mu.Unlock()
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
}

// verifyLog checks the invariants listed at the top of the file.
func (c *commitLoad) verifyLog(t *testing.T, l *Log) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	hwm := l.HighWatermark()
	next := l.NextOffset()
	if hwm > next {
		t.Fatalf("hwm %d > next offset %d", hwm, next)
	}
	for off := l.OldestOffset(); off < hwm; off++ {
		rec, err := l.Read(off)
		if err != nil {
			if c.allowHoles && (errors.Is(err, ErrOffsetNotFound) || IsCorrupt(err)) {
				c.holes++
				continue
			}
			t.Fatalf("Read(%d) below hwm %d: %v", off, hwm, err)
		}
		if !c.submitted[string(rec)] {
			t.Fatalf("offset %d below hwm holds bytes never appended: %q", off, rec)
		}
		if want, ok := c.committed[off]; ok && want != string(rec) {
			t.Fatalf("offset %d holds %q, committed %q", off, rec, want)
		}
	}
	for off, want := range c.committed {
		if off >= hwm {
			t.Fatalf("committed offset %d is not visible (hwm %d)", off, hwm)
		}
		rec, err := l.Read(off)
		if err != nil || string(rec) != want {
			t.Fatalf("committed offset %d: got %q err=%v, want %q", off, rec, err, want)
		}
	}
	for off := hwm; off < next; off++ {
		rec, err := l.Read(off)
		if err != nil {
			if errors.Is(err, ErrOffsetNotFound) {
				continue
			}
			t.Fatalf("Read(%d) in hidden tail: %v", off, err)
		}
		if !c.submitted[string(rec)] {
			t.Fatalf("hidden tail offset %d holds bytes never appended: %q", off, rec)
		}
	}
	verifyIndexAgreesWithDisk(t, l)
}

// verifyIndexAgreesWithDisk walks every frame in every segment file and
// checks Read resolves each record to the bytes in that frame, so the
// sparse index, the nav cache and the file never drift apart. Frames
// above NextOffset (a hidden tail recovery refused) may be unreadable;
// everything below must match.
func verifyIndexAgreesWithDisk(t *testing.T, l *Log) {
	t.Helper()
	l.rwmu.RLock()
	type segView struct {
		path string
		size int64
		base int64
	}
	var segs []segView
	for _, s := range l.segments {
		segs = append(segs, segView{path: s.path, size: s.sizeBytes, base: s.baseOffset})
	}
	l.rwmu.RUnlock()
	next := l.NextOffset()
	var onDisk int64
	for _, sv := range segs {
		f, err := os.Open(sv.path)
		if err != nil {
			t.Fatalf("open %s: %v", sv.path, err)
		}
		pos := int64(0)
		for pos < sv.size {
			h, records, end, err := readFrameAt(f, pos, l)
			if err != nil {
				if errors.Is(err, errBadMagic) || IsCorrupt(err) || errors.Is(err, io.ErrUnexpectedEOF) {
					// Garbage a torn-tail scenario left behind; recovery
					// must have skipped it, so nothing below NextOffset
					// may live only there.
					pos = nextMagicInSegment(f, pos+1, sv.size)
					continue
				}
				t.Fatalf("frame at %s:%d: %v", sv.path, pos, err)
			}
			for i, rec := range records {
				off := h.baseOffset + int64(i)
				if off >= next {
					continue
				}
				onDisk++
				got, rerr := l.Read(off)
				if rerr != nil {
					t.Fatalf("index drift: offset %d is on disk in %s@%d but Read failed: %v", off, filepath.Base(sv.path), pos, rerr)
				}
				if !bytes.Equal(got, rec) {
					t.Fatalf("index drift: offset %d reads %q, file holds %q", off, got, rec)
				}
			}
			pos = end
		}
		f.Close()
	}
	t.Logf("index agrees with disk: %d records in %d segments, next=%d hwm=%d", onDisk, len(segs), next, l.HighWatermark())
}

func reopenLog(t *testing.T, dir string, m MetricsRecorder) *Log {
	t.Helper()
	l, err := NewLog(dir, faultOptions(m))
	if err != nil {
		t.Fatalf("reopen %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// The Nth fdatasync of a segment fails under load: the commit that ran
// into it fails, the log is poisoned (every later append and commit
// refuses with ErrLogPoisoned, counted as fsync_poisoned), reads of
// committed records keep working, and a reopen recovers the committed
// prefix and accepts the retried batches.
func TestFaultSegmentSyncFailsPoisonsLog(t *testing.T) {
	dir := testLogPath(t)
	m := &faultMetrics{}
	inj := faulttest.New(t)
	rule := inj.FailNth(syncfile.OpSyncData, segmentFileSuffix, 25, syscall.EIO)

	l, err := NewLog(dir, faultOptions(m))
	if err != nil {
		t.Fatal(err)
	}
	load := newCommitLoad("poison")
	load.run(t, l, 8, 12, 5, nil)

	if rule.Fired() != 1 {
		t.Fatalf("sync fault fired %d times, want 1", rule.Fired())
	}
	perr := l.Poisoned()
	if perr == nil || !errors.Is(perr, ErrLogPoisoned) || !errors.Is(perr, syscall.EIO) {
		t.Fatalf("Poisoned() = %v, want ErrLogPoisoned wrapping EIO", perr)
	}
	if load.poisoned == 0 {
		t.Fatal("no worker observed ErrLogPoisoned")
	}
	if got := m.count("fsync_poisoned"); got != 1 {
		t.Fatalf("fsync_poisoned counted %d times, want 1", got)
	}
	// Refuses, does not continue.
	if _, err := l.Append([]byte("after-poison")); !errors.Is(err, ErrLogPoisoned) {
		t.Fatalf("Append on a poisoned log: %v, want ErrLogPoisoned", err)
	}
	if err := l.CommitDurable(0, 0); !errors.Is(err, ErrLogPoisoned) {
		t.Fatalf("CommitDurable on a poisoned log: %v, want ErrLogPoisoned", err)
	}
	hwmBefore := l.HighWatermark()
	load.verifyLog(t, l)
	_ = l.Close()

	l2 := reopenLog(t, dir, m)
	if l2.Poisoned() != nil {
		t.Fatal("reopened log is still poisoned")
	}
	if got := l2.HighWatermark(); got != hwmBefore {
		t.Fatalf("hwm after reopen = %d, want %d", got, hwmBefore)
	}
	load.verifyLog(t, l2)

	// The dispatcher's retry after the reopen: more batches commit.
	committedBefore, poisonedBefore := len(load.committed), load.poisoned
	load.run(t, l2, 4, 6, 5, nil)
	if load.poisoned != poisonedBefore || len(load.committed) <= committedBefore {
		t.Fatalf("after reopen: committed %d (was %d), poisoned workers %d (was %d)", len(load.committed), committedBefore, load.poisoned, poisonedBefore)
	}
	load.verifyLog(t, l2)
	t.Logf("committed=%d failures=%d hwm=%d next=%d", len(load.committed), load.failures, l2.HighWatermark(), l2.NextOffset())
}

// The Nth fdatasync of the high-watermark file fails (the partition's
// visibility index): the commit fails and discards its tail, the log is
// NOT poisoned (the 8-byte file is rewritten whole on the next persist),
// the retry lands exactly one copy, and nothing is delivered twice.
func TestFaultHWMSyncFailsCommitRetriesExactlyOnce(t *testing.T) {
	dir := testLogPath(t)
	m := &faultMetrics{}
	inj := faulttest.New(t)
	rules := []*faulttest.Rule{
		inj.FailNth(syncfile.OpSyncData, hwmFileName, 7, syscall.EIO),
		inj.FailNth(syncfile.OpSyncData, hwmFileName, 30, syscall.EIO),
		inj.FailNth(syncfile.OpWrite, hwmFileName, 45, syscall.EIO),
	}

	l, err := NewLog(dir, faultOptions(m))
	if err != nil {
		t.Fatal(err)
	}
	load := newCommitLoad("hwm")
	load.run(t, l, 8, 12, 4, nil)
	for i, r := range rules {
		if r.Fired() != 1 {
			t.Fatalf("hwm fault %d fired %d times, want 1", i, r.Fired())
		}
	}
	if load.failures == 0 {
		t.Fatal("no commit observed the hwm persist failure")
	}
	if load.poisoned != 0 || l.Poisoned() != nil {
		t.Fatalf("hwm persist failure must not poison the log: poisoned workers=%d err=%v", load.poisoned, l.Poisoned())
	}
	if got := m.count("commit_discard"); got == 0 {
		t.Fatal("failed commits did not count commit_discard")
	}
	assertExactlyOnce(t, l, load)
	load.verifyLog(t, l)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l2 := reopenLog(t, dir, m)
	assertExactlyOnce(t, l2, load)
	load.verifyLog(t, l2)
}

// assertExactlyOnce checks every submitted record is visible exactly
// once: a failed commit's discarded copy never resurfaces next to the
// retry's copy.
func assertExactlyOnce(t *testing.T, l *Log, load *commitLoad) {
	t.Helper()
	load.mu.Lock()
	defer load.mu.Unlock()
	hwm := l.HighWatermark()
	seen := make(map[string]int)
	for off := int64(0); off < hwm; off++ {
		rec, err := l.Read(off)
		if err != nil {
			t.Fatalf("Read(%d): %v", off, err)
		}
		seen[string(rec)]++
	}
	for p := range load.submitted {
		if seen[p] != 1 {
			t.Fatalf("record %q visible %d times, want exactly once", p, seen[p])
		}
	}
	if int64(len(seen)) != hwm {
		t.Fatalf("%d distinct records below hwm %d", len(seen), hwm)
	}
}

// A segment write fails mid-batch with ENOSPC: the commit fails and
// discards, the retry (the disk has space again) lands exactly one copy
// at the same offsets.
func TestFaultSegmentWriteFailsMidBatch(t *testing.T) {
	dir := testLogPath(t)
	m := &faultMetrics{}
	inj := faulttest.New(t)
	rules := []*faulttest.Rule{
		inj.FailNth(syncfile.OpWrite, segmentFileSuffix, 4, syscall.ENOSPC),
		inj.FailNth(syncfile.OpWrite, segmentFileSuffix, 19, syscall.EIO),
		inj.FailNth(syncfile.OpWrite, segmentFileSuffix, 40, syscall.ENOSPC),
	}

	l, err := NewLog(dir, faultOptions(m))
	if err != nil {
		t.Fatal(err)
	}
	load := newCommitLoad("write")
	load.run(t, l, 8, 12, 4, nil)
	for i, r := range rules {
		if r.Fired() != 1 {
			t.Fatalf("write fault %d fired %d times, want 1", i, r.Fired())
		}
	}
	if load.failures == 0 {
		t.Fatal("no commit observed the write failure")
	}
	if load.poisoned != 0 {
		t.Fatalf("a write failure must not poison the log (%d workers stopped)", load.poisoned)
	}
	assertExactlyOnce(t, l, load)
	load.verifyLog(t, l)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	l2 := reopenLog(t, dir, m)
	assertExactlyOnce(t, l2, load)
	load.verifyLog(t, l2)
}

// A segment roll fails: creating the next segment (ENOSPC) or fsyncing
// the partition directory afterwards (EIO). The commit that needed the
// roll fails and is retried; the next roll must succeed once the disk
// is fine, which it did not before the fix: a segment file created
// just before the directory fsync failed was left behind, and every
// later O_EXCL create of the same name failed with EEXIST, wedging the
// partition until a restart.
func TestFaultSegmentRollFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   syncfile.Op
		err  error
	}{
		{"create-enospc", syncfile.OpOpen, syscall.ENOSPC},
		{"dirsync-eio", syncfile.OpSync, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := testLogPath(t)
			m := &faultMetrics{}
			inj := faulttest.New(t)
			l, err := NewLog(dir, faultOptions(m))
			if err != nil {
				t.Fatal(err)
			}
			// Installed after NewLog so the first segment's create and
			// directory sync are not the calls that fail. A segment file
			// is 20 digits plus ".log" (the hwm file also lives under a
			// directory named p.log, so a suffix match is not enough);
			// the first directory sync under load is the hwm file's
			// creation, the second is the first roll.
			var rule *faulttest.Rule
			if tc.op == syncfile.OpOpen {
				rule = inj.FailNthFunc(tc.op, func(p string) bool {
					_, ok := parseSegmentFileName(filepath.Base(p))
					return ok
				}, 1, tc.err)
			} else {
				rule = inj.FailNthFunc(tc.op, func(p string) bool { return p == dir }, 2, tc.err)
			}

			load := newCommitLoad("roll")
			load.run(t, l, 8, 24, 4, nil)
			if rule.Fired() != 1 {
				t.Fatalf("roll fault fired %d times, want 1", rule.Fired())
			}
			// The roll runs either before the frame write that needs it
			// (then the commit fails and is retried) or after a commit
			// returned (then it is deferred and retried before the next
			// write). Both are fine; what matters is that the roll
			// after the fault succeeded, so the segment that failed to
			// roll did not grow for the rest of the load.
			for _, err := range load.errs {
				if !strings.Contains(err.Error(), "roll") {
					t.Fatalf("a commit failed outside the roll: %v", err)
				}
			}
			if load.poisoned != 0 {
				t.Fatalf("a roll failure must not poison the log (%d workers stopped)", load.poisoned)
			}
			if n := l.SegmentCount(); n < 3 {
				t.Fatalf("%d segments after the load: the log never rolled again after the failed roll", n)
			}
			l.rwmu.RLock()
			active := l.segments[len(l.segments)-1]
			activeSize := active.sizeBytes
			l.rwmu.RUnlock()
			if activeSize > 2*l.opts.SegmentBytes {
				t.Fatalf("active segment is %d bytes with SegmentBytes %d: rolls stopped working", activeSize, l.opts.SegmentBytes)
			}
			assertExactlyOnce(t, l, load)
			load.verifyLog(t, l)
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			l2 := reopenLog(t, dir, m)
			assertExactlyOnce(t, l2, load)
			load.verifyLog(t, l2)
		})
	}
}

// The consumer frontier file: its fdatasync fails, the write reports
// the error, and the file reads back as either the old value or none,
// never as garbage.
func TestFaultConsumerOffsetSyncFails(t *testing.T) {
	dir := t.TempDir()
	if err := WriteConsumerOffset(dir, 41); err != nil {
		t.Fatal(err)
	}
	inj := faulttest.New(t)
	inj.FailNth(syncfile.OpSyncData, consumerOffsetFileName, 1, syscall.EIO)
	if err := WriteConsumerOffset(dir, 42); !errors.Is(err, syscall.EIO) {
		t.Fatalf("WriteConsumerOffset: %v, want EIO", err)
	}
	got, ok, err := ReadConsumerOffset(dir)
	if err != nil {
		t.Fatalf("ReadConsumerOffset after failed sync: %v", err)
	}
	if ok && got != 41 && got != 42 {
		t.Fatalf("consumer offset = %d, want 41 (old) or 42 (new)", got)
	}
	if err := WriteConsumerOffset(dir, 43); err != nil {
		t.Fatalf("WriteConsumerOffset after the disk recovered: %v", err)
	}
	if got, ok, err := ReadConsumerOffset(dir); err != nil || !ok || got != 43 {
		t.Fatalf("ReadConsumerOffset = (%d, %v, %v), want 43", got, ok, err)
	}
}

// The disk lies: fdatasyncs of the segments and the high-watermark
// file report success without writing, in alternating windows of three
// honest then six lying syncs (the directory journal stays honest). The load is then "crashed" by
// cutting every file back to its last honest durability point, and
// recovery must:
//
//   - never panic and serve only submitted, intact records;
//   - keep every record whose commit was fully honest (no lie during
//     its CommitDurable) visible at its offset;
//   - keep the index and the files in agreement;
//   - accept new commits afterwards and survive a second reopen.
//
// What is lost: records whose commit was acked on the strength of a
// lying fsync. Either their frames are gone (segment sync lied) or the
// high-watermark that exposed them regressed (hwm sync lied), in which
// case they sit in the hidden tail and the ingress WAL re-commits them
// at fresh offsets (duplicates, never silent loss, exactly the
// documented crash semantics). That is the disk's fault, not the
// engine's; what must never happen is a served torn record, a crash
// loop at open, or an index that disagrees with the file.
func TestFaultLyingFsyncCrash(t *testing.T) {
	dir := testLogPath(t)
	m := &faultMetrics{}
	inj := faulttest.New(t)
	l, err := NewLog(dir, faultOptions(m))
	if err != nil {
		t.Fatal(err)
	}
	inj.LieSyncs(dir, 3, 6, true)

	load := newCommitLoad("lie")
	load.run(t, l, 8, 12, 4, inj.Lies)
	if load.failures != 0 {
		t.Fatalf("commits failed under a lying disk: %d (%v)", load.failures, load.lastCommitE)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inj.Lies() == 0 || inj.HonestSyncs() == 0 {
		t.Fatalf("lies=%d honest=%d: the run needs both", inj.Lies(), inj.HonestSyncs())
	}
	honestCommits := 0
	for _, h := range load.honest {
		if h {
			honestCommits++
		}
	}
	if honestCommits == 0 {
		t.Fatal("no commit was fully honest; raise the honest ratio")
	}
	if err := inj.Crash(dir); err != nil {
		t.Fatalf("crash: %v", err)
	}
	pristine := copyLogDir(t, dir)

	l2 := reopenLog(t, dir, m)
	hwm := l2.HighWatermark()
	lost, hidden, holes := 0, 0, 0
	load.mu.Lock()
	load.allowHoles = true
	for off, want := range load.committed {
		rec, err := l2.Read(off)
		visible := err == nil && off < hwm && string(rec) == want
		if load.honest[off] && !visible {
			load.mu.Unlock()
			t.Fatalf("honestly committed offset %d (%q) not visible after crash: hwm=%d got=%q err=%v", off, want, hwm, rec, err)
		}
		if !visible {
			if err == nil && string(rec) == want {
				hidden++
			} else {
				lost++
			}
		}
	}
	// Recovery serves only submitted records below the hwm. An offset
	// with no frame at all (the tail of a sealed segment whose last
	// sync lied, while the segment rolled after it survived) reads as
	// not-found: the consume path and the fan-out reader skip such an
	// offset with the corrupt-skipped counter (recorded loss, never a
	// stall, never a fabricated record).
	for off := int64(0); off < hwm; off++ {
		rec, err := l2.Read(off)
		if err != nil {
			if errors.Is(err, ErrOffsetNotFound) || IsCorrupt(err) {
				holes++
				continue
			}
			load.mu.Unlock()
			t.Fatalf("offset %d below hwm %d after crash: %v", off, hwm, err)
		}
		if !load.submitted[string(rec)] {
			load.mu.Unlock()
			t.Fatalf("offset %d below hwm %d after crash holds bytes never appended: %q", off, hwm, rec)
		}
	}
	load.mu.Unlock()
	verifyIndexAgreesWithDisk(t, l2)
	t.Logf("committed=%d honest=%d hwm=%d next=%d lost-to-lying-fsync=%d (holes below hwm: %d) hidden-tail=%d lies=%d honest-syncs=%d",
		len(load.committed), honestCommits, hwm, l2.NextOffset(), lost, holes, hidden, inj.Lies(), inj.HonestSyncs())

	// The engine keeps working on the recovered log, honestly now.
	inj.StopLying()
	after := newCommitLoad("after").inherit(load)
	after.allowHoles = true
	after.run(t, l2, 4, 6, 4, nil)
	if after.failures != 0 {
		t.Fatalf("commits after crash recovery failed: %v", after.lastCommitE)
	}
	after.verifyLog(t, l2)
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	l3 := reopenLog(t, dir, m)
	after.verifyLog(t, l3)
	_ = l3.Close()

	// Torn tails on top of the crash, each on a fresh copy of the
	// crashed directory: the active segment cut inside its last frame,
	// extended with random garbage (which may contain frame magics),
	// or its tail zero-filled; and the high-watermark file torn to a
	// size a single-sector write can never produce.
	rng := rand.New(rand.NewPCG(3, 5))
	for _, tc := range []struct {
		name string
		hurt func(dir, active string) error
	}{
		{"truncate-mid-frame", func(_, p string) error { return faulttest.TruncateTail(p, int64(1+rng.IntN(200))) }},
		{"append-garbage", func(_, p string) error { return faulttest.AppendGarbage(p, 4096, rng.Uint64()) }},
		{"zero-fill-tail", func(_, p string) error { return faulttest.ZeroTail(p, int64(1+rng.IntN(200))) }},
		{"garbage-then-truncate", func(_, p string) error {
			if err := faulttest.AppendGarbage(p, 1000, rng.Uint64()); err != nil {
				return err
			}
			return faulttest.TruncateTail(p, 7)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work := copyLogDir(t, pristine)
			names, err := listSegmentFileNames(work)
			if err != nil || len(names) == 0 {
				t.Fatalf("list segments: %v", err)
			}
			if err := tc.hurt(work, filepath.Join(work, names[len(names)-1])); err != nil {
				t.Fatal(err)
			}
			lt := reopenLog(t, work, m)
			assertCleanPrefix(t, lt, load, hwm)
			verifyIndexAgreesWithDisk(t, lt)
			more := newCommitLoad("more").inherit(load)
			more.allowHoles = true
			more.run(t, lt, 4, 4, 4, nil)
			if more.failures != 0 {
				t.Fatalf("commits after torn-tail recovery failed: %v", more.lastCommitE)
			}
			more.verifyLog(t, lt)
			if err := lt.Close(); err != nil {
				t.Fatal(err)
			}
			again := reopenLog(t, work, m)
			more.verifyLog(t, again)
		})
	}

	t.Run("torn-hwm-file", func(t *testing.T) {
		work := copyLogDir(t, pristine)
		if err := os.WriteFile(hwmFilePath(work), []byte{0, 0, 0}, 0o600); err != nil {
			t.Fatal(err)
		}
		// A 3-byte high-watermark is outside the single-sector model
		// the file relies on. The log refuses to open (loudly, no
		// panic) rather than guess a visibility boundary; the refusal
		// is stable across retries (no crash loop that makes progress
		// by accident), and the operator's fix is to remove the file
		// (recovery then exposes the whole durable tail, which the
		// ingress WAL may duplicate but never loses).
		for range 3 {
			if _, err := NewLog(work, faultOptions(m)); err == nil {
				t.Fatal("NewLog accepted a 3-byte hwm file")
			}
		}
		if err := os.Remove(hwmFilePath(work)); err != nil {
			t.Fatal(err)
		}
		lt := reopenLog(t, work, m)
		if lt.HighWatermark() != lt.NextOffset() {
			t.Fatalf("without an hwm file recovery bootstraps hwm=%d to the tail %d", lt.HighWatermark(), lt.NextOffset())
		}
		assertCleanPrefix(t, lt, load, lt.HighWatermark())
	})
}

// assertCleanPrefix checks that everything below the recovered
// high-watermark is a submitted record at the offset it was committed
// at, that the high-watermark did not grow past what the crash left
// (maxHWM), and that committed records that are visible are at their
// offsets.
func assertCleanPrefix(t *testing.T, l *Log, load *commitLoad, maxHWM int64) {
	t.Helper()
	load.mu.Lock()
	defer load.mu.Unlock()
	hwm := l.HighWatermark()
	if hwm > maxHWM {
		t.Fatalf("hwm %d grew past the crash's %d", hwm, maxHWM)
	}
	if hwm > l.NextOffset() {
		t.Fatalf("hwm %d > next %d", hwm, l.NextOffset())
	}
	for off := int64(0); off < hwm; off++ {
		rec, err := l.Read(off)
		if err != nil {
			if errors.Is(err, ErrOffsetNotFound) || IsCorrupt(err) {
				continue // a recorded hole, see TestFaultLyingFsyncCrash
			}
			t.Fatalf("Read(%d) below hwm %d: %v", off, hwm, err)
		}
		if !load.submitted[string(rec)] {
			t.Fatalf("offset %d below hwm holds bytes never appended: %q", off, rec)
		}
		if want, ok := load.committed[off]; ok && want != string(rec) {
			t.Fatalf("offset %d holds %q, committed %q", off, rec, want)
		}
	}
}

// copyLogDir copies a partition directory's regular files.
func copyLogDir(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "log")
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// A rename fails (EIO) on the files that are replaced atomically: the
// topic incarnation marker, the fan-out cursor, and the quarantine of a
// stale topic directory. The caller gets the error, the old file (or
// the old directory) is intact, no temp file is left behind, and the
// retry succeeds once the disk is fine.
func TestFaultRenameFails(t *testing.T) {
	dataDir := t.TempDir()
	topicDir := TopicDir(dataDir, "orders")
	if err := WriteTopicIncarnation(topicDir, "0123456789abcdef"); err != nil {
		t.Fatal(err)
	}
	partitionDir := TopicPartitionDir(dataDir, "orders", 0)
	if err := os.MkdirAll(partitionDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteFanoutCursorIfPartitionDirExists(partitionDir, "child", FanoutCursor{}); err != nil {
		t.Fatal(err)
	}

	inj := faulttest.New(t)
	marker := inj.FailNth(syncfile.OpRename, IncarnationMarkerFileName, 1, syscall.EIO)
	cursor := inj.FailNth(syncfile.OpRename, fanoutCursorFileName("child"), 1, syscall.EIO)
	stale := inj.FailNth(syncfile.OpRename, StaleTopicDirSuffix, 1, syscall.EIO)

	if err := WriteTopicIncarnation(topicDir, "fedcba9876543210"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("WriteTopicIncarnation: %v, want EIO", err)
	}
	if id, ok, err := ReadTopicIncarnation(topicDir); err != nil || !ok || id != "0123456789abcdef" {
		t.Fatalf("marker after failed rename = (%q, %v, %v), want the old id intact", id, ok, err)
	}
	if err := WriteFanoutCursorIfPartitionDirExists(partitionDir, "child", FanoutCursor{}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("WriteFanoutCursorIfPartitionDirExists: %v, want EIO", err)
	}
	if _, ok, err := ReadFanoutCursor(partitionDir, "child"); err != nil || !ok {
		t.Fatalf("cursor after failed rename = (ok=%v, %v), want the old cursor intact", ok, err)
	}
	if _, err := QuarantineTopicDir(dataDir, "orders", "0123456789abcdef"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("QuarantineTopicDir: %v, want EIO", err)
	}
	if _, err := os.Stat(topicDir); err != nil {
		t.Fatalf("topic dir after failed quarantine rename: %v, want it in place", err)
	}
	for i, r := range []*faulttest.Rule{marker, cursor, stale} {
		if r.Fired() != 1 {
			t.Fatalf("rename fault %d fired %d times, want 1", i, r.Fired())
		}
	}
	for _, dir := range []string{topicDir, partitionDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if strings.Contains(e.Name(), ".tmp") || strings.Contains(e.Name(), IncarnationMarkerFileName+".") {
				t.Fatalf("temp file %s left behind in %s after a failed rename", e.Name(), dir)
			}
		}
	}

	// The disk is fine again: every write lands on the retry.
	if err := WriteTopicIncarnation(topicDir, "fedcba9876543210"); err != nil {
		t.Fatalf("WriteTopicIncarnation retry: %v", err)
	}
	if id, _, _ := ReadTopicIncarnation(topicDir); id != "fedcba9876543210" {
		t.Fatalf("marker after retry = %q", id)
	}
	if err := WriteFanoutCursorIfPartitionDirExists(partitionDir, "child", FanoutCursor{}); err != nil {
		t.Fatalf("cursor retry: %v", err)
	}
	if _, err := QuarantineTopicDir(dataDir, "orders", "fedcba9876543210"); err != nil {
		t.Fatalf("QuarantineTopicDir retry: %v", err)
	}
	if _, err := os.Stat(topicDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("topic dir after quarantine: %v, want gone", err)
	}
}
