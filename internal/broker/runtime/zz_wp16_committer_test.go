package runtime

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// zzWP16SlowSyncs makes every consumer-state data sync take d and
// reports the most that were in flight at once.
func zzWP16SlowSyncs(t *testing.T, d time.Duration) (maxInFlight func() int64) {
	t.Helper()
	var inFlight, peak atomic.Int64
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op != syncfile.OpSyncData || !strings.HasPrefix(filepath.Base(path), "consumer.") {
			return nil
		}
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(d)
		inFlight.Add(-1)
		return nil
	})
	t.Cleanup(restore)
	return peak.Load
}

// consume-local#0: a flush made its dirty partitions durable one at a
// time, a data sync each, so a node with many partitions spent every
// tick (and more) inside back-to-back syncs and persisted offsets
// lagged acks by several intervals. The partitions of one flush are
// independent files: their syncs overlap, up to the flush's bound.
func TestZZWP16CommitterFlushOverlapsPartitionSyncs(t *testing.T) {
	const parts = 16
	c, src, _ := zzWP16Committer(t, parts)
	peak := zzWP16SlowSyncs(t, 20*time.Millisecond)
	src.step(c, 1)
	start := time.Now()
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	if got := peak(); got < 2 || got > committerFlushWorkers {
		t.Fatalf("at most %d partition syncs overlapped in one flush, want between 2 and %d", got, committerFlushWorkers)
	}
	if serial := parts * 20 * time.Millisecond; took >= serial {
		t.Fatalf("flush of %d partitions took %s, no faster than one sync after another (%s)", parts, took, serial)
	}
}

// A partition whose write fails is queued again for the next flush,
// while the others of the same flush land; the flush reports the
// failure.
func TestZZWP16CommitterFlushRequeuesOnlyTheFailedPartitions(t *testing.T) {
	const parts = 12
	c, src, dataDir := zzWP16Committer(t, parts)
	failing := storage.TopicPartitionDir(dataDir, "t", 5)
	var fail atomic.Bool
	fail.Store(true)
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpWrite && fail.Load() && filepath.Dir(path) == failing {
			return syscall.EIO
		}
		return nil
	})
	defer restore()
	src.step(c, 1)
	if err := c.flush(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("flush error = %v, want the EIO of partition 5", err)
	}
	for p := range parts {
		rec, ok, err := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, "t", p))
		if err != nil || !ok {
			t.Fatalf("partition %d: consumer.ahead ok %v err %v", p, ok, err)
		}
		want := int64(20)
		if p == 5 {
			want = 10 // the warm-up record: this flush's write failed
		}
		if rec.Committed != want {
			t.Fatalf("partition %d: consumer.ahead committed %d, want %d", p, rec.Committed, want)
		}
	}
	c.mu.Lock()
	queued, ok := c.pending[offsetCommitKey{topic: "t", partition: 5}]
	others := len(c.pending)
	c.mu.Unlock()
	if !ok || queued != 20 || others != 1 {
		t.Fatalf("pending after the flush = %d entries (partition 5: %d, %v), want only partition 5 at 20", others, queued, ok)
	}
	fail.Store(false)
	if err := c.flush(); err != nil {
		t.Fatalf("retry flush: %v", err)
	}
	if rec, _, _ := storage.ReadConsumerAhead(failing); rec.Committed != 20 {
		t.Fatalf("partition 5 after the retry: committed %d, want 20", rec.Committed)
	}
}

// Commits keep arriving while flushes run on a short interval, several
// partitions' syncs at once: after Close every partition's frontier file
// holds the last frontier committed for it, never an earlier one.
func TestZZWP16CommitterConcurrentFlushesKeepEveryFrontier(t *testing.T) {
	const parts, rounds = 24, 300
	dataDir := t.TempDir()
	for p := range parts {
		mustCreatePartitionDir(t, dataDir, "t", p)
	}
	c := NewConsumerOffsetCommitter(dataDir, time.Millisecond, nil)
	var wg sync.WaitGroup
	for p := range parts {
		wg.Go(func() {
			for i := range rounds {
				c.Commit("t", p, int64(i))
				if i%50 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		})
	}
	wg.Wait()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for p := range parts {
		got, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, "t", p))
		if err != nil || !ok || got != rounds-1 {
			t.Fatalf("partition %d: consumer.offset = %d (ok %v err %v), want %d", p, got, ok, err, rounds-1)
		}
	}
}

// Close's final flush writes every pending partition, and reports one
// that failed rather than dropping the error.
func TestZZWP16CommitterCloseReportsAFailedPartition(t *testing.T) {
	const parts = 10
	c, src, dataDir := zzWP16Committer(t, parts)
	failing := storage.TopicPartitionDir(dataDir, "t", 3)
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSyncData && filepath.Dir(path) == failing {
			return syscall.EIO
		}
		return nil
	})
	defer restore()
	src.step(c, 1)
	if err := c.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Close error = %v, want partition 3's EIO", err)
	}
	for p := range parts {
		if p == 3 {
			continue
		}
		if rec, _, _ := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, "t", p)); rec.Committed != 20 {
			t.Fatalf("partition %d after Close: committed %d, want 20", p, rec.Committed)
		}
	}
}

// zzWP16Loop is a committer whose own loop does the flushing, over
// parts partitions, with every consumer-state sync skipped and, while
// slow is set, taking syncTime.
type zzWP16Loop struct {
	t        *testing.T
	c        *ConsumerOffsetCommitter
	dataDir  string
	parts    int
	version  atomic.Uint64
	slow     atomic.Bool
	inFlight atomic.Int64
	peak     atomic.Int64
	// overlapped counts syncs that started while another was running.
	overlapped atomic.Int64
}

func newZZWP16Loop(t *testing.T, parts int, interval, syncTime time.Duration) *zzWP16Loop {
	t.Helper()
	l := &zzWP16Loop{t: t, dataDir: t.TempDir(), parts: parts}
	for p := range parts {
		mustCreatePartitionDir(t, l.dataDir, "t", p)
	}
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op != syncfile.OpSyncData || !strings.HasPrefix(filepath.Base(path), "consumer.") {
			return nil
		}
		n := l.inFlight.Add(1)
		defer l.inFlight.Add(-1)
		if n > 1 {
			l.overlapped.Add(1)
		}
		for {
			p := l.peak.Load()
			if n <= p || l.peak.CompareAndSwap(p, n) {
				break
			}
		}
		if l.slow.Load() {
			time.Sleep(syncTime)
		}
		return syncfile.ErrLie
	})
	t.Cleanup(restore)
	l.c = NewConsumerOffsetCommitter(l.dataDir, interval, nil)
	t.Cleanup(func() { _ = l.c.Close() })
	l.c.SetAheadSource(func(string, int) (int64, []int64, uint64, bool) {
		v := l.version.Load()
		return int64(v) * 10, []int64{int64(v)*10 + 2}, v, true
	})
	return l
}

// round dirties every partition in one step, so a single flush takes
// them all, and waits until that flush has written and synced them. It
// returns the most syncs that ran at once, and how many started while
// another was running.
func (l *zzWP16Loop) round(v uint64) (peak, overlapped int64) {
	l.t.Helper()
	l.peak.Store(0)
	l.overlapped.Store(0)
	l.version.Store(v)
	l.c.mu.Lock()
	for p := range l.parts {
		l.c.pending[offsetCommitKey{topic: "t", partition: p}] = int64(v) * 10
	}
	l.c.mu.Unlock()
	deadline := time.Now().Add(20 * time.Second)
	for {
		done := l.inFlight.Load() == 0
		for p := range l.parts {
			rec, ok, _ := storage.ReadConsumerAhead(storage.TopicPartitionDir(l.dataDir, "t", p))
			done = done && ok && rec.Committed == int64(v)*10
		}
		if done {
			return l.peak.Load(), l.overlapped.Load()
		}
		if time.Now().After(deadline) {
			l.t.Fatalf("round %d: the committer never wrote every partition", v)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A committer that cannot keep up must not sync more often than a
// serial one would: once a flush's writes add up to more than the
// interval, the next flush writes one partition at a time, and it goes
// back to overlapping them once the writes fit again.
func TestZZWP16SaturatedCommitterFlushesSerially(t *testing.T) {
	// 16 syncs of 20ms add up to more than the interval.
	l := newZZWP16Loop(t, 16, 200*time.Millisecond, 20*time.Millisecond)
	l.slow.Store(true)
	if peak, _ := l.round(1); peak < 2 {
		t.Fatalf("first flush overlapped %d syncs, want several", peak)
	}
	if peak, _ := l.round(2); peak != 1 {
		t.Fatalf("flush after one that overran its interval overlapped %d syncs, want 1 (serial)", peak)
	}
	l.slow.Store(false)
	l.round(3) // serial still, and quick: the committer keeps up again
	l.slow.Store(true)
	if peak, _ := l.round(4); peak < 2 {
		t.Fatalf("flush after one that fit its interval overlapped %d syncs, want several", peak)
	}
}

// The first flush of a backlog too big for the interval overlaps its
// writes only until they add up to the interval, then finishes one
// partition at a time: a burst of 8-way syncs never runs for the whole
// backlog.
func TestZZWP16OverrunningFlushNarrowsToOneWriter(t *testing.T) {
	const parts = 48
	// Eight 20ms syncs add up to more than the 100ms interval.
	l := newZZWP16Loop(t, parts, 100*time.Millisecond, 20*time.Millisecond)
	l.slow.Store(true)
	peak, overlapped := l.round(1)
	if peak < 2 {
		t.Fatalf("flush overlapped %d syncs, want several at first", peak)
	}
	if limit := int64(2 * committerFlushWorkers); overlapped > limit {
		t.Fatalf("%d of %d syncs overlapped another, want at most %d: the flush kept overlapping after its writes overran the interval", overlapped, parts, limit)
	}
}
