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

// zzWP16SlowSyncs makes every consumer-state data sync take d (skipping
// the real one) and reports the most that ran at once.
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
		return syncfile.ErrLie
	})
	t.Cleanup(restore)
	return peak.Load
}

// consume-local#0: overlapping a flush's partition syncs shortened the
// flush but held back the produce syncs that share the disk (on macOS
// an owner's commit beside 12 dirty partitions went from 10ms to 22ms
// at p99), so a flush syncs one partition at a time.
func TestZZWP16CommitterSyncsOnePartitionAtATime(t *testing.T) {
	const parts = 16
	c, src, _ := zzWP16Committer(t, parts)
	peak := zzWP16SlowSyncs(t, 2*time.Millisecond)
	src.step(c, 1)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if got := peak(); got != 1 {
		t.Fatalf("%d partition syncs ran at once in one flush, want 1", got)
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

// Commits keep arriving while flushes run on a short interval: after
// Close every partition's frontier file holds the last frontier
// committed for it, never an earlier one.
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
