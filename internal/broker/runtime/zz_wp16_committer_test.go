package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
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

// zzWP16SlowSyncs is a committer durability seam whose every writeout
// takes d (skipping the real one); maxInFlight reports the most that
// ran at once.
func zzWP16SlowSyncs(d time.Duration) (io offsetIO, maxInFlight func() int64) {
	var inFlight, peak atomic.Int64
	io.writeOut = func(*os.File) error {
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
	}
	io.flushDevice = func(*os.File) error { return nil }
	io.syncDir = func(*os.File) error { return nil }
	return io, peak.Load
}

// consume-local#0: overlapping a flush's partition syncs shortened the
// flush but held back the produce syncs that share the disk (on macOS
// an owner's commit beside 12 dirty partitions went from 10ms to 22ms
// at p99), so a tick writes its partitions out one at a time.
func TestZZWP16CommitterSyncsOnePartitionAtATime(t *testing.T) {
	const parts = 16
	c, src, _ := zzWP16Committer(t, parts)
	var peak func() int64
	c.io, peak = zzWP16SlowSyncs(2 * time.Millisecond)
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
	c.io.writeOut = func(f *os.File) error {
		if filepath.Dir(f.Name()) == failing {
			return syscall.EIO
		}
		return offsetWriteOut(f)
	}
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

// zzWP16LogSink collects what a JSON slog handler writes, one record a
// line.
type zzWP16LogSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *zzWP16LogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

// records returns the records logged at level with message prefix msg.
func (s *zzWP16LogSink) records(t *testing.T, level, msg string) []map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for line := range strings.Lines(s.buf.String()) {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec["level"] == level && strings.HasPrefix(rec["msg"].(string), msg) {
			out = append(out, rec)
		}
	}
	return out
}

// zzWP16LoggedLoop runs a committer's own loop for d over parts
// partitions that a feeder keeps dirty, every consumer-state sync taking
// syncTime, and returns what it logged.
func zzWP16LoggedLoop(t *testing.T, parts int, interval, syncTime, d time.Duration) *zzWP16LogSink {
	t.Helper()
	dataDir := t.TempDir()
	for p := range parts {
		mustCreatePartitionDir(t, dataDir, "t", p)
	}
	io, _ := zzWP16SlowSyncs(syncTime)
	sink := &zzWP16LogSink{}
	c := newConsumerOffsetCommitter(dataDir, interval, slog.New(slog.NewJSONHandler(sink, nil)), committerOptions{io: io})
	var version atomic.Uint64
	c.SetAheadSource(func(string, int) (int64, []int64, uint64, bool) {
		v := version.Load()
		return int64(v) * 10, []int64{int64(v)*10 + 2}, v, true
	})
	stop := make(chan struct{})
	var fed sync.WaitGroup
	fed.Go(func() {
		tick := time.NewTicker(interval / 10)
		defer tick.Stop()
		for {
			v := version.Add(1)
			for p := range parts {
				c.Commit("t", p, int64(v)*10)
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	})
	time.Sleep(d)
	close(stop)
	fed.Wait()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	return sink
}

// cross-cutting#5: a flush that takes longer than the interval leaves
// persisted offsets lagging acks by the flush time, not the interval the
// docs promise, and the committer holding the disk the whole time. That
// used to go unsaid; it is logged, once a minute at most, with what an
// operator needs to see why.
func TestZZWP16CommitterWarnsWhenItCannotKeepToItsInterval(t *testing.T) {
	const parts = 8
	// 8 partitions of 20ms each: every flush takes about 160ms, more
	// than three 50ms intervals.
	sink := zzWP16LoggedLoop(t, parts, 50*time.Millisecond, 20*time.Millisecond, time.Second)
	warned := sink.records(t, "WARN", "consumer offset commits cannot keep to their interval")
	if len(warned) != 1 {
		t.Fatalf("%d warnings over several overrunning flushes, want exactly 1 (once a minute)", len(warned))
	}
	rec := warned[0]
	if rec["partitions"] != float64(parts) || rec["interval"] != float64(50*time.Millisecond) {
		t.Errorf("warning attributes %v, want partitions=%d interval=50ms", rec, parts)
	}
	if took, _ := rec["flush_took"].(float64); time.Duration(took) <= 50*time.Millisecond {
		t.Errorf("warning flush_took=%v, want more than the interval", rec["flush_took"])
	}
}

// A committer that keeps up says nothing. The background loop times its
// ticks by the wall clock, which a loaded machine (make check runs
// packages in parallel under -race) stretches past the tick on its own,
// so this drives the ticks by hand at synthetic times and hands warn
// the flush time: one well under the tick must not warn, and the same
// committer must warn for one over it, so the test can fail. (The first
// tick, which creates every partition's consumer.ahead, is not held to
// the interval.)
func TestZZWP16CommitterThatKeepsUpDoesNotWarn(t *testing.T) {
	const parts = 8
	dataDir := t.TempDir()
	for p := range parts {
		mustCreatePartitionDir(t, dataDir, "t", p)
	}
	io, _ := zzWP16SlowSyncs(time.Millisecond)
	sink := &zzWP16LogSink{}
	c := newConsumerOffsetCommitter(dataDir, 250*time.Millisecond, slog.New(slog.NewJSONHandler(sink, nil)), committerOptions{io: io, manual: true})
	defer c.Close()
	var version uint64
	c.SetAheadSource(func(string, int) (int64, []int64, uint64, bool) {
		return int64(version) * 10, []int64{int64(version)*10 + 2}, version, true
	})

	start := time.Now()
	for i := range 20 {
		version++
		for p := range parts {
			c.Commit("t", p, int64(version)*10)
		}
		at := start.Add(time.Duration(i) * c.tick)
		if err := c.tickAt(at, offsetTickNormal); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		c.warn(at, c.tick/10)
	}
	if warned := sink.records(t, "WARN", "consumer offset"); len(warned) != 0 {
		t.Fatalf("a committer that keeps up warned: %v", warned)
	}

	c.warn(start.Add(time.Hour), 2*c.tick)
	if warned := sink.records(t, "WARN", "consumer offset commits cannot keep to their interval"); len(warned) != 1 {
		t.Fatalf("a flush over the tick gave %d warnings, want 1: %v", len(warned), warned)
	}
}
