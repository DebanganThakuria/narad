package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzPerfFClock is a hand-stepped clock for a Snapshotter.
type zzPerfFClock struct{ at time.Time }

func (c *zzPerfFClock) now() time.Time { return c.at }

// zzPerfFClosedNode builds a node owning parts partitions of each named
// topic, every topic with an incarnation ID (so its directory carries a
// topic marker) and every log idle-evicted with a one-record backlog and
// no consumer shard, and a Snapshotter over it that reads the returned
// clock. No snapshot has run yet.
func zzPerfFClosedNode(t *testing.T, topics []string, parts int) (*Snapshotter, *zzPerfFClock) {
	t.Helper()
	ms := newRuntimeFakeMetastore()
	for i, name := range topics {
		ms.topics[name] = topic.Topic{Name: name, ID: fmt.Sprintf("%016x", 0xf600+i), Partitions: parts, RetentionMs: int64(7 * 24 * time.Hour / time.Millisecond)}
	}
	logs := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	for _, name := range topics {
		for p := range parts {
			l, err := logs.Get(name, p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))); err != nil {
				t.Fatal(err)
			}
			if err := l.AdvanceHighWatermark(1); err != nil {
				t.Fatal(err)
			}
		}
	}
	time.Sleep(20 * time.Millisecond) // let the flusher write the segments
	if n := logs.EvictIdleOnce(time.Nanosecond); n != len(topics)*parts {
		t.Fatalf("evicted %d logs, want %d", n, len(topics)*parts)
	}
	for _, name := range topics {
		want := ms.topics[name].ID
		if id, ok, err := storage.ReadTopicIncarnation(storage.TopicDir(logs.DataDir(), name)); err != nil || !ok || id != want {
			t.Fatalf("topic %s marker = %q (ok %v, err %v), want %q", name, id, ok, err, want)
		}
	}
	s := NewSnapshotter(ms, consumer.NewInFlight(wp8Caps, nil), logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	clk := &zzPerfFClock{at: time.Unix(1_800_000_000, 0)}
	s.now = clk.now
	return s, clk
}

// zzPerfFMarkerReads wraps readTopicIncarnation for the rest of the
// test: every read is counted, and a read of a directory in fail
// returns an error instead.
func zzPerfFMarkerReads(t *testing.T, fail map[string]bool) *int {
	t.Helper()
	orig := readTopicIncarnation
	t.Cleanup(func() { readTopicIncarnation = orig })
	n := new(int)
	readTopicIncarnation = func(dir string) (string, bool, error) {
		*n++
		if fail[dir] {
			return "", false, errors.New("injected marker read error")
		}
		return orig(dir)
	}
	return n
}

// zzPerfFSnapshot runs one snapshot and returns how many partitions it
// reported per topic.
func zzPerfFSnapshot(t *testing.T, s *Snapshotter) map[string]int {
	t.Helper()
	snap, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]int, len(snap))
	for _, ts := range snap {
		got[ts.Topic] = len(ts.Partitions)
	}
	return got
}

// TestPerfFSnapshotReadsTopicMarkerOncePerTopic: a poll that reloads
// closed partitions reads each topic's incarnation marker once, not once
// per partition, still reads nothing when no partition reloads, and a
// marker it cannot read drops only that topic's reloaded partitions.
func TestPerfFSnapshotReadsTopicMarkerOncePerTopic(t *testing.T) {
	const parts = 20
	s, clk := zzPerfFClosedNode(t, []string{"a", "b"}, parts)
	fail := make(map[string]bool)
	reads := zzPerfFMarkerReads(t, fail)
	want := map[string]int{"a": parts, "b": parts}

	// The first poll loads every partition.
	if got := zzPerfFSnapshot(t, s); !maps.Equal(got, want) {
		t.Fatalf("first poll reported %v partitions, want %v", got, want)
	}
	if *reads != 2 {
		t.Fatalf("first poll read the topic marker %d times, want 2 (one per topic)", *reads)
	}

	// Past the refresh interval every partition reloads.
	*reads = 0
	clk.at = clk.at.Add(coldRefresh)
	if got := zzPerfFSnapshot(t, s); !maps.Equal(got, want) {
		t.Fatalf("forced reload reported %v partitions, want %v", got, want)
	}
	if *reads != 2 {
		t.Fatalf("forced reload read the topic marker %d times, want 2 (one per topic)", *reads)
	}

	// Nothing to reload: no marker read at all.
	*reads = 0
	if got := zzPerfFSnapshot(t, s); !maps.Equal(got, want) {
		t.Fatalf("cached poll reported %v partitions, want %v", got, want)
	}
	if *reads != 0 {
		t.Fatalf("a poll that reloads nothing read the topic marker %d times, want 0", *reads)
	}

	// A marker read error drops the partitions of that topic that needed
	// it, and nothing else.
	*reads = 0
	fail[storage.TopicDir(s.logs.DataDir(), "a")] = true
	clk.at = clk.at.Add(coldRefresh)
	if got, want := zzPerfFSnapshot(t, s), map[string]int{"a": 0, "b": parts}; !maps.Equal(got, want) {
		t.Fatalf("with topic a's marker unreadable the poll reported %v partitions, want %v", got, want)
	}
	if *reads != 2 {
		t.Fatalf("with topic a's marker unreadable the poll read markers %d times, want 2", *reads)
	}
}

// TestPerfFSnapshotColdRefreshSpread: closed partitions loaded in the
// same poll must not all expire in the same later poll. Over a minute
// of 5 s polls, no poll after the first reloads more than about a sixth
// of them, and no reading is used once it is coldRefresh old, so every
// partition is still reloaded within coldRefresh of its previous load.
func TestPerfFSnapshotColdRefreshSpread(t *testing.T) {
	const (
		parts = 120
		tick  = 5 * time.Second
		limit = parts/6 + 6
	)
	s, clk := zzPerfFClosedNode(t, []string{"orders"}, parts)

	type stamps struct{ log, next time.Time }
	prev := make(map[int]stamps, parts)
	lastLog := make(map[int]time.Time, parts) // when each part was last read
	lastNext := make(map[int]time.Time, parts)
	worst, worstAt := 0, time.Duration(0)
	for step := range 13 {
		if step > 0 {
			clk.at = clk.at.Add(tick)
		}
		now, elapsed := clk.at, time.Duration(step)*tick
		if got := zzPerfFSnapshot(t, s); got["orders"] != parts {
			t.Fatalf("poll at +%v reported %d partitions, want %d", elapsed, got["orders"], parts)
		}
		logLoads, nextLoads := 0, 0
		s.coldMu.Lock()
		if len(s.cold) != parts {
			t.Fatalf("poll at +%v holds %d cold entries, want %d", elapsed, len(s.cold), parts)
		}
		for k, e := range s.cold {
			p, seen := prev[k.partition]
			if !seen || !e.logAt.Equal(p.log) {
				if seen && now.Sub(lastLog[k.partition]) > coldRefresh {
					t.Errorf("partition %d: log files reread %v after the previous read, want at most %v", k.partition, now.Sub(lastLog[k.partition]), coldRefresh)
				}
				lastLog[k.partition] = now
				logLoads++
			}
			if !seen || !e.nextAt.Equal(p.next) {
				if seen && now.Sub(lastNext[k.partition]) > coldRefresh {
					t.Errorf("partition %d: frontier files reread %v after the previous read, want at most %v", k.partition, now.Sub(lastNext[k.partition]), coldRefresh)
				}
				lastNext[k.partition] = now
				nextLoads++
			}
			if age := now.Sub(lastLog[k.partition]); age >= coldRefresh {
				t.Errorf("poll at +%v used a %v old reading of partition %d's log files", elapsed, age, k.partition)
			}
			if age := now.Sub(lastNext[k.partition]); age >= coldRefresh {
				t.Errorf("poll at +%v used a %v old reading of partition %d's frontier files", elapsed, age, k.partition)
			}
			prev[k.partition] = stamps{log: e.logAt, next: e.nextAt}
		}
		s.coldMu.Unlock()
		if step == 0 {
			if logLoads != parts || nextLoads != parts {
				t.Fatalf("first poll loaded %d log and %d frontier readings, want %d of each", logLoads, nextLoads, parts)
			}
			continue
		}
		if n := max(logLoads, nextLoads); n > worst {
			worst, worstAt = n, elapsed
		}
	}
	if worst > limit {
		t.Fatalf("the poll at +%v reloaded %d of %d closed partitions, want at most %d: readings loaded together must not expire together", worstAt, worst, parts, limit)
	}
}
