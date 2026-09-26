package runtime

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// wp8SnapshotEnv is one node with a 7-day-retention topic "billing"
// whose partition 0 holds n committed records nobody consumed.
type wp8SnapshotEnv struct {
	dataDir string
	ms      *runtimeFakeMetastore
	logs    *Logs
	offsets *consumer.InFlight
	snap    *Snapshotter
}

func newWP8SnapshotEnv(t *testing.T, n int) *wp8SnapshotEnv {
	t.Helper()
	env := &wp8SnapshotEnv{dataDir: t.TempDir(), ms: newRuntimeFakeMetastore()}
	env.ms.topics["billing"] = topic.Topic{Name: "billing", ID: "inc-1", Partitions: 1, RetentionMs: int64(7 * 24 * time.Hour / time.Millisecond)}
	env.logs = NewLogs(env.dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, env.ms, nil)
	t.Cleanup(func() { _ = env.logs.CloseAll() })
	env.offsets = consumer.NewInFlight(wp8Caps, nil)
	env.snap = NewSnapshotter(env.ms, env.offsets, env.logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	env.produce(t, n)
	return env
}

func (env *wp8SnapshotEnv) produce(t *testing.T, n int) {
	t.Helper()
	l, err := env.logs.Get("billing", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range n {
		if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.AdvanceHighWatermark(l.NextOffset()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // let the flusher write the segment
}

func (env *wp8SnapshotEnv) dir() string {
	return storage.TopicPartitionDir(env.dataDir, "billing", 0)
}

// partition returns billing/0's snapshot, ok=false when it is omitted.
func (env *wp8SnapshotEnv) partition(t *testing.T) (metrics.PartitionSnapshot, bool) {
	t.Helper()
	snaps, err := env.snap.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, ts := range snaps {
		if ts.Topic != "billing" {
			continue
		}
		for _, ps := range ts.Partitions {
			if ps.Partition == 0 {
				return ps, true
			}
		}
	}
	return metrics.PartitionSnapshot{}, false
}

// TestWP8EvictedBacklogKeepsLagSeries: a consumer outage on a quiet
// topic. Idle eviction closes the log while 100 records wait, and the
// lag and oldest-unconsumed-age series must stay, since they are
// exactly what should page someone.
func TestWP8EvictedBacklogKeepsLagSeries(t *testing.T) {
	env := newWP8SnapshotEnv(t, 100)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := prometheus.NewRegistry()
	poller := metrics.NewPoller(metrics.New(reg), env.snap, logger)

	wp8PollOnce(poller)
	if lag, ok := wp8Gauge(t, reg, "narad_consumer_lag_messages", "billing", "0"); !ok || lag != 100 {
		t.Fatalf("lag before eviction = %v (present %v), want 100", lag, ok)
	}
	if n := env.logs.EvictIdleOnce(time.Nanosecond); n != 1 {
		t.Fatalf("evicted %d logs, want 1", n)
	}
	wp8PollOnce(poller)
	if lag, ok := wp8Gauge(t, reg, "narad_consumer_lag_messages", "billing", "0"); !ok || lag != 100 {
		t.Fatalf("lag after eviction = %v (present %v), want 100: the backlog is untouched", lag, ok)
	}
	if age, ok := wp8Gauge(t, reg, "narad_oldest_unconsumed_message_age_seconds", "billing", "0"); !ok || age < 0 {
		t.Fatalf("oldest-unconsumed age after eviction = %v (present %v), want a series", age, ok)
	}
	if _, ok := wp8Gauge(t, reg, "narad_partition_size_bytes", "billing", "0"); !ok {
		t.Fatal("partition size series vanished after eviction")
	}
}

// TestWP8ShardlessPartitionReportsPersistedFrontier: after a restart a
// partition's log is open (producers, or the startup reconcile) but no
// consumer has touched it, so there is no shard. Lag must come from the
// persisted frontier, the larger of consumer.offset and consumer.ahead,
// not from zero (which reported the whole log as lag).
func TestWP8ShardlessPartitionReportsPersistedFrontier(t *testing.T) {
	env := newWP8SnapshotEnv(t, 100)
	if err := storage.WriteConsumerOffset(env.dir(), 59); err != nil {
		t.Fatal(err)
	}
	ps, ok := env.partition(t)
	if !ok || ps.CommittedOffset != 60 || ps.HighWatermark-ps.CommittedOffset != 40 {
		t.Fatalf("open, shardless: %+v (ok %v), want next 60, lag 40", ps, ok)
	}

	// consumer.ahead carries a newer frontier (the committer skips the
	// consumer.offset sync when it writes consumer.ahead), and acked-ahead
	// offsets that continue it collapse into it as recovery would.
	env2 := newWP8SnapshotEnv(t, 100)
	if err := storage.WriteConsumerOffset(env2.dir(), 59); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteConsumerAhead(env2.dir(), 0, 1, 69, []int64{70, 71, 80}); err != nil {
		t.Fatal(err)
	}
	if n := env2.logs.EvictIdleOnce(time.Nanosecond); n != 1 {
		t.Fatalf("evicted %d logs, want 1", n)
	}
	ps, ok = env2.partition(t)
	if !ok || ps.CommittedOffset != 72 {
		t.Fatalf("closed, shardless: %+v (ok %v), want next 72 (ahead frontier 69, 70 and 71 acked)", ps, ok)
	}
}

// TestWP8ClosedPartitionMatchesTheLiveSnapshot: what the files say
// about a closed partition is what the open log said.
func TestWP8ClosedPartitionMatchesTheLiveSnapshot(t *testing.T) {
	env := newWP8SnapshotEnv(t, 100)
	ctx := context.Background()
	// A consumer took the first 30 and acked them in order.
	for range 30 {
		r, err := env.offsets.ReserveNext(ctx, "billing", 0, time.Minute, 100)
		if err != nil || !r.Reserved {
			t.Fatalf("reserve: %+v %v", r, err)
		}
		if err := env.offsets.CommitHandle("billing", 0, r.Offset, r.Nonce); err != nil {
			t.Fatal(err)
		}
	}
	live, ok := env.partition(t)
	if !ok {
		t.Fatal("open partition omitted")
	}
	env.logs.EvictIdleOnce(time.Nanosecond)
	cold, ok := env.partition(t)
	if !ok {
		t.Fatal("closed partition omitted")
	}
	live.OldestUnconsumedAt, cold.OldestUnconsumedAt = min(live.OldestUnconsumedAt, 1), min(cold.OldestUnconsumedAt, 1)
	if live != cold {
		t.Fatalf("closed snapshot %+v differs from the live one %+v", cold, live)
	}
	if cold.CommittedOffset != 30 || cold.HighWatermark != 100 || cold.SegmentCount != 1 || cold.SizeBytes == 0 {
		t.Fatalf("closed snapshot %+v, want next 30 (shard), hwm 100, one non-empty segment", cold)
	}
}

// TestWP8ColdSnapshotRereadsAfterTheLogWasOpen: the cached reading of
// a closed partition is dropped once the log is seen open, so the
// records produced meanwhile show up after it closes again.
func TestWP8ColdSnapshotRereadsAfterTheLogWasOpen(t *testing.T) {
	env := newWP8SnapshotEnv(t, 10)
	env.logs.EvictIdleOnce(time.Nanosecond)
	if ps, ok := env.partition(t); !ok || ps.HighWatermark != 10 {
		t.Fatalf("closed: %+v (ok %v), want hwm 10", ps, ok)
	}
	env.produce(t, 5)
	if ps, ok := env.partition(t); !ok || ps.HighWatermark != 15 {
		t.Fatalf("reopened: %+v (ok %v), want hwm 15", ps, ok)
	}
	env.logs.EvictIdleOnce(time.Nanosecond)
	if ps, ok := env.partition(t); !ok || ps.HighWatermark != 15 {
		t.Fatalf("closed again: %+v (ok %v), want hwm 15 (not the cached 10)", ps, ok)
	}
}

// TestWP8ClosedPartitionOfAnotherIncarnationIsOmitted: a directory a
// deleted same-named topic left behind (this node missed the purge)
// must not be reported as the live topic's backlog.
func TestWP8ClosedPartitionOfAnotherIncarnationIsOmitted(t *testing.T) {
	env := newWP8SnapshotEnv(t, 10)
	env.logs.EvictIdleOnce(time.Nanosecond)
	if _, ok := env.partition(t); !ok {
		t.Fatal("closed partition of the live incarnation omitted")
	}
	// The name is deleted and recreated; this node still holds the old
	// incarnation's directory.
	env.ms.topics["billing"] = topic.Topic{Name: "billing", ID: "inc-2", Partitions: 1, RetentionMs: int64(7 * 24 * time.Hour / time.Millisecond)}
	env.snap.coldMu.Lock()
	clear(env.snap.cold) // as after the refresh interval
	env.snap.coldMu.Unlock()
	if ps, ok := env.partition(t); ok {
		t.Fatalf("stale incarnation reported as %+v", ps)
	}
	if marker, _, _ := storage.ReadTopicIncarnation(filepath.Dir(env.dir())); marker != "inc-1" {
		t.Fatalf("marker = %q, want the old incarnation untouched", marker)
	}
}
