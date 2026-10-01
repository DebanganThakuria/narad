package runtime

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzPerfFColdSnapshotter builds a node owning parts partitions of a
// topic that has an incarnation ID (so its directory carries a topic
// marker, as every topic created since IDs does), each log with a
// 10-record backlog and then idle-evicted with no consumer shard, and
// returns a Snapshotter over it after one warm snapshot. This is
// BenchmarkWP8Snapshot's closed case with the marker in place: a cold
// load of a partition then reads the topic marker too.
func zzPerfFColdSnapshotter(tb testing.TB, parts int) *Snapshotter {
	tb.Helper()
	ms := newRuntimeFakeMetastore()
	ms.topics["t"] = topic.Topic{Name: "t", ID: "00000000000000f6", Partitions: parts, RetentionMs: int64(7 * 24 * time.Hour / time.Millisecond)}
	logs := NewLogs(tb.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	tb.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(wp8Caps, nil)
	for p := range parts {
		l, err := logs.Get("t", p)
		if err != nil {
			tb.Fatal(err)
		}
		for range 10 {
			if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))); err != nil {
				tb.Fatal(err)
			}
		}
		if err := l.AdvanceHighWatermark(10); err != nil {
			tb.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	logs.EvictIdleOnce(time.Nanosecond)
	if got := logs.OpenCount(); got != 0 {
		tb.Fatalf("%d logs still open after eviction", got)
	}
	s := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	snap, err := s.Snapshot(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	if len(snap) != 1 || len(snap[0].Partitions) != parts {
		tb.Fatalf("warm snapshot = %+v, want %d closed partitions of one topic", snap, parts)
	}
	return s
}

// BenchmarkPerfFSnapshotColdReread times one metrics snapshot that
// reloads every closed partition from its files, as a poll does once
// per coldRefresh for each: the cache is cleared before every snapshot.
// Unlike BenchmarkWP8Snapshot's closed-reread, the topic has an ID, so
// each load also reads the topic marker.
func BenchmarkPerfFSnapshotColdReread(b *testing.B) {
	for _, parts := range []int{64, 640} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			s := zzPerfFColdSnapshotter(b, parts)
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				clear(s.cold)
				if _, err := s.Snapshot(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
