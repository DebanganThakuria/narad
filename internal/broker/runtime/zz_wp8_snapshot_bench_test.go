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

// BenchmarkWP8Snapshot times one metrics snapshot of a node owning 64
// partitions. open: every log open with a consumer shard (the busy
// case, polled every 5s). closed: every log idle-evicted with a
// backlog and no shard, the case the poller used to skip. closed-reread
// drops the cached readings before every snapshot, the cost paid once
// per coldRefresh.
func BenchmarkWP8Snapshot(b *testing.B) {
	const parts = 64
	for _, mode := range []string{"open", "closed", "closed-reread"} {
		name, closed, reread := mode, mode != "open", mode == "closed-reread"
		b.Run(fmt.Sprintf("%s/parts=%d", name, parts), func(b *testing.B) {
			dataDir := b.TempDir()
			ms := newRuntimeFakeMetastore()
			ms.topics["t"] = topic.Topic{Name: "t", Partitions: parts, RetentionMs: int64(7 * 24 * time.Hour / time.Millisecond)}
			logs := NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
			b.Cleanup(func() { _ = logs.CloseAll() })
			offsets := consumer.NewInFlight(wp8Caps, nil)
			ctx := context.Background()
			for p := range parts {
				l, err := logs.Get("t", p)
				if err != nil {
					b.Fatal(err)
				}
				for range 10 {
					if _, err := l.Append(storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))); err != nil {
						b.Fatal(err)
					}
				}
				if err := l.AdvanceHighWatermark(10); err != nil {
					b.Fatal(err)
				}
				if !closed {
					if _, err := offsets.ReserveNext(ctx, "t", p, time.Minute, 10); err != nil {
						b.Fatal(err)
					}
				}
			}
			time.Sleep(20 * time.Millisecond)
			if closed {
				logs.EvictIdleOnce(time.Nanosecond)
			}
			s := NewSnapshotter(ms, offsets, logs, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
			if _, err := s.Snapshot(ctx); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if reread {
					clear(s.cold)
				}
				if _, err := s.Snapshot(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
