package messaging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// zzWP25AcceptEngine builds an Engine with the node's real partitioner
// over a fake metastore holding eight 12-partition topics and a real
// ingress WAL, for the accept path only.
func zzWP25AcceptEngine(b *testing.B) (*Engine, []string) {
	b.Helper()
	ms := newMessagingFakeMetastore()
	names := make([]string, 8)
	for i := range names {
		names[i] = fmt.Sprintf("orders-%d", i)
		ms.topics[names[i]] = topic.Topic{Name: names[i], ID: fmt.Sprintf("%016x", i+1), Partitions: 12}
	}
	manager, err := ingress.OpenManager(b.TempDir(), wal.Options{SegmentBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = manager.Close() })
	logs := runtime.NewLogs(b.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	b.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	engine := NewEngine(ms, schema.NewAlwaysValid(), partition.NewHashRoundRobin(), offsets, logs, manager, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	return engine, names
}

// BenchmarkZZWP25AcceptProduceKeyless is Engine.AcceptProduce for a
// keyless message, which takes the partitioner's round-robin: one
// caller on one topic, and 8 parallel callers rotating over 8 topics.
// "nosync" skips the WAL data sync so the device flush does not bury
// the CPU cost of the accept; "sync" is the path as it runs.
func BenchmarkZZWP25AcceptProduceKeyless(b *testing.B) {
	payload := bytes.Repeat([]byte("a"), 300)
	for _, durable := range []bool{false, true} {
		mode := "sync"
		if !durable {
			mode = "nosync"
		}
		b.Run(mode+"/serial", func(b *testing.B) {
			if !durable {
				defer zzWP25SkipDataSync()()
			}
			engine, names := zzWP25AcceptEngine(b)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := engine.AcceptProduce(context.Background(), names[0], "", payload); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(mode+"/parallel-8-topics", func(b *testing.B) {
			if !durable {
				defer zzWP25SkipDataSync()()
			}
			engine, names := zzWP25AcceptEngine(b)
			b.SetParallelism(8)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if _, err := engine.AcceptProduce(context.Background(), names[i&7], "", payload); err != nil {
						b.Error(err)
						return
					}
					i++
				}
			})
		})
	}
}

// zzWP25SkipDataSync reports every WAL data sync done without running
// it, until the returned restore is called.
func zzWP25SkipDataSync() func() {
	return syncfile.SetFaultHook(func(op syncfile.Op, _ string) error {
		if op == syncfile.OpSyncData {
			return syncfile.ErrLie
		}
		return nil
	})
}
