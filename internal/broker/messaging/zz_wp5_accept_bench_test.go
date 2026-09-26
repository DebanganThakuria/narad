package messaging

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// zzWP5AcceptEngine builds an Engine over a fake metastore holding one
// 12-partition topic and a real ingress WAL, for the accept path only.
func zzWP5AcceptEngine(tb testing.TB, topicID string) (*Engine, *ingress.Manager) {
	tb.Helper()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", ID: topicID, Partitions: 12}
	manager, err := ingress.OpenManager(tb.TempDir(), wal.Options{SegmentBytes: 1 << 30})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = manager.Close() })
	logs := runtime.NewLogs(tb.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	tb.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	engine := NewEngine(ms, schema.NewAlwaysValid(), fixedPartitioner{picked: 5}, offsets, logs, manager, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	return engine, manager
}

// BenchmarkZZWP5EngineAcceptProduceParallel is Engine.AcceptProduce
// under 96 concurrent producers: topic lookup, partition pick and the
// durable WAL accept.
func BenchmarkZZWP5EngineAcceptProduceParallel(b *testing.B) {
	engine, _ := zzWP5AcceptEngine(b, "f76e7d4b965dd285")
	payload := bytes.Repeat([]byte("a"), 300)
	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := engine.AcceptProduce(context.Background(), "orders", "customer-42", payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
