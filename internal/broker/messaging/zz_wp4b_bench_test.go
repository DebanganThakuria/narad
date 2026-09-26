package messaging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// wp4bNewStore is newTestStore for any testing.TB, so benchmarks can
// run against a real single-node metastore.
func wp4bNewStore(tb testing.TB) *metastore.Store {
	tb.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   "node-self",
		DataDir:  tb.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		tb.Fatalf("metastore.New() error = %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 3}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			return store
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatal("timed out waiting for leader")
	return nil
}

// wp4bNewEngine wires an Engine over ms with a real JSONSchema registry.
func wp4bNewEngine(tb testing.TB, ms metastore.Metastore, reg schema.Registry) *Engine {
	tb.Helper()
	logs := runtime.NewLogs(tb.TempDir(), storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	tb.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(ms, reg, fixedPartitionManager{}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
	tb.Cleanup(func() { e.dispatch.close() })
	return e
}

// The steady load driver's payload and schema: the realistic per-produce
// shape on a schema topic.
var wp4bDriverPayload = []byte(`{"id":"lc-1790424109845303000-t1-0000123456","topic":"lc-1790424109845303000-t1","sequence":123456,"key":"k-42","run_id":"lc-1790424109845303000"}`)

const wp4bDriverSchema = `{"type":"object","properties":{"id":{"type":"string"},"topic":{"type":"string"},"sequence":{"type":"integer"},"key":{"type":"string"},"run_id":{"type":"string"}},"required":["id","topic","sequence","key","run_id"],"additionalProperties":false}`

// BenchmarkWP4BValidateProducePayload is the per-produce schema cost on
// the accept path once the schema cache is warm: the version check in
// syncTopicSchemas plus the registry's Validate. Schemaless is the same
// path for a topic without a schema.
func BenchmarkWP4BValidateProducePayload(b *testing.B) {
	store := wp4bNewStore(b)
	e := wp4bNewEngine(b, store, schema.NewJSONSchema())
	ctx := context.Background()
	for _, name := range []string{"orders", "plain"} {
		if err := store.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3}); err != nil {
			b.Fatal(err)
		}
	}
	if err := store.PutSchema(ctx, "orders", 1, []byte(wp4bDriverSchema)); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, topic string }{{"schema", "orders"}, {"schemaless", "plain"}} {
		if err := e.validateProducePayload(ctx, tc.topic, wp4bDriverPayload); err != nil {
			b.Fatal(err)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := e.validateProducePayload(ctx, tc.topic, wp4bDriverPayload); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkWP4BMetadataCacheHit is the warm read of the topic and
// assignment caches every produce, consume and commit pays.
func BenchmarkWP4BMetadataCacheHit(b *testing.B) {
	store := wp4bNewStore(b)
	e := wp4bNewEngine(b, store, schema.NewAlwaysValid())
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 12}); err != nil {
		b.Fatal(err)
	}
	for p := 0; p < 12; p++ {
		if err := store.AssignPartition(ctx, "orders", p, "node-self"); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := e.getTopic(ctx, "orders"); err != nil {
		b.Fatal(err)
	}
	if _, err := e.getAssignment("orders", 3); err != nil {
		b.Fatal(err)
	}
	b.Run("getTopic", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := e.getTopic(ctx, "orders"); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("getAssignment", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := e.getAssignment("orders", 3); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// BenchmarkWP4BSchemaHydrate is the cost of one hydrate of a topic whose
// history holds versions schemas of about 40 KB each: what every produce
// that missed the schema cache paid on its own before the per-topic
// flight, and what one flight pays now.
func BenchmarkWP4BSchemaHydrate(b *testing.B) {
	for _, versions := range []int{1, 50} {
		b.Run(fmt.Sprintf("versions=%d", versions), func(b *testing.B) {
			store := wp4bNewStore(b)
			ctx := context.Background()
			if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
				b.Fatal(err)
			}
			for v := 1; v <= versions; v++ {
				if err := store.PutSchema(ctx, "orders", v, wp4bBigSchema(400, v)); err != nil {
					b.Fatal(err)
				}
			}
			reg := schema.NewJSONSchema()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := schema.Hydrate(ctx, store, reg, "orders"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// wp4bBigSchema is a wide object schema of about 100 bytes per property.
func wp4bBigSchema(props, salt int) []byte {
	out := []byte(`{"type":"object","properties":{`)
	for i := 0; i < props; i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out = fmt.Appendf(out, `"field_%05d_%d":{"type":"string","maxLength":%d,"description":"padding padding padding padding"}`, i, salt, 100+i)
	}
	return append(out, `}}`...)
}
