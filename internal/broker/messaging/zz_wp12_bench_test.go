package messaging

import (
	"context"
	"fmt"
	"testing"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP12Fill appends n records to each of the engine's parts partitions
// of "t" and makes them visible.
func zzWP12Fill(b *testing.B, e *Engine, parts, n int) {
	b.Helper()
	rec := storage.EncodeKeyedRecord("", 1, []byte(`{"id":1,"v":"abcdefghijklmnop"}`))
	for p := range parts {
		l, err := e.logs.Get("t", p)
		if err != nil {
			b.Fatal(err)
		}
		for done := 0; done < n; {
			k := min(4096, n-done)
			batch := make([][]byte, k)
			for i := range batch {
				batch[i] = rec
			}
			_, last, err := l.AppendBatch(batch)
			if err != nil {
				b.Fatal(err)
			}
			if err := l.AdvanceHighWatermark(last + 1); err != nil {
				b.Fatal(err)
			}
			done += k
		}
	}
}

// BenchmarkZZWP12ConsumeBatchVsSingle reserves and acks records on the
// engine: one Consume per record, against one ConsumeBatch per n
// records. ns/op is per record either way.
func BenchmarkZZWP12ConsumeBatchVsSingle(b *testing.B) {
	const parts = 8
	ack := func(b *testing.B, e *Engine, msg topic.Message) {
		h, err := consumer.DecodeHandle(msg.ReceiptHandle)
		if err != nil {
			b.Fatal(err)
		}
		if err := e.Ack(context.Background(), "t", h); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("single", func(b *testing.B) {
		e := zzWP7aBenchEngine(b, parts)
		zzWP12Fill(b, e, parts, b.N/parts+2)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			msg, found, err := e.Consume(ctx, "t", ConsumeOpts{})
			if err != nil || !found {
				b.Fatal(err, found)
			}
			ack(b, e, msg)
		}
	})
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, parts)
			zzWP12Fill(b, e, parts, b.N/parts+n+2)
			ctx := context.Background()
			buf := make([]topic.Message, 0, n)
			b.ReportAllocs()
			b.ResetTimer()
			for done := 0; done < b.N; {
				msgs, _, err := e.ConsumeBatch(ctx, "t", ConsumeOpts{}, n, buf[:0])
				if err != nil || len(msgs) == 0 {
					b.Fatal(err, len(msgs))
				}
				for _, msg := range msgs {
					ack(b, e, msg)
				}
				done += len(msgs)
			}
		})
	}
}
