package messaging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP7aBenchEngine builds an Engine over a fake metastore holding one
// topic "t" with parts partitions, every log open and every shard
// created once, as on a warm broker.
func zzWP7aBenchEngine(b *testing.B, parts int) *Engine {
	b.Helper()
	dir := b.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["t"] = topic.Topic{Name: "t", ID: "t-id", Partitions: parts, VisibilityTimeoutMs: 30000}
	ms.topicVersions["t"] = 7
	logs := runtime.NewLogs(dir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	b.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1 << 20, MaxAckedAhead: 1 << 20}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	b.Cleanup(func() { e.dispatch.close() })
	for p := 0; p < parts; p++ {
		if _, err := logs.Get("t", p); err != nil {
			b.Fatal(err)
		}
	}
	if _, _, err := e.Consume(context.Background(), "t", ConsumeOpts{}); err != nil {
		b.Fatal(err)
	}
	return e
}

func zzWP7aBatch(n int, topicID string) []ingress.ProduceRecord {
	payload := []byte(`{"id":1,"v":"abcdefghijklmnopqrstuvwxyz0123456789"}`)
	records := make([]ingress.ProduceRecord, n)
	for i := range records {
		records[i] = ingress.ProduceRecord{Topic: "t", TopicID: topicID, TargetPartition: 0, Key: "k", Payload: payload}
	}
	return records
}

// BenchmarkZZWP7aCommitBatch24 is one committer on one partition: a
// 24-record batch per op, the size the devstack dispatchers send.
func BenchmarkZZWP7aCommitBatch24(b *testing.B) {
	e := zzWP7aBenchEngine(b, 1)
	records := zzWP7aBatch(24, "t-id")
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := e.CommitAcceptedProduceBatch(ctx, records); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZZWP7aCommitConcurrent is k committers (one per node's
// dispatcher) each sending a 24-record batch to the same partition at
// the same moment; one op is one round of k batches.
func BenchmarkZZWP7aCommitConcurrent(b *testing.B) {
	for _, k := range []int{3, 8} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, 1)
			records := zzWP7aBatch(24, "t-id")
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				for range k {
					wg.Go(func() {
						if _, err := e.CommitAcceptedProduceBatch(ctx, records); err != nil {
							b.Error(err)
						}
					})
				}
				wg.Wait()
			}
			b.ReportMetric(float64(b.N*k*24)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

// BenchmarkZZWP7aCommitSpread is 8 committers on 8 different
// partitions: nothing to combine, so it shows the combiner's own cost.
func BenchmarkZZWP7aCommitSpread(b *testing.B) {
	e := zzWP7aBenchEngine(b, 8)
	ctx := context.Background()
	batches := make([][]ingress.ProduceRecord, 8)
	for p := range batches {
		batches[p] = zzWP7aBatch(24, "t-id")
		for i := range batches[p] {
			batches[p][i].TargetPartition = p
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for p := range 8 {
			wg.Go(func() {
				if _, err := e.CommitAcceptedProduceBatch(ctx, batches[p]); err != nil {
					b.Error(err)
				}
			})
		}
		wg.Wait()
	}
}

// BenchmarkZZWP7aConsumeEmpty: an empty topic, so every consume scans
// all P partitions and answers "empty".
func BenchmarkZZWP7aConsumeEmpty(b *testing.B) {
	for _, parts := range []int{1, 8, 12} {
		b.Run(fmt.Sprintf("P=%d", parts), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, parts)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, found, err := e.Consume(ctx, "t", ConsumeOpts{})
				if err != nil || found {
					b.Fatal(err, found)
				}
			}
		})
	}
}

func BenchmarkZZWP7aConsumeEmptyParallel(b *testing.B) {
	e := zzWP7aBenchEngine(b, 8)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, found, err := e.Consume(ctx, "t", ConsumeOpts{})
			if err != nil || found {
				panic(fmt.Sprint(err, found))
			}
		}
	})
}

// BenchmarkZZWP7aProbeEmpty is a peer probe of an empty topic: the
// ConsumeProbe miss path, which also builds the parked waiter.
func BenchmarkZZWP7aProbeEmpty(b *testing.B) {
	e := zzWP7aBenchEngine(b, 8)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, found, _, err := e.ConsumeProbe(ctx, "t", ConsumeOpts{})
		if err != nil || found {
			b.Fatal(err, found)
		}
	}
}

// BenchmarkZZWP7aConsumeHitAck: every partition has data, each consume
// hits on the partition the rotating cursor starts at and is acked.
func BenchmarkZZWP7aConsumeHitAck(b *testing.B) {
	for _, parts := range []int{1, 8} {
		b.Run(fmt.Sprintf("P=%d", parts), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, parts)
			per := b.N/parts + 2
			rec := storage.EncodeKeyedRecord("", 1, []byte(`{"id":1,"v":"abcdefghijklmnop"}`))
			for p := 0; p < parts; p++ {
				l, err := e.logs.Get("t", p)
				if err != nil {
					b.Fatal(err)
				}
				for done := 0; done < per; {
					n := min(4096, per-done)
					batch := make([][]byte, n)
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
					done += n
				}
			}
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg, found, err := e.Consume(ctx, "t", ConsumeOpts{})
				if err != nil || !found {
					b.Fatal(err, found)
				}
				h, err := consumer.DecodeHandle(msg.ReceiptHandle)
				if err != nil {
					b.Fatal(err)
				}
				if err := e.Ack(ctx, "t", h); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkZZWP7aPauseChecks is the pair of handoff-pause lookups the
// produce and consume paths make per partition when nothing is paused.
func BenchmarkZZWP7aPauseChecks(b *testing.B) {
	e := zzWP7aBenchEngine(b, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if e.isConsumePaused("t", i&7) || e.isProducePaused("t", i&7) {
			b.Fatal("paused")
		}
	}
}

func BenchmarkZZWP7aPauseChecksParallel(b *testing.B) {
	e := zzWP7aBenchEngine(b, 8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if e.isConsumePaused("t", i&7) || e.isProducePaused("t", i&7) {
				panic("paused")
			}
			i++
		}
	})
}

// BenchmarkZZWP7aWakeNotifier is the callback every high-watermark
// advance runs on the committing goroutine, for a topic with a parked
// consumer (the path that marks the topic dirty) and without one.
func BenchmarkZZWP7aWakeNotifier(b *testing.B) {
	for _, parked := range []bool{false, true} {
		b.Run(fmt.Sprintf("parked=%v", parked), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, 1)
			wake := e.dispatch.wakeNotifier("t")
			if parked {
				// A consumer parked on the empty topic for the whole run:
				// each wake marks the topic dirty and the pump rescans it.
				w := &waiter{cw: &ConsumeWaiter{topic: "t", scan: []int{0}, visibilityTimeout: time.Minute, start: time.Now()}, ch: make(chan waiterDelivery, 1)}
				e.dispatch.enqueue("t", w)
				b.Cleanup(func() { e.dispatch.dequeue("t", w) })
				for !e.dispatch.stateFor("t").hasWaiters.Load() {
					time.Sleep(time.Millisecond)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wake()
			}
		})
	}
}
