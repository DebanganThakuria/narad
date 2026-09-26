package messaging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
	"unsafe"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP11BFanoutEngine is an engine over the production storage options
// (zstd frames) with one parent partition holding n committed records
// of size bytes, waited out until every record is flushed to a frame.
func zzWP11BFanoutEngine(tb testing.TB, n, size int) *Engine {
	tb.Helper()
	ms := newMessagingFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 1}
	opts := storage.DefaultOptions()
	opts.FlushInterval = time.Millisecond
	logs := runtime.NewLogs(tb.TempDir(), opts, ms, nil)
	tb.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	tb.Cleanup(func() { e.dispatch.close() })

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	const chunk = 512
	for start := 0; start < n; start += chunk {
		records := make([]ingress.ProduceRecord, 0, chunk)
		for i := start; i < min(start+chunk, n); i++ {
			records = append(records, ingress.ProduceRecord{
				Topic: "parent", Key: fmt.Sprintf("key-%d", i), TargetPartition: 0, Payload: payload,
			})
		}
		if _, err := e.CommitAcceptedProduceBatch(context.Background(), records); err != nil {
			tb.Fatalf("CommitAcceptedProduceBatch: %v", err)
		}
	}
	time.Sleep(20 * time.Millisecond) // let the flusher move the tail out of the write buffer
	return e
}

// The slab read hands out the log's own record bytes instead of a copy
// per record: the child commit copies the payload again anyway (into
// the keyed envelope or the RPC frame), so the first copy is garbage.
// Two reads of the same range therefore return the same backing bytes.
func TestZZWP11BFanoutSlabSharesRecordBytes(t *testing.T) {
	e := zzWP11BFanoutEngine(t, 64, 128)
	opts := topic.FanoutReadOpts{FromOffset: 0, MaxRecords: 64, MaxBytes: 1 << 20}
	first, err := e.ReadFanoutSlab(context.Background(), "parent", 0, opts)
	if err != nil {
		t.Fatalf("ReadFanoutSlab: %v", err)
	}
	second, err := e.ReadFanoutSlab(context.Background(), "parent", 0, opts)
	if err != nil {
		t.Fatalf("ReadFanoutSlab again: %v", err)
	}
	if len(first.Records) != 64 || len(second.Records) != 64 {
		t.Fatalf("slabs hold %d and %d records, want 64", len(first.Records), len(second.Records))
	}
	for i := range first.Records {
		a, b := first.Records[i], second.Records[i]
		if a.Key != b.Key || string(a.Payload) != string(b.Payload) {
			t.Fatalf("record %d differs between reads", i)
		}
		if unsafe.SliceData(a.Payload) != unsafe.SliceData(b.Payload) {
			t.Fatalf("record %d payload copied on every slab read, want the log's shared bytes", i)
		}
	}
}

// BenchmarkZZWP11BFanoutReadSlab measures one full slab read (4096 x
// 1 KiB) through ReadFanoutSlab against a warm frame cache.
func BenchmarkZZWP11BFanoutReadSlab(b *testing.B) {
	const n, size = 4096, 1024
	e := zzWP11BFanoutEngine(b, n, size)
	ctx := context.Background()
	opts := topic.FanoutReadOpts{FromOffset: 0, MaxRecords: n, MaxBytes: 8 << 20}
	if _, err := e.ReadFanoutSlab(ctx, "parent", 0, opts); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(n * size)
	b.ResetTimer()
	for b.Loop() {
		slab, err := e.ReadFanoutSlab(ctx, "parent", 0, opts)
		if err != nil || len(slab.Records) != n {
			b.Fatalf("slab: %d records, err %v", len(slab.Records), err)
		}
	}
}
