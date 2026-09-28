package ingress

import (
	"bytes"
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// zzWP5FillManager accepts n records (512-byte payload, 27-byte topic,
// 11-byte key) from 64 goroutines so the WAL batches them.
func zzWP5FillManager(tb testing.TB, m *Manager, n int) {
	tb.Helper()
	payload := bytes.Repeat([]byte("x"), 512)
	const topic = "orders-run-1790424109845303"
	const workers = 64
	errs := make(chan error, workers)
	for range workers {
		go func() {
			for i := range n / workers {
				if _, err := m.AcceptProduce(context.Background(), topic, "customer-42", i%36, payload); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range workers {
		if err := <-errs; err != nil {
			tb.Fatal(err)
		}
	}
}

// BenchmarkZZWP5ReplayProduceFromCursor4096 is one dispatcher pass over
// 4096 records: the per-record allocations of reading, decoding and
// handing each record to the dispatcher.
func BenchmarkZZWP5ReplayProduceFromCursor4096(b *testing.B) {
	m, err := OpenManager(b.TempDir(), wal.Options{SegmentBytes: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	const n = 4096
	zzWP5FillManager(b, m, n)
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count := 0
		err := m.ReplayProduceFromCursor(wal.Cursor{}, func(r ProduceRecord, _ wal.Cursor) error {
			count++
			sink += len(r.Payload) + len(r.Topic) + len(r.Key)
			return nil
		})
		if err != nil || count != n {
			b.Fatalf("replay: %v count=%d", err, count)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/record")
	_ = sink
}

// BenchmarkZZWP5ReplayProduceTail is the steady-state dispatcher pass:
// resume from a cursor near the end of the log and read the last 64
// records, so the fixed per-pass cost (listing, open, read buffer) is
// visible next to the per-record cost.
func BenchmarkZZWP5ReplayProduceTail(b *testing.B) {
	m, err := OpenManager(b.TempDir(), wal.Options{SegmentBytes: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	const n = 4096
	zzWP5FillManager(b, m, n)
	var cursor wal.Cursor
	err = m.ReplayProduceFromCursor(wal.Cursor{}, func(r ProduceRecord, next wal.Cursor) error {
		if r.WAL.Seq == n-65 {
			cursor = next
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count := 0
		err := m.ReplayProduceFromCursor(cursor, func(ProduceRecord, wal.Cursor) error {
			count++
			return nil
		})
		if err != nil || count != 64 {
			b.Fatalf("replay: %v count=%d", err, count)
		}
	}
}

// BenchmarkZZWP5AcceptProduceParallel is the accept path under 96
// concurrent producers: encode into the group-commit buffer, then wait
// for the batch's sync.
func BenchmarkZZWP5AcceptProduceParallel(b *testing.B) {
	m, err := OpenManager(b.TempDir(), wal.Options{SegmentBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	payload := bytes.Repeat([]byte("a"), 300)
	b.SetParallelism(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := m.AcceptProduce(context.Background(), "orders-run-1790424109845303", "customer-42", 3, payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkZZWP5ReplayProducePeekStuck is a stuck-mode dispatcher pass:
// the window is 4096 records of which the dispatcher already committed
// seven in eight on earlier passes, and passes over them with a peek.
func BenchmarkZZWP5ReplayProducePeekStuck(b *testing.B) {
	m, err := OpenManager(b.TempDir(), wal.Options{SegmentBytes: 64 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	const n = 4096
	zzWP5FillManager(b, m, n)
	committedAhead := func(id wal.RecordID, _ wal.Cursor) (bool, error) { return id.Seq%8 != 0, nil }
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count := 0
		if err := m.ReplayProduceFromCursorPeek(wal.Cursor{}, committedAhead, func(ProduceRecord, wal.Cursor) error {
			count++
			return nil
		}); err != nil || count != n/8 {
			b.Fatalf("replay: %v count=%d", err, count)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/record")
}
