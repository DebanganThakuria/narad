package ingress

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// zzWP19BenchBatch is the per-message cost of AcceptProduceBatch for a
// batch of size records of 300 bytes, from callers concurrent callers.
// Every call waits for its batch to be durable, so ns/msg is the group
// commit cost a batch pays divided by its size.
func zzWP19BenchBatch(b *testing.B, size, callers int) {
	m, err := OpenManager(b.TempDir(), wal.Options{SegmentBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	payload := bytes.Repeat([]byte("b"), 300)
	records := make([]BatchRecord, size)
	for i := range records {
		records[i] = BatchRecord{Key: fmt.Sprintf("customer-%d", i%7), TargetPartition: i % 3, Payload: payload}
	}
	var msgs atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	if callers == 1 {
		for b.Loop() {
			if _, err := m.AcceptProduceBatch(context.Background(), "orders-run-1790424109845303", "id-1", records); err != nil {
				b.Fatal(err)
			}
			msgs.Add(int64(size))
		}
	} else {
		b.SetParallelism(callers)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := m.AcceptProduceBatch(context.Background(), "orders-run-1790424109845303", "id-1", records); err != nil {
					b.Error(err)
					return
				}
				msgs.Add(int64(size))
			}
		})
	}
	b.StopTimer()
	if n := msgs.Load(); n > 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/msg")
	}
}

// BenchmarkZZWP19AcceptProduceBatch is batch produce through the real
// ingress WAL: one caller (every batch pays its own group commits) and
// 8*GOMAXPROCS callers (batches share them), at batch sizes 1, 10 and
// 100.
func BenchmarkZZWP19AcceptProduceBatch(b *testing.B) {
	for _, callers := range []int{1, 8} {
		name := "serial"
		if callers > 1 {
			name = "parallel"
		}
		for _, size := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("%s/batch=%d", name, size), func(b *testing.B) {
				zzWP19BenchBatch(b, size, callers)
			})
		}
	}
}

// BenchmarkZZWP19AcceptProduceBatchNoSync is the parallel case with
// every WAL data sync skipped (reported done without running), so what
// is left is the CPU and lock cost of staging a batch: encoding, the
// WAL's append lock, and the wait hand-off, without the device flush
// that dominates (and adds noise to) the benchmark above.
func BenchmarkZZWP19AcceptProduceBatchNoSync(b *testing.B) {
	restore := syncfile.SetFaultHook(func(op syncfile.Op, _ string) error {
		if op == syncfile.OpSyncData {
			return syncfile.ErrLie
		}
		return nil
	})
	defer restore()
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("parallel/batch=%d", size), func(b *testing.B) {
			zzWP19BenchBatch(b, size, 8)
		})
	}
}

// BenchmarkZZWP19AcceptProduceSerial is one caller producing single
// records: the per-message cost batching is measured against.
func BenchmarkZZWP19AcceptProduceSerial(b *testing.B) {
	m, err := OpenManager(b.TempDir(), wal.Options{SegmentBytes: 1 << 30})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	payload := bytes.Repeat([]byte("s"), 300)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := m.AcceptProduce(context.Background(), "orders-run-1790424109845303", "customer-42", 3, payload); err != nil {
			b.Fatal(err)
		}
	}
}
