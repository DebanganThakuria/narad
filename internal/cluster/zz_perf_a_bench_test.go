package cluster

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// zzPerfATimer is the part of *testing.B a drain drives: the timer runs
// while the dispatcher drains and nowhere else.
type zzPerfATimer interface {
	StartTimer()
	StopTimer()
}

// zzPerfAFill accepts n records of 256 B into topic "orders", record i
// on partition i%partitions, in AcceptProduceBatch calls of 1024 (one
// WAL group commit each), the shape a batch producer leaves behind.
func zzPerfAFill(tb testing.TB, m *ingress.Manager, n, partitions int) {
	tb.Helper()
	payload := bytes.Repeat([]byte("x"), 256)
	batch := make([]ingress.BatchRecord, 0, 1024)
	for i := range n {
		batch = append(batch, ingress.BatchRecord{Key: "k", TargetPartition: i % partitions, Payload: payload})
		if len(batch) == cap(batch) || i == n-1 {
			if _, err := m.AcceptProduceBatch(context.Background(), "orders", "", batch); err != nil {
				tb.Fatal(err)
			}
			batch = batch[:0]
		}
	}
}

// zzPerfADrain builds a node that owns every partition of "orders",
// leaves n accepted records in its WAL, and times the Run loop draining
// them into a local committer that takes delay per commit (the owner's
// fsync). The timer runs from the start of Run until the last record
// committed; setup and shutdown stay outside it. It returns that time.
func zzPerfADrain(tb testing.TB, tm zzPerfATimer, n, partitions int, delay time.Duration) time.Duration {
	tb.Helper()
	store := zzWP6NewStore(tb)
	seedProduceDispatchTopicPartitionsTB(tb, store, "node-self", partitions)
	m := zzWP6Manager(tb)
	zzPerfAFill(tb, m, n, partitions)
	sink := &zzWP6Sink{delay: delay}
	d := NewProduceDispatcher(m, store, "node-self", sink, nil, nil, ProduceDispatcherConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	deadline := time.Now().Add(5 * time.Minute)
	start := time.Now()
	tm.StartTimer()
	go func() { d.Run(ctx); close(done) }()
	for sink.committed.Load() < int64(n) && time.Now().Before(deadline) {
		time.Sleep(200 * time.Microsecond)
	}
	tm.StopTimer()
	took := time.Since(start)
	cancel()
	<-done
	if got := sink.committed.Load(); got < int64(n) {
		tb.Fatalf("drained %d of %d records before the deadline", got, n)
	}
	return took
}

// BenchmarkPerfAHotDestinationDrain times the Run loop draining a
// backlog of 131072 records of 256 B. 1p-*: every record on one local
// partition, which fills its commit queue (perDestCap) at once, so the
// rest of the backlog is skipped and read again by the rescan after
// each commit. 36p-1ms: the same backlog spread over 36 local
// partitions, the control where each record is decoded once. The delay
// is what each local commit takes. records/s is the drain throughput;
// B/op and allocs/op include the rescans' decodes.
func BenchmarkPerfAHotDestinationDrain(b *testing.B) {
	for _, tc := range []struct {
		name       string
		partitions int
		delay      time.Duration
	}{
		{"1p-1ms", 1, time.Millisecond},
		{"1p-0ms", 1, 0},
		{"36p-1ms", 36, time.Millisecond},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const n = 131072
			b.ReportAllocs()
			b.StopTimer()
			var total time.Duration
			for range b.N {
				total += zzPerfADrain(b, b, n, tc.partitions, tc.delay)
			}
			b.ReportMetric(float64(n)*float64(b.N)/total.Seconds(), "records/s")
		})
	}
}
