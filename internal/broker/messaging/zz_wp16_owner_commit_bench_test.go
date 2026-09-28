package messaging

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// zzWP16BusyCommitter runs a consumer offset committer on the default
// interval over parts partitions that are all dirty, with a changed
// acked-ahead set, on every tick: the steady out-of-order ack load. It
// shares the disk with whatever the benchmark measures.
func zzWP16BusyCommitter(b *testing.B, parts int, interval time.Duration) {
	b.Helper()
	dataDir := b.TempDir()
	for p := range parts {
		if err := os.MkdirAll(storage.TopicPartitionDir(dataDir, "acks", p), 0o755); err != nil {
			b.Fatal(err)
		}
	}
	var version atomic.Uint64
	c := runtime.NewConsumerOffsetCommitter(dataDir, interval, nil)
	c.SetAheadSource(func(_ string, _ int) (int64, []int64, uint64, bool) {
		v := version.Load()
		return int64(v) * 10, []int64{int64(v)*10 + 2}, v, true
	})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(interval / 4)
		defer tick.Stop()
		for {
			v := version.Add(1)
			for p := range parts {
				c.Commit("acks", p, int64(v)*10)
			}
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	})
	b.Cleanup(func() {
		close(stop)
		wg.Wait()
		_ = c.Close()
	})
}

// BenchmarkZZWP16OwnerCommitUnderOffsetFlush is an owner's produce
// commit (a 24-record batch: append, data sync, read-back) while the
// consumer offset committer flushes a node's dirty partitions on the
// same disk. parts=0 is the commit alone. Besides the commit's time it
// reports its p50 and p99, the committer's syncs per second and the
// cycle, the time it took to sync every partition once: how far the
// persisted offsets lag the acks. Run with -benchtime=3s or more, so
// the commits span several cycles.
func BenchmarkZZWP16OwnerCommitUnderOffsetFlush(b *testing.B) {
	for _, parts := range []int{0, 12, 32, 64, 256} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, 1)
			var syncs atomic.Int64
			restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
				if op == syncfile.OpSyncData && strings.HasPrefix(filepath.Base(path), "consumer.") {
					syncs.Add(1)
				}
				return nil
			})
			defer restore()
			if parts > 0 {
				zzWP16BusyCommitter(b, parts, 100*time.Millisecond)
				// Let the committer reach its steady state.
				time.Sleep(time.Second)
			}
			records := zzWP7aBatch(24, "t-id")
			ctx := context.Background()
			took := make([]time.Duration, 0, b.N)
			syncs.Store(0)
			start := time.Now()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				t0 := time.Now()
				if _, err := e.CommitAcceptedProduceBatch(ctx, records); err != nil {
					b.Fatal(err)
				}
				took = append(took, time.Since(t0))
			}
			b.StopTimer()
			elapsed := time.Since(start)
			slices.Sort(took)
			b.ReportMetric(float64(took[len(took)/2])/1e6, "p50-ms")
			b.ReportMetric(float64(took[len(took)*99/100])/1e6, "p99-ms")
			if n := syncs.Load(); parts > 0 && n > 0 {
				rate := float64(n) / elapsed.Seconds()
				b.ReportMetric(rate, "csyncs/s")
				b.ReportMetric(float64(parts)/rate*1e3, "cycle-ms")
			}
		})
	}
}

// BenchmarkZZWP16SpreadCommitUnderOffsetFlush is 8 owners' dispatch
// streams, each committing 24-record batches to its own partition as
// fast as it can, while the consumer offset committer flushes a node's
// dirty partitions on the same disk: the disk already busy, where every
// sync the committer adds is one produce does not get. One op is one
// commit; it reports the commits' throughput, p50 and p99. Run with
// -benchtime=3s or more.
func BenchmarkZZWP16SpreadCommitUnderOffsetFlush(b *testing.B) {
	for _, parts := range []int{0, 12, 64, 256} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, 8)
			if parts > 0 {
				zzWP16BusyCommitter(b, parts, 100*time.Millisecond)
				time.Sleep(time.Second)
			}
			ctx := context.Background()
			batches := make([][]ingress.ProduceRecord, 8)
			for p := range batches {
				batches[p] = zzWP7aBatch(24, "t-id")
				for i := range batches[p] {
					batches[p][i].TargetPartition = p
				}
			}
			var next atomic.Int64
			took := make([][]time.Duration, len(batches))
			var wg sync.WaitGroup
			start := time.Now()
			b.ResetTimer()
			for p := range batches {
				wg.Go(func() {
					for next.Add(1) <= int64(b.N) {
						t0 := time.Now()
						if _, err := e.CommitAcceptedProduceBatch(ctx, batches[p]); err != nil {
							b.Error(err)
							return
						}
						took[p] = append(took[p], time.Since(t0))
					}
				})
			}
			wg.Wait()
			b.StopTimer()
			elapsed := time.Since(start)
			all := slices.Concat(took...)
			slices.Sort(all)
			b.ReportMetric(float64(len(all))/elapsed.Seconds(), "commits/s")
			b.ReportMetric(float64(all[len(all)/2])/1e6, "p50-ms")
			b.ReportMetric(float64(all[len(all)*99/100])/1e6, "p99-ms")
		})
	}
}
