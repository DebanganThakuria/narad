package messaging

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
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
// same disk. parts=0 is the commit alone.
func BenchmarkZZWP16OwnerCommitUnderOffsetFlush(b *testing.B) {
	for _, parts := range []int{0, 12, 64, 256} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, 1)
			if parts > 0 {
				zzWP16BusyCommitter(b, parts, 100*time.Millisecond)
				// Let the committer reach its steady state.
				time.Sleep(300 * time.Millisecond)
			}
			records := zzWP7aBatch(24, "t-id")
			ctx := context.Background()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := e.CommitAcceptedProduceBatch(ctx, records); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
