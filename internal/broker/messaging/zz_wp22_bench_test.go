package messaging

import (
	"context"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// zzWP22NoFsync makes every fsync report success without running, so a
// commit benchmark measures the cycle's own work rather than the disk.
func zzWP22NoFsync(b *testing.B) {
	restore := syncfile.SetFaultHook(func(op syncfile.Op, _ string) error {
		if op == syncfile.OpSyncData || op == syncfile.OpSync {
			return syncfile.ErrLie
		}
		return nil
	})
	b.Cleanup(restore)
}

// BenchmarkWP22CommitCycleNoFsync is BenchmarkZZWP7aCommitBatch24 and
// BenchmarkZZWP7aCommitSpread without the fsync, so the per-cycle cost
// of checking that the log is still current shows.
func BenchmarkWP22CommitCycleNoFsync(b *testing.B) {
	b.Run("batch24", func(b *testing.B) {
		zzWP22NoFsync(b)
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
	})
	b.Run("spread8", func(b *testing.B) {
		zzWP22NoFsync(b)
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
	})
}
