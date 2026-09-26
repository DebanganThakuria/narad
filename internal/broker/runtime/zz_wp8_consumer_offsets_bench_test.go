package runtime

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// wp8BenchSource is a per-partition AheadSource whose state the
// benchmark advances between flushes.
type wp8BenchSource struct {
	committed []int64
	offsets   [][]int64
	version   []uint64
}

func (s *wp8BenchSource) source(_ string, p int) (int64, []int64, uint64, bool) {
	return s.committed[p], s.offsets[p], s.version[p], true
}

// BenchmarkWP8CommitterFlush times one committer flush over a node's
// worth of dirty partitions. outoforder is the steady PCA shape: every
// partition's frontier moved and its acked-ahead set changed since the
// last flush. inorder moves only the frontier. fsyncs/op counts the
// data syncs the flush issued.
func BenchmarkWP8CommitterFlush(b *testing.B) {
	const parts = 12
	for _, outOfOrder := range []bool{false, true} {
		name := "inorder"
		if outOfOrder {
			name = "outoforder"
		}
		b.Run(fmt.Sprintf("%s/parts=%d", name, parts), func(b *testing.B) {
			dataDir := b.TempDir()
			for p := range parts {
				if err := os.MkdirAll(storage.TopicPartitionDir(dataDir, "t", p), 0o755); err != nil {
					b.Fatal(err)
				}
			}
			c := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
			b.Cleanup(func() { _ = c.Close() })
			src := &wp8BenchSource{
				committed: make([]int64, parts),
				offsets:   make([][]int64, parts),
				version:   make([]uint64, parts),
			}
			c.SetAheadSource(src.source)
			step := func(i int) {
				for p := range parts {
					src.committed[p] = int64(i+1) * 10
					if outOfOrder {
						src.offsets[p] = []int64{src.committed[p] + 2, src.committed[p] + 5}
						src.version[p]++
					}
					c.Commit("t", p, src.committed[p])
				}
			}
			// Warm: create both files in every partition.
			step(0)
			if err := c.flush(); err != nil {
				b.Fatal(err)
			}
			var syncs atomic.Int64
			restore := syncfile.SetFaultHook(func(op syncfile.Op, _ string) error {
				if op == syncfile.OpSyncData || op == syncfile.OpSync {
					syncs.Add(1)
				}
				return nil
			})
			defer restore()
			b.ResetTimer()
			for i := range b.N {
				step(i + 1)
				if err := c.flush(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(syncs.Load())/float64(b.N), "fsyncs/op")
		})
	}
}
