package runtime

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// BenchmarkZZWP23WriteTick times the committer's own work on a tick
// that writes every partition's window, with the syncs stubbed out: the
// snapshots, the prime loop's per-partition checks, and the page-cache
// writes, which the disk's sync time hides in the flush benchmarks. One
// op is one tick over parts dirty partitions.
func BenchmarkZZWP23WriteTick(b *testing.B) {
	nop := func(*os.File) error { return nil }
	for _, parts := range []int{64, 256} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			dataDir := b.TempDir()
			for p := range parts {
				if err := os.MkdirAll(storage.TopicPartitionDir(dataDir, "t", p), 0o755); err != nil {
					b.Fatal(err)
				}
			}
			c := newConsumerOffsetCommitter(dataDir, time.Hour, nil, committerOptions{
				io:     offsetIO{writeOut: nop, flushDevice: nop, syncDir: nop},
				manual: true,
			})
			b.Cleanup(func() { _ = c.Close() })
			src := newZZWP16Source(parts)
			c.SetAheadSource(src.source)
			src.step(c, 0)
			if err := c.flush(); err != nil {
				b.Fatal(err)
			}
			now := time.Now()
			b.ResetTimer()
			for i := range b.N {
				b.StopTimer()
				src.step(c, i+1)
				now = now.Add(c.tick)
				b.StartTimer()
				if err := c.tickAt(now, offsetTickNormal); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
