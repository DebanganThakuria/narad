package runtime

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP16Source is a per-partition AheadSource whose state a test or a
// benchmark advances between flushes.
type zzWP16Source struct {
	committed []int64
	offsets   [][]int64
	version   []uint64
}

func newZZWP16Source(parts int) *zzWP16Source {
	return &zzWP16Source{
		committed: make([]int64, parts),
		offsets:   make([][]int64, parts),
		version:   make([]uint64, parts),
	}
}

func (s *zzWP16Source) source(_ string, p int) (int64, []int64, uint64, bool) {
	return s.committed[p], s.offsets[p], s.version[p], true
}

// step is the steady PCA shape between two flushes: every partition's
// frontier moved and its acked-ahead set changed, so each costs the
// flush one consumer.ahead write and its data sync.
func (s *zzWP16Source) step(c *ConsumerOffsetCommitter, i int) {
	for p := range s.committed {
		s.committed[p] = int64(i+1) * 10
		s.offsets[p] = []int64{s.committed[p] + 2, s.committed[p] + 5}
		s.version[p]++
		c.Commit("t", p, s.committed[p])
	}
}

// zzWP16Committer builds a committer over parts real partition
// directories, its loop idle (flushes are driven by hand), with every
// partition's consumer.ahead already created.
func zzWP16Committer(tb testing.TB, parts int) (*ConsumerOffsetCommitter, *zzWP16Source, string) {
	tb.Helper()
	dataDir := tb.TempDir()
	for p := range parts {
		if err := os.MkdirAll(storage.TopicPartitionDir(dataDir, "t", p), 0o755); err != nil {
			tb.Fatal(err)
		}
	}
	c := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	tb.Cleanup(func() { _ = c.Close() })
	src := newZZWP16Source(parts)
	c.SetAheadSource(src.source)
	src.step(c, 0)
	if err := c.flush(); err != nil {
		tb.Fatal(err)
	}
	return c, src, dataDir
}

// BenchmarkZZWP16CommitterFlush times one committer flush over a node's
// dirty partitions on real files, data syncs on: one flush is one tick
// of the committer under steady out-of-order acks.
func BenchmarkZZWP16CommitterFlush(b *testing.B) {
	for _, parts := range []int{12, 64, 256} {
		b.Run(fmt.Sprintf("parts=%d", parts), func(b *testing.B) {
			c, src, _ := zzWP16Committer(b, parts)
			b.ResetTimer()
			for i := range b.N {
				b.StopTimer()
				src.step(c, i+1)
				b.StartTimer()
				if err := c.flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
