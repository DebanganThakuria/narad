package runtime

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// BenchmarkZZWP23CommitterCommit is the ack path's share of the
// committer: 8 goroutines queueing frontier commits over 1024
// partitions while the committer's loop runs beside them. One op is
// one Commit.
func BenchmarkZZWP23CommitterCommit(b *testing.B) {
	const keys, goroutines = 1024, 8
	c := NewConsumerOffsetCommitter(b.TempDir(), time.Hour, nil)
	b.Cleanup(func() { _ = c.Close() })
	topics := make([]string, 4)
	for i := range topics {
		topics[i] = fmt.Sprintf("t%d", i)
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	b.ResetTimer()
	for range goroutines {
		wg.Go(func() {
			for {
				i := next.Add(1)
				if i > int64(b.N) {
					return
				}
				k := int(i % keys)
				c.Commit(topics[k%len(topics)], k/len(topics), i)
			}
		})
	}
	wg.Wait()
}

// zzWP23BusyCommitter runs a committer on its own loop over parts
// partitions that a feeder acks out of order every 25ms, all of them
// dirty on every tick: the steady PCA load.
func zzWP23BusyCommitter(b *testing.B, parts int, opts committerOptions) {
	b.Helper()
	dataDir := b.TempDir()
	for p := range parts {
		if err := os.MkdirAll(storage.TopicPartitionDir(dataDir, "acks", p), 0o755); err != nil {
			b.Fatal(err)
		}
	}
	var version atomic.Uint64
	c := newConsumerOffsetCommitter(dataDir, 0, nil, opts)
	c.SetAheadSource(func(string, int) (int64, []int64, uint64, bool) {
		v := version.Load()
		return int64(v) * 10, []int64{int64(v)*10 + 2}, v, true
	})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		tick := time.NewTicker(25 * time.Millisecond)
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

// BenchmarkZZWP23SyncBesideCommitter is 8 produce-like streams, each
// appending a 24-record batch (1.5 KiB) to its own file and syncing it
// (syncfile.SyncData, as a commit does), beside a committer on the
// default durability interval over parts dirty partitions. spread is
// the committer as shipped; burst writes every partition out on the
// same tick of the interval, the shape the spread replaced. One op is
// one batch; it reports throughput, p50 and p99.
func BenchmarkZZWP23SyncBesideCommitter(b *testing.B) {
	batch := make([]byte, 24*64)
	for _, shape := range []string{"spread", "burst"} {
		for _, parts := range []int{0, 64, 256} {
			if parts == 0 && shape == "burst" {
				continue
			}
			b.Run(fmt.Sprintf("%s/parts=%d", shape, parts), func(b *testing.B) {
				dir := b.TempDir()
				files := make([]*os.File, 8)
				for i := range files {
					f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("seg%d", i)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() { _ = f.Close() })
					files[i] = f
				}
				if parts > 0 {
					zzWP23BusyCommitter(b, parts, committerOptions{burst: shape == "burst"})
					time.Sleep(1500 * time.Millisecond)
				}
				var next atomic.Int64
				took := make([][]time.Duration, len(files))
				var wg sync.WaitGroup
				start := time.Now()
				b.ResetTimer()
				for i, f := range files {
					wg.Go(func() {
						for next.Add(1) <= int64(b.N) {
							t0 := time.Now()
							if _, err := f.Write(batch); err != nil {
								b.Error(err)
								return
							}
							if err := syncfile.SyncData(f); err != nil {
								b.Error(err)
								return
							}
							took[i] = append(took[i], time.Since(t0))
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
}
