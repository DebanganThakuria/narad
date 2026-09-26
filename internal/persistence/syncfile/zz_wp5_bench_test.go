package syncfile

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

var zzWP5Sink atomic.Pointer[[]byte]

// BenchmarkZZWP5AllocDuringSyncData measures allocation throughput on
// the benchmark goroutine while 8 goroutines loop a 4 KiB write plus
// SyncData, the shape of a broker under produce load: every durable
// step is an fsync in flight while request goroutines allocate. A sync
// that holds its P outside syscall state stretches every GC stop-the-
// world to the longest sync in flight, which shows up here as ns/op.
// syncs/s is reported so a faster allocator bought with fewer syncs
// would be visible.
func BenchmarkZZWP5AllocDuringSyncData(b *testing.B) {
	dir := b.TempDir()
	const writers = 8
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var syncs atomic.Int64
	for i := range writers {
		f, err := os.Create(filepath.Join(dir, "w"+string(rune('a'+i))))
		if err != nil {
			b.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer f.Close()
			buf := make([]byte, 4096)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := f.Write(buf); err != nil {
					b.Error(err)
					return
				}
				if err := SyncData(f); err != nil {
					b.Error(err)
					return
				}
				syncs.Add(1)
			}
		}()
	}
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	syncs.Store(0)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for range 64 {
			p := make([]byte, 1024)
			zzWP5Sink.Store(&p)
		}
	}
	b.StopTimer()
	elapsed := b.Elapsed()
	n := syncs.Load()
	close(stop)
	wg.Wait()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	b.ReportMetric(float64(n)/elapsed.Seconds(), "syncs/s")
	if gcs := after.NumGC - before.NumGC; gcs > 0 {
		b.ReportMetric(float64(after.PauseTotalNs-before.PauseTotalNs)/float64(gcs)/1e3, "us-pause/gc")
	}
}
