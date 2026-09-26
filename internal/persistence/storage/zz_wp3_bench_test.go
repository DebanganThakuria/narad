package storage

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Benchmarks for the commit and read paths. The *CPU variants make every
// fsync report success without the syscall, so they measure CPU and
// page-cache I/O rather than the device (macOS F_FULLFSYNC dominates
// otherwise); the *Fsync variants keep the real syncs.

// wp3NoFsync turns every data and directory sync into a no-op for the
// rest of the benchmark or test.
func wp3NoFsync(tb testing.TB) {
	restore := syncfile.SetFaultHook(func(op syncfile.Op, _ string) error {
		if op == syncfile.OpSyncData || op == syncfile.OpSync {
			return syncfile.ErrLie
		}
		return nil
	})
	tb.Cleanup(restore)
}

func wp3Records(n, size int) [][]byte {
	recs := make([][]byte, n)
	for i := range recs {
		r := make([]byte, size)
		for j := range r {
			r[j] = byte(i*31 + j)
		}
		recs[i] = r
	}
	return recs
}

// wp3Payload is a 300-byte JSON-ish produce payload, wrapped in the keyed
// envelope exactly as the commit path stores it.
var wp3Payload = func() []byte {
	out := []byte(`{"order_id":"ord_00000001","merchant":"m_12345","amount":100,"currency":"INR","status":"captured","method":"card","notes":{"a":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","c":"dddddddddddddddddddddddddddddd"},"created_at":1790424109845}`)
	for len(out) < 300 {
		out = append(out, ' ')
	}
	return out
}()

func wp3KeyedBatch(n, seq int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		p := append([]byte(nil), wp3Payload...)
		p[20] = byte('0' + (seq+i)%10)
		out[i] = EncodeKeyedRecord("", 1790424109845, p)
	}
	return out
}

func wp3CommitBatch(tb testing.TB, l *Log, recs [][]byte) (int64, int64) {
	first, last, err := l.AppendBatchOwned(recs)
	if err != nil {
		tb.Fatal(err)
	}
	if err := l.CommitDurable(first, last); err != nil {
		tb.Fatal(err)
	}
	return first, last
}

func wp3FilledLog(tb testing.TB, opts Options, frames, perFrame, size int) *Log {
	l, err := NewLog(filepath.Join(tb.TempDir(), "p0"), opts)
	if err != nil {
		tb.Fatal(err)
	}
	for range frames {
		wp3CommitBatch(tb, l, wp3Records(perFrame, size))
	}
	return l
}

// wp3Metrics records durations and counts of the storage metrics.
type wp3Metrics struct {
	fsyncN, hwmN, hwmNs atomic.Int64
}

func (m *wp3Metrics) ObserveFlush(time.Duration, int64) {}
func (m *wp3Metrics) ObserveFsync(time.Duration)        { m.fsyncN.Add(1) }
func (m *wp3Metrics) ObserveHighWatermarkPersist(d time.Duration, _ string) {
	m.hwmN.Add(1)
	m.hwmNs.Add(int64(d))
}
func (m *wp3Metrics) IncRetentionDeletion(string, int64, int64) {}
func (m *wp3Metrics) ObserveRetentionRun(time.Duration)         {}

// BenchmarkWP3CommitFsync is one producer committing keyed batches with
// the real segment and high-watermark syncs: the per-commit disk time.
func BenchmarkWP3CommitFsync(b *testing.B) {
	for _, n := range []int{24, 72} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			m := &wp3Metrics{}
			opts := DefaultOptions()
			opts.Metrics = m
			l, err := NewLog(filepath.Join(b.TempDir(), "p0"), opts)
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			wp3CommitBatch(b, l, wp3KeyedBatch(n, 0)) // creates the hwm file
			m.fsyncN.Store(0)
			m.hwmN.Store(0)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wp3CommitBatch(b, l, wp3KeyedBatch(n, i))
			}
			b.StopTimer()
			b.ReportMetric(float64(m.hwmN.Load())/float64(b.N), "hwmPersists/commit")
			b.ReportMetric(float64(m.fsyncN.Load())/float64(b.N), "segFsyncs/commit")
		})
	}
}

// BenchmarkWP3CommitFsyncSerialized is three committers sharing one
// partition behind a produce lock, as the ingress dispatchers of three
// nodes do.
func BenchmarkWP3CommitFsyncSerialized(b *testing.B) {
	const n = 24
	l, err := NewLog(filepath.Join(b.TempDir(), "p0"), DefaultOptions())
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	wp3CommitBatch(b, l, wp3KeyedBatch(n, 0))
	var mu sync.Mutex
	var wg sync.WaitGroup
	per := (b.N + 2) / 3
	b.ResetTimer()
	for g := range 3 {
		wg.Go(func() {
			for i := range per {
				batch := wp3KeyedBatch(n, g+i)
				mu.Lock()
				first, last, err := l.AppendBatchOwned(batch)
				if err == nil {
					err = l.CommitDurable(first, last)
				}
				mu.Unlock()
				if err != nil {
					b.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(per*3*n)/b.Elapsed().Seconds(), "rec/s")
}

// BenchmarkWP3CommitCPU is the commit path without device syncs, with
// the commit read-back on (the default). hdrReads/commit counts frame
// header preads the read-back's index lookup performs.
func BenchmarkWP3CommitCPU(b *testing.B) {
	for _, sz := range []struct{ n, size int }{{7, 190}, {33, 250}} {
		b.Run(fmt.Sprintf("%dx%d", sz.n, sz.size), func(b *testing.B) {
			wp3NoFsync(b)
			l, err := NewLog(filepath.Join(b.TempDir(), "p0"), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			var hdr atomic.Int64
			frameHeaderReadHook = func() { hdr.Add(1) }
			defer func() { frameHeaderReadHook = nil }()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				wp3CommitBatch(b, l, wp3Records(sz.n, sz.size))
			}
			b.StopTimer()
			b.ReportMetric(float64(hdr.Load())/float64(b.N), "hdrReads/commit")
		})
	}
}

// BenchmarkWP3CommitThenRead commits a batch and then reads every record
// of it, the way a consumer at the frontier does.
func BenchmarkWP3CommitThenRead(b *testing.B) {
	for _, sz := range []struct{ n, size int }{{7, 190}, {33, 250}} {
		b.Run(fmt.Sprintf("%dx%d", sz.n, sz.size), func(b *testing.B) {
			wp3NoFsync(b)
			l, err := NewLog(filepath.Join(b.TempDir(), "p0"), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				first, last := wp3CommitBatch(b, l, wp3Records(sz.n, sz.size))
				for off := first; off <= last; off++ {
					if _, err := l.ReadShared(off); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkWP3ReadMiss reads a different frame on every call through a
// one-frame cache: every read decodes a frame from the file.
func BenchmarkWP3ReadMiss(b *testing.B) {
	zc, err := codec.NewZstdCodec(zstd.SpeedFastest)
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		n, size     int
		write, read codec.Codec
	}{
		{"none/7x190", 7, 190, nil, nil},
		{"none/33x250", 33, 250, nil, nil},
		{"zstd/7x190", 7, 190, zc, zc},
		{"zstd-by-noop-log/7x190", 7, 190, zc, nil},
	} {
		b.Run(tc.name, func(b *testing.B) {
			wp3NoFsync(b)
			const frames = 1024
			dir := filepath.Join(b.TempDir(), "p0")
			w, err := NewLog(dir, Options{Codec: tc.write})
			if err != nil {
				b.Fatal(err)
			}
			for range frames {
				wp3CommitBatch(b, w, wp3Records(tc.n, tc.size))
			}
			if err := w.Close(); err != nil {
				b.Fatal(err)
			}
			l, err := NewLog(dir, Options{Codec: tc.read})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			l.frameCache = newFrameCache(1, maxDecodeCacheBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := l.ReadShared(int64(i%frames) * int64(tc.n)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWP3ReadHit is a read served by the frame cache.
func BenchmarkWP3ReadHit(b *testing.B) {
	wp3NoFsync(b)
	l := wp3FilledLog(b, Options{}, 1, 7, 190)
	defer l.Close()
	if _, err := l.ReadShared(0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := l.ReadShared(int64(i % 7)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWP3ReadHitParallel is cached reads from every P, optionally
// while a producer commits on the same log.
func BenchmarkWP3ReadHitParallel(b *testing.B) {
	for _, producer := range []bool{false, true} {
		b.Run(fmt.Sprintf("producer=%v", producer), func(b *testing.B) {
			wp3NoFsync(b)
			l := wp3FilledLog(b, Options{}, 1, 7, 190)
			defer l.Close()
			if _, err := l.ReadShared(0); err != nil {
				b.Fatal(err)
			}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			if producer {
				wg.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						first, last, err := l.AppendBatchOwned(wp3Records(7, 190))
						if err != nil {
							return
						}
						_ = l.CommitDurable(first, last)
					}
				})
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if _, err := l.ReadShared(int64(i % 7)); err != nil {
						b.Error(err)
						return
					}
					i++
				}
			})
			b.StopTimer()
			close(stop)
			wg.Wait()
		})
	}
}

func wp3FullNav() *navCache {
	c := newNavCache(maxNavCacheEntries)
	for i := range maxNavCacheEntries {
		c.put(indexEntry{segmentBaseOffset: 0, baseOffset: int64(i * 7), recordCount: 7, framePos: int64(i * 1400), frameLen: 1400})
	}
	return c
}

// BenchmarkWP3Nav measures navCache.bestAnchor on a full cache: a read
// in the newest frame (front hit), alternating reads of the two newest
// frames (every call misses the front), and the first offset past the
// newest frame (the commit read-back's lookup before it is cached).
func BenchmarkWP3Nav(b *testing.B) {
	b.Run("front", func(b *testing.B) {
		c := wp3FullNav()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := c.bestAnchor(0, 255*7+int64(i%7)); !ok {
				b.Fatal("miss")
			}
		}
	})
	b.Run("pingpong", func(b *testing.B) {
		c := wp3FullNav()
		offs := [2]int64{254 * 7, 255 * 7}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := c.bestAnchor(0, offs[i&1]); !ok {
				b.Fatal("miss")
			}
		}
	})
	b.Run("pastnewest", func(b *testing.B) {
		c := wp3FullNav()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := c.bestAnchor(0, 256*7); !ok {
				b.Fatal("miss")
			}
		}
	})
}

// BenchmarkWP3FrontierSim is one committer and eight consumers taking
// offsets in order below the high-watermark, as queue consumers at the
// frontier do. ns/op is per commit.
func BenchmarkWP3FrontierSim(b *testing.B) {
	wp3NoFsync(b)
	l, err := NewLog(filepath.Join(b.TempDir(), "p0"), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	var next, consumed atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				off := next.Load()
				if off >= l.HighWatermark() {
					runtime.Gosched()
					continue
				}
				if !next.CompareAndSwap(off, off+1) {
					continue
				}
				if _, err := l.ReadShared(off); err != nil {
					b.Error(err)
					return
				}
				consumed.Add(1)
			}
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wp3CommitBatch(b, l, wp3Records(7, 190))
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
	b.ReportMetric(float64(consumed.Load())/float64(b.N), "reads/commit")
}

// BenchmarkWP3LogLifecycle is one partition reopened, committed to once
// and closed, with the real syncs: what a partition's life costs on top
// of its commits (an idle eviction and the next produce, or a restart).
func BenchmarkWP3LogLifecycle(b *testing.B) {
	m := &wp3Metrics{}
	opts := DefaultOptions()
	opts.Metrics = m
	dir := filepath.Join(b.TempDir(), "p0")
	l, err := NewLog(dir, opts)
	if err != nil {
		b.Fatal(err)
	}
	wp3CommitBatch(b, l, wp3KeyedBatch(24, 0))
	if err := l.Close(); err != nil {
		b.Fatal(err)
	}
	m.fsyncN.Store(0)
	m.hwmN.Store(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := NewLog(dir, opts)
		if err != nil {
			b.Fatal(err)
		}
		wp3CommitBatch(b, l, wp3KeyedBatch(24, i))
		if err := l.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(m.hwmN.Load())/float64(b.N), "hwmPersists/cycle")
	b.ReportMetric(float64(m.fsyncN.Load())/float64(b.N), "segFsyncs/cycle")
}

// BenchmarkWP3CommitRoundTrip is the commit path with no disk I/O at
// all: segment writes and every sync report success without the
// syscall, and the read-back is off. What is left is the CPU of a
// commit round trip through the flusher goroutine (drain, encode,
// index, advance, wake), which a loaded machine's device stalls would
// otherwise drown.
func BenchmarkWP3CommitRoundTrip(b *testing.B) {
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		switch {
		case op == syncfile.OpSyncData || op == syncfile.OpSync:
			return syncfile.ErrLie
		case op == syncfile.OpWrite && strings.HasSuffix(path, segmentFileSuffix):
			return syncfile.ErrLie
		}
		return nil
	})
	b.Cleanup(restore)
	opts := Options{DisableCommitVerify: true}
	l, err := NewLog(filepath.Join(b.TempDir(), "p0"), opts)
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	recs := wp3Records(7, 190)
	wp3CommitBatch(b, l, recs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		first, last, err := l.AppendBatch(recs)
		if err != nil {
			b.Fatal(err)
		}
		if err := l.CommitDurable(first, last); err != nil {
			b.Fatal(err)
		}
	}
}
