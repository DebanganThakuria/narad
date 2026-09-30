package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP6NewStore is newTestStore for benchmarks.
func zzWP6NewStore(tb testing.TB) *metastore.Store {
	tb.Helper()
	store, err := metastore.New(metastore.Config{NodeID: "node-self", DataDir: tb.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			return store
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatal("timed out waiting for leader")
	return nil
}

// zzWP6SeedSpread creates topic "orders" with the given partitions spread
// over three owners the way a 3-node cluster places them: partition p is
// owned by node-self when p%3 == 0 and by one of two remote members
// otherwise.
func zzWP6SeedSpread(tb testing.TB, store *metastore.Store, partitions int) {
	tb.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: partitions}); err != nil {
		tb.Fatal(err)
	}
	for _, m := range []metastore.Member{
		{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive},
		{ID: "node-r1", Addr: "r1.example:7942", Status: metastore.MemberAlive},
		{ID: "node-r2", Addr: "r2.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			tb.Fatal(err)
		}
	}
	owners := []string{"node-self", "node-r1", "node-r2"}
	for p := range partitions {
		if err := store.AssignPartition(ctx, "orders", p, owners[p%3]); err != nil {
			tb.Fatal(err)
		}
	}
}

// zzWP6Sink counts committed records and remembers when each WAL seq
// first committed. delay stands in for the owner's fsync.
type zzWP6Sink struct {
	delay     time.Duration
	addrDelay map[string]time.Duration
	committed atomic.Int64
	batches   atomic.Int64
	mu        sync.Mutex
	at        map[uint64]time.Time
	track     bool
}

func (s *zzWP6Sink) note(seqs func(func(uint64))) {
	if !s.track {
		return
	}
	now := time.Now()
	s.mu.Lock()
	seqs(func(seq uint64) {
		if _, ok := s.at[seq]; !ok {
			s.at[seq] = now
		}
	})
	s.mu.Unlock()
}

func (s *zzWP6Sink) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := s.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (s *zzWP6Sink) CommitAcceptedProduceBatch(_ context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.note(func(f func(uint64)) {
		for _, r := range recs {
			f(r.WAL.Seq)
		}
	})
	s.batches.Add(1)
	s.committed.Add(int64(len(recs)))
	return make([]int64, len(recs)), nil
}

func (s *zzWP6Sink) peer() fakePeerClient {
	return fakePeerClient{commitProduceBatchFn: func(ctx context.Context, addr string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		if d := s.addrDelay[addr]; d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return nodewire.Response{}, ctx.Err()
			}
		}
		s.batches.Add(1)
		s.committed.Add(int64(len(req.Records)))
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
}

// zzWP6Fill accepts n records spread round-robin over partitions from
// 32 goroutines so the WAL group-commits them.
func zzWP6Fill(tb testing.TB, m *ingress.Manager, n, partitions int) {
	tb.Helper()
	payload := bytes.Repeat([]byte("x"), 256)
	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < n; i += workers {
				if _, err := m.AcceptProduce(context.Background(), "orders", "k", i%partitions, payload); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		tb.Fatal(err)
	}
}

func zzWP6Manager(tb testing.TB) *ingress.Manager {
	tb.Helper()
	m, err := ingress.OpenManager(tb.TempDir(), wal.Options{SegmentBytes: 64 << 20})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = m.Close() })
	return m
}

// BenchmarkZZWP6DispatchPassLocal is the dispatcher's own cost: one
// DispatchAvailable over a 4096-record backlog on 36 local partitions
// with a committer that returns at once. It covers the scan, bucketing,
// the commit fan-out and the checkpoint store.
func BenchmarkZZWP6DispatchPassLocal(b *testing.B) {
	const n, partitions = 4096, 36
	store := zzWP6NewStore(b)
	seedProduceDispatchTopicPartitionsTB(b, store, "node-self", partitions)
	m := zzWP6Manager(b)
	sink := &zzWP6Sink{}
	d := NewProduceDispatcher(m, store, "node-self", sink, nil, nil, ProduceDispatcherConfig{})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		b.StopTimer()
		zzWP6Fill(b, m, n, partitions)
		b.StartTimer()
		for {
			processed, err := d.DispatchAvailable(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if processed == 0 {
				break
			}
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/record")
}

// BenchmarkZZWP6RunDrain measures how fast the Run loop drains a
// backlog spread over a 3-owner layout (12 local partitions, 24 remote
// on two owners) when every commit costs a couple of milliseconds, the
// shape of an fsync on the owner. Records/s is the dispatch throughput
// ceiling for that commit latency.
func BenchmarkZZWP6RunDrain(b *testing.B) {
	for _, tc := range []struct {
		name       string
		local      time.Duration
		r1, r2     time.Duration
		partitions int
	}{
		{"36p-2ms", 2 * time.Millisecond, 3 * time.Millisecond, 3 * time.Millisecond, 36},
		{"216p-2ms", 2 * time.Millisecond, 3 * time.Millisecond, 3 * time.Millisecond, 216},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const n = 16384
			var total time.Duration
			var commits int64
			b.StopTimer()
			for range b.N {
				store := zzWP6NewStore(b)
				zzWP6SeedSpread(b, store, tc.partitions)
				m := zzWP6Manager(b)
				zzWP6Fill(b, m, n, tc.partitions)
				sink := &zzWP6Sink{delay: tc.local, addrDelay: map[string]time.Duration{"r1.example:7942": tc.r1, "r2.example:7942": tc.r2}}
				d := NewProduceDispatcher(m, store, "node-self", sink, sink.peer(), nil, ProduceDispatcherConfig{})
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				start := time.Now()
				b.StartTimer()
				go func() { d.Run(ctx); close(done) }()
				for sink.committed.Load() < n {
					time.Sleep(200 * time.Microsecond)
				}
				b.StopTimer()
				total += time.Since(start)
				cancel()
				<-done
				commits += sink.batches.Load()
			}
			b.ReportMetric(float64(n)*float64(b.N)/total.Seconds(), "records/s")
			b.ReportMetric(float64(n)*float64(b.N)/float64(commits), "records/commit")
			b.ReportMetric(0, "ns/op")
		})
	}
}

// BenchmarkZZWP6RunSteadyLatency drives the Run loop at a steady rate
// and reports accept-to-commit latency for the records of the node's
// own (local) partitions, and how many records each commit carried (the
// owner pays an fsync per commit). "slow-remote" makes one remote owner
// take 50 ms per commit: a healthy local partition should not inherit
// it. "busy-216p" is a devstack-like layout under load.
func BenchmarkZZWP6RunSteadyLatency(b *testing.B) {
	for _, tc := range []struct {
		name       string
		r1, r2     time.Duration
		partitions int
		rate       int // records/s
	}{
		{"even", 3 * time.Millisecond, 3 * time.Millisecond, 36, 2000},
		{"slow-remote", 50 * time.Millisecond, 3 * time.Millisecond, 36, 2000},
		{"busy-216p", 3 * time.Millisecond, 3 * time.Millisecond, 216, 12000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			partitions, rate := tc.partitions, tc.rate
			const dur = 2 * time.Second
			var p50s, p99s []float64
			var records, commits int64
			for b.Loop() {
				store := zzWP6NewStore(b)
				zzWP6SeedSpread(b, store, partitions)
				m := zzWP6Manager(b)
				sink := &zzWP6Sink{delay: time.Millisecond, addrDelay: map[string]time.Duration{"r1.example:7942": tc.r1, "r2.example:7942": tc.r2}, track: true, at: map[uint64]time.Time{}}
				d := NewProduceDispatcher(m, store, "node-self", sink, sink.peer(), nil, ProduceDispatcherConfig{})
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { d.Run(ctx); close(done) }()
				type acc struct {
					seq uint64
					at  time.Time
				}
				var mu sync.Mutex
				var local []acc
				var wg sync.WaitGroup
				tick := time.NewTicker(time.Second / time.Duration(rate))
				payload := bytes.Repeat([]byte("x"), 256)
				stop := time.Now().Add(dur)
				i := 0
				for time.Now().Before(stop) {
					<-tick.C
					p := i % partitions
					i++
					wg.Add(1)
					go func() {
						defer wg.Done()
						res, err := m.AcceptProduce(context.Background(), "orders", "k", p, payload)
						if err != nil {
							b.Error(err)
							return
						}
						if p%3 == 0 {
							mu.Lock()
							local = append(local, acc{seq: res.WAL.Seq, at: time.Now()})
							mu.Unlock()
						}
					}()
				}
				tick.Stop()
				wg.Wait()
				deadline := time.Now().Add(10 * time.Second)
				var lats []float64
				for _, a := range local {
					for {
						sink.mu.Lock()
						t, ok := sink.at[a.seq]
						sink.mu.Unlock()
						if ok {
							lats = append(lats, float64(t.Sub(a.at).Nanoseconds()))
							break
						}
						if time.Now().After(deadline) {
							b.Fatalf("seq %d never committed", a.seq)
						}
						time.Sleep(time.Millisecond)
					}
				}
				cancel()
				<-done
				records += sink.committed.Load()
				commits += sink.batches.Load()
				slices.Sort(lats)
				p50s = append(p50s, lats[len(lats)/2])
				p99s = append(p99s, lats[len(lats)*99/100])
			}
			avg := func(v []float64) float64 {
				var s float64
				for _, x := range v {
					s += x
				}
				return s / float64(len(v))
			}
			b.ReportMetric(avg(p50s), "p50-ns")
			b.ReportMetric(avg(p99s), "p99-ns")
			b.ReportMetric(float64(records)/float64(commits), "records/commit")
		})
	}
}

// seedProduceDispatchTopicPartitionsTB is seedProduceDispatchTopicPartitions
// for benchmarks.
func seedProduceDispatchTopicPartitionsTB(tb testing.TB, store *metastore.Store, ownerID string, partitions int) {
	tb.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: partitions}); err != nil {
		tb.Fatal(err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		tb.Fatal(err)
	}
	for p := range partitions {
		if err := store.AssignPartition(ctx, "orders", p, ownerID); err != nil {
			tb.Fatal(fmt.Errorf("assign %d: %w", p, err))
		}
	}
}
