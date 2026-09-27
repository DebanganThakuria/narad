package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// zzWP6Backlog accepts n records spread round-robin over parts from 32
// goroutines, so the WAL group-commits them.
func zzWP6Backlog(t *testing.T, m *ingress.Manager, n int, parts ...int) {
	t.Helper()
	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Go(func() {
			for i := w; i < n; i += workers {
				if _, err := m.AcceptProduce(context.Background(), "orders", "k", parts[i%len(parts)], []byte(`{"b":1}`)); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// produce-dispatch review, per-destination cap: one destination could
// queue only a quarter of the window, 1024 records at the 4096-record
// window, so a latency-bound destination (a hot key, a one-partition
// topic, a backlog draining to a remote owner) committed a quarter of
// what the pass-based dispatcher sent per round trip. A single hot
// destination must carry a whole base window per commit.
func TestZZWP6HotDestinationCommitsABaseWindow(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 1)
	m := newDispatchIngressManagerLargeSegments(t)
	const total = 3*produceDispatchBaseWindow + 100
	zzWP6Backlog(t, m, total, 0)
	c := &fakeProduceCommitter{}
	d := NewProduceDispatcher(m, store, "node-self", c, nil, nil, ProduceDispatcherConfig{})
	for range 20 {
		if _, err := d.DispatchAvailable(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(c.committed()) == total {
			break
		}
	}
	if got := len(c.committed()); got != total {
		t.Fatalf("committed %d of %d", got, total)
	}
	batches := c.batchCalls()
	for i, n := range batches[:len(batches)-1] {
		if n < produceDispatchBaseWindow {
			t.Fatalf("batch %d of %v carried %d records, want a whole base window (%d) per commit while a backlog remains",
				i, batches, n, produceDispatchBaseWindow)
		}
	}
}

// Three hot destinations at the 4096-record window: records in flight
// used to count against the window with the queues, so each got about a
// third of it per commit, below what the pass-based dispatcher gave them
// together. Each must be able to carry a base window.
func TestZZWP6ThreeHotDestinationsEachCommitABaseWindow(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 3)
	m := newDispatchIngressManagerLargeSegments(t)
	zzWP6Backlog(t, m, 3*(produceDispatchBaseWindow+64), 0, 1, 2)
	c := &perPartitionBatchCommitter{}
	d := NewProduceDispatcher(m, store, "node-self", c, nil, nil, ProduceDispatcherConfig{})
	if _, err := d.DispatchAvailable(context.Background()); err != nil {
		t.Fatal(err)
	}
	for p := range 3 {
		first := c.first(p)
		if first < produceDispatchBaseWindow {
			t.Fatalf("partition %d's first commit carried %d records, want a whole base window (%d)", p, first, produceDispatchBaseWindow)
		}
	}
}

// perPartitionBatchCommitter records the size of each partition's
// batches.
type perPartitionBatchCommitter struct {
	mu      sync.Mutex
	batches map[int][]int
}

func (c *perPartitionBatchCommitter) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := c.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (c *perPartitionBatchCommitter) CommitAcceptedProduceBatch(_ context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.batches == nil {
		c.batches = map[int][]int{}
	}
	p := recs[0].TargetPartition
	c.batches[p] = append(c.batches[p], len(recs))
	return make([]int64, len(recs)), nil
}

func (c *perPartitionBatchCommitter) first(p int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.batches[p]) == 0 {
		return 0
	}
	return c.batches[p][0]
}

// zzWP6BlockingPartitionCommitter holds every commit to partition 0
// until its context ends and records when each other record committed.
type zzWP6BlockingPartitionCommitter struct {
	zzWP6TimedCommitter
}

func (c *zzWP6BlockingPartitionCommitter) CommitAcceptedProduceBatch(ctx context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	if recs[0].TargetPartition == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.zzWP6TimedCommitter.CommitAcceptedProduceBatch(ctx, recs)
}

func (c *zzWP6BlockingPartitionCommitter) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := c.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

// A hot destination whose commit has not landed yet may hold a whole
// base window in flight and another queued, but must leave room in the
// window: the other partitions' records keep being read and committed
// while it waits, well before the commit counts as slow.
func TestZZWP6HotDestinationLeavesRoomForOthers(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 2)
	m := newDispatchIngressManagerLargeSegments(t)
	zzWP6Backlog(t, m, 3*produceDispatchBaseWindow, 0)
	c := &zzWP6BlockingPartitionCommitter{}
	d := NewProduceDispatcher(m, store, "node-self", c, nil, nil, ProduceDispatcherConfig{})
	zzWP6StartRun(t, d)
	res, err := m.AcceptProduce(context.Background(), "orders", "k", 1, []byte(`{"other":1}`))
	if err != nil {
		t.Fatal(err)
	}
	accepted := time.Now()
	for {
		if _, ok := c.when(res.WAL.Seq); ok {
			return
		}
		if time.Since(accepted) > produceDispatchSlowAfter/2 {
			t.Fatalf("partition 1's record not committed %v after its accept while partition 0's commit is pending: the hot destination filled the window",
				produceDispatchSlowAfter/2)
		}
		time.Sleep(time.Millisecond)
	}
}
