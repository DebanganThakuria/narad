package cluster

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP6Clock is a settable clock safe to read from commit goroutines.
type zzWP6Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *zzWP6Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *zzWP6Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// zzWP6GatedCommitter reports each batch's size on calls and holds it
// until the test releases it.
type zzWP6GatedCommitter struct {
	calls   chan int
	release chan struct{}
}

func newZZWP6GatedCommitter() *zzWP6GatedCommitter {
	return &zzWP6GatedCommitter{calls: make(chan int, 16), release: make(chan struct{})}
}

func (c *zzWP6GatedCommitter) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := c.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (c *zzWP6GatedCommitter) CommitAcceptedProduceBatch(ctx context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	c.calls <- len(recs)
	select {
	case <-c.release:
		return make([]int64, len(recs)), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *zzWP6GatedCommitter) peer() fakePeerClient {
	return fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		c.calls <- len(req.Records)
		select {
		case <-c.release:
			return nodewire.Response{Status: http.StatusOK}, nil
		case <-ctx.Done():
			return nodewire.Response{}, ctx.Err()
		}
	}}
}

// started waits for the next batch to reach c and returns its size.
func (c *zzWP6GatedCommitter) started(t *testing.T) int {
	t.Helper()
	select {
	case n := <-c.calls:
		return n
	case <-time.After(5 * time.Second):
		t.Fatal("no commit reached the committer")
		return 0
	}
}

// zzWP6Land releases the commit c holds and merges its result, as the
// Run loop would.
func zzWP6Land(t *testing.T, d *ProduceDispatcher, c *zzWP6GatedCommitter) {
	t.Helper()
	c.release <- struct{}{}
	select {
	case res := <-d.results:
		if res.err != nil {
			t.Fatalf("commit failed: %v", res.err)
		}
		d.finish(context.Background(), d.state, res)
	case <-time.After(5 * time.Second):
		t.Fatal("the released commit never reported back")
	}
}

func zzWP6Accept(t *testing.T, m *ingress.Manager, partition, n int) {
	t.Helper()
	for range n {
		if _, err := m.AcceptProduce(context.Background(), "orders", "k", partition, []byte(`{"b":1}`)); err != nil {
			t.Fatal(err)
		}
	}
}

// produce-dispatch review, batch floor: every destination used to commit
// as soon as it held one record and a fan-out slot was free, and the
// loop wakes on every WAL group commit, so under load each group commit
// went out as one small commit (one fsync) per partition. While other
// commits are in flight, a destination below the batch floor now
// lingers for up to twice its owner's recent commit latency. An idle
// dispatcher still commits at once, a full batch goes at once, and one
// slow owner does not lengthen the linger of another owner's
// partitions.
func TestZZWP6SmallBatchLingersWhileOtherCommitsRun(t *testing.T) {
	store := newTestStore(t)
	zzWP6SeedTwoOwners(t, store, 1, 1) // p0 here, p1 on node-remote
	m := newDispatchIngressManagerLargeSegments(t)
	local, remote := newZZWP6GatedCommitter(), newZZWP6GatedCommitter()
	d := NewProduceDispatcher(m, store, "node-self", local, remote.peer(), nil, ProduceDispatcherConfig{})
	clock := &zzWP6Clock{now: time.Unix(1_700_000_000, 0)}
	d.now = clock.Now
	ctx := context.Background()
	if err := d.loadCursor(); err != nil {
		t.Fatal(err)
	}
	st := d.state
	p0 := dispatchDestKey{topic: "orders", partition: 0}
	inflight := func(key dispatchDestKey) bool {
		dest, ok := st.dests[key]
		return ok && dest.inflight
	}

	// Warm up: the remote owner's commits take 40 ms, this node's 2 ms.
	zzWP6Accept(t, m, 1, 1)
	d.step(ctx, st)
	if n := remote.started(t); n != 1 {
		t.Fatalf("remote warm-up batch = %d, want 1", n)
	}
	clock.Advance(40 * time.Millisecond)
	zzWP6Land(t, d, remote)
	zzWP6Accept(t, m, 0, 1)
	d.step(ctx, st)
	if !inflight(p0) {
		t.Fatal("an idle dispatcher held back a one-record commit, want it sent at once")
	}
	if n := local.started(t); n != 1 {
		t.Fatalf("local warm-up batch = %d, want 1", n)
	}
	clock.Advance(2 * time.Millisecond)
	zzWP6Land(t, d, local)

	// A remote commit is in flight; three records for p0 wait for more
	// rather than going out as a three-record commit.
	zzWP6Accept(t, m, 1, 1)
	d.step(ctx, st)
	if n := remote.started(t); n != 1 {
		t.Fatalf("remote batch = %d, want 1", n)
	}
	zzWP6Accept(t, m, 0, 3)
	d.step(ctx, st)
	if inflight(p0) {
		t.Fatal("p0 committed a 3-record batch at once while another commit was in flight, want it to linger for more records")
	}
	if wake := d.nextWake(st); wake != 4*time.Millisecond {
		t.Fatalf("nextWake = %v while p0 lingers, want 4ms (twice this node's 2ms commit latency)", wake)
	}

	// The linger follows this node's commit latency (2 ms), not the slow
	// remote owner's (40 ms): p0 goes after 4 ms.
	clock.Advance(3 * time.Millisecond)
	d.step(ctx, st)
	if inflight(p0) {
		t.Fatal("p0 committed before its linger ran out")
	}
	clock.Advance(2 * time.Millisecond)
	d.step(ctx, st)
	if !inflight(p0) {
		t.Fatal("p0 still lingering 5ms in, want its commit sent after 4ms (twice this node's commit latency, not the remote owner's)")
	}
	if n := local.started(t); n != 3 {
		t.Fatalf("p0 batch = %d, want the 3 lingering records", n)
	}
	zzWP6Land(t, d, local)

	// A full batch does not wait.
	zzWP6Accept(t, m, 0, produceDispatchTargetPerPartition)
	d.step(ctx, st)
	if !inflight(p0) {
		t.Fatalf("p0 held back a %d-record batch, want a full batch sent at once", produceDispatchTargetPerPartition)
	}
	if n := local.started(t); n != produceDispatchTargetPerPartition {
		t.Fatalf("p0 batch = %d, want %d", n, produceDispatchTargetPerPartition)
	}
	zzWP6Land(t, d, local)
	zzWP6Land(t, d, remote)

	// Idle again: a single record goes at once.
	zzWP6Accept(t, m, 0, 1)
	d.step(ctx, st)
	if !inflight(p0) {
		t.Fatal("an idle dispatcher held back a one-record commit, want it sent at once")
	}
	if n := local.started(t); n != 1 {
		t.Fatalf("p0 batch = %d, want 1", n)
	}
	zzWP6Land(t, d, local)
}
