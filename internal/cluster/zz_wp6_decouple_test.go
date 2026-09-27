package cluster

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP6TimedCommitter is a local committer that records when each WAL
// seq first committed.
type zzWP6TimedCommitter struct {
	mu sync.Mutex
	at map[uint64]time.Time
}

func (c *zzWP6TimedCommitter) CommitAcceptedProduce(ctx context.Context, r ingress.ProduceRecord) (int64, error) {
	_, err := c.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{r})
	return 0, err
}

func (c *zzWP6TimedCommitter) CommitAcceptedProduceBatch(_ context.Context, recs []ingress.ProduceRecord) ([]int64, error) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.at == nil {
		c.at = map[uint64]time.Time{}
	}
	for _, r := range recs {
		if _, ok := c.at[r.WAL.Seq]; !ok {
			c.at[r.WAL.Seq] = now
		}
	}
	return make([]int64, len(recs)), nil
}

func (c *zzWP6TimedCommitter) when(seq uint64) (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.at[seq]
	return at, ok
}

// zzWP6SeedTwoOwners creates "orders" with partitions 0..local-1 owned
// here and local..local+remote-1 owned by node-remote.
func zzWP6SeedTwoOwners(t *testing.T, store *metastore.Store, local, remote int) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: local + remote}); err != nil {
		t.Fatal(err)
	}
	for _, m := range []metastore.Member{
		{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive},
		{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	for p := range local + remote {
		owner := "node-self"
		if p >= local {
			owner = "node-remote"
		}
		if err := store.AssignPartition(ctx, "orders", p, owner); err != nil {
			t.Fatal(err)
		}
	}
}

// zzWP6LocalLatencies accepts n records for local partition 0, one every
// gap, and returns each one's accept-to-commit latency, failing if any
// takes longer than limit.
func zzWP6LocalLatencies(t *testing.T, m *ingress.Manager, c *zzWP6TimedCommitter, n int, gap, limit time.Duration) []time.Duration {
	t.Helper()
	var lats []time.Duration
	for i := range n {
		time.Sleep(gap)
		res, err := m.AcceptProduce(context.Background(), "orders", "k", 0, fmt.Appendf(nil, `{"l":%d}`, i))
		if err != nil {
			t.Fatal(err)
		}
		accepted := time.Now()
		deadline := accepted.Add(limit)
		for {
			if at, ok := c.when(res.WAL.Seq); ok {
				lats = append(lats, at.Sub(accepted))
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("local record %d (seq %d) not committed %v after its accept: a remote owner is holding up the node's own partition", i, res.WAL.Seq, limit)
			}
			time.Sleep(time.Millisecond)
		}
	}
	return lats
}

func zzWP6Mean(lats []time.Duration) time.Duration {
	var sum time.Duration
	for _, l := range lats {
		sum += l
	}
	return sum / time.Duration(len(lats))
}

func zzWP6StartRun(t *testing.T, d *ProduceDispatcher) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// produce-dispatch-commit#0: a remote owner that never answers (a frozen
// process, a black-holed link, a wedged commit handler) used to hold the
// whole dispatch pass for the 30 s commit deadline, so this node's own
// healthy partition saw nothing new for 30 s. Its records must keep
// committing while the remote commit hangs.
func TestZZWP6HungOwnerDoesNotStallLocalPartitions(t *testing.T) {
	store := newTestStore(t)
	zzWP6SeedTwoOwners(t, store, 1, 1)
	m := newDispatchIngressManagerLargeSegments(t)
	c := &zzWP6TimedCommitter{}
	var remoteCalls atomic.Int32
	peer := fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		remoteCalls.Add(1)
		<-ctx.Done()
		return nodewire.Response{}, ctx.Err()
	}}
	if _, err := m.AcceptProduce(context.Background(), "orders", "k", 1, []byte(`{"r":1}`)); err != nil {
		t.Fatal(err)
	}
	d := NewProduceDispatcher(m, store, "node-self", c, peer, nil, ProduceDispatcherConfig{})
	zzWP6StartRun(t, d)
	deadline := time.Now().Add(5 * time.Second)
	for remoteCalls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the remote commit never started")
		}
		time.Sleep(time.Millisecond)
	}
	lats := zzWP6LocalLatencies(t, m, c, 20, 20*time.Millisecond, 2*time.Second)
	if mean := zzWP6Mean(lats); mean > 200*time.Millisecond {
		t.Fatalf("local accept-to-commit mean %v while a remote commit hangs, want it unaffected", mean)
	}
}

// Many hung destinations on one owner must not take every commit slot
// for the length of the RPC deadline: a commit that runs past the slow
// threshold stops counting against the fan-out.
func TestZZWP6HungOwnerCannotHoldEveryCommitSlot(t *testing.T) {
	store := newTestStore(t)
	const remote = 3 * defaultProduceDispatchCommitFanout
	zzWP6SeedTwoOwners(t, store, 1, remote)
	m := newDispatchIngressManagerLargeSegments(t)
	c := &zzWP6TimedCommitter{}
	peer := fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		<-ctx.Done()
		return nodewire.Response{}, ctx.Err()
	}}
	for p := 1; p <= remote; p++ {
		if _, err := m.AcceptProduce(context.Background(), "orders", "k", p, []byte(`{"r":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	d := NewProduceDispatcher(m, store, "node-self", c, peer, nil, ProduceDispatcherConfig{})
	zzWP6StartRun(t, d)
	zzWP6LocalLatencies(t, m, c, 5, 50*time.Millisecond, 4*time.Second)
}

// A remote owner that is merely slow (every commit takes 3 s and
// succeeds) must not set the pace for this node's own partition either.
func TestZZWP6SlowOwnerDoesNotSlowLocalPartitions(t *testing.T) {
	store := newTestStore(t)
	zzWP6SeedTwoOwners(t, store, 1, 1)
	m := newDispatchIngressManagerLargeSegments(t)
	c := &zzWP6TimedCommitter{}
	peer := fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		select {
		case <-time.After(3 * time.Second):
			return nodewire.Response{Status: http.StatusOK}, nil
		case <-ctx.Done():
			return nodewire.Response{}, ctx.Err()
		}
	}}
	d := NewProduceDispatcher(m, store, "node-self", c, peer, nil, ProduceDispatcherConfig{})
	zzWP6StartRun(t, d)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
			}
			_, _ = m.AcceptProduce(context.Background(), "orders", "k", 1, []byte(`{"r":1}`))
		}
	}()
	lats := zzWP6LocalLatencies(t, m, c, 25, 60*time.Millisecond, 5*time.Second)
	close(stop)
	wg.Wait()
	if mean := zzWP6Mean(lats); mean > 200*time.Millisecond {
		t.Fatalf("local accept-to-commit mean %v next to a 3 s remote owner, want it unaffected", mean)
	}
}

// produce-dispatch-commit#2: one destination that keeps failing fast
// (an owner restarting, a handoff freeze) used to put the whole
// dispatcher on a one-second backoff after every pass, so every other
// partition on the node waited up to a second too.
func TestZZWP6FailingOwnerDoesNotBackOffOtherPartitions(t *testing.T) {
	store := newTestStore(t)
	zzWP6SeedTwoOwners(t, store, 1, 1)
	m := newDispatchIngressManagerLargeSegments(t)
	c := &zzWP6TimedCommitter{}
	var remoteCalls atomic.Int32
	peer := fakePeerClient{commitProduceBatchFn: func(context.Context, string, nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		remoteCalls.Add(1)
		return nodewire.Response{Status: http.StatusConflict}, nil
	}}
	if _, err := m.AcceptProduce(context.Background(), "orders", "k", 1, []byte(`{"r":1}`)); err != nil {
		t.Fatal(err)
	}
	d := NewProduceDispatcher(m, store, "node-self", c, peer, nil, ProduceDispatcherConfig{})
	zzWP6StartRun(t, d)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
			_, _ = m.AcceptProduce(context.Background(), "orders", "k", 1, []byte(`{"r":1}`))
		}
	}()
	lats := zzWP6LocalLatencies(t, m, c, 30, 50*time.Millisecond, 2*time.Second)
	close(stop)
	wg.Wait()
	if mean := zzWP6Mean(lats); mean > 100*time.Millisecond {
		t.Fatalf("local accept-to-commit mean %v while another destination fails, want no backoff", mean)
	}
	// The failing destination is retried about once a second, not at
	// the pass rate.
	if calls := remoteCalls.Load(); calls > 12 {
		t.Fatalf("failing destination retried %d times in ~2.5s, want about once a second", calls)
	}
}

// A freshly created topic whose partitions have no owner yet cannot
// dispatch; that must not slow the node's other topics down either.
func TestZZWP6UnassignedTopicDoesNotBackOffOthers(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "fresh", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	m := newDispatchIngressManagerLargeSegments(t)
	c := &zzWP6TimedCommitter{}
	if _, err := m.AcceptProduce(context.Background(), "fresh", "k", 1, []byte(`{"f":1}`)); err != nil {
		t.Fatal(err)
	}
	d := NewProduceDispatcher(m, store, "node-self", c, nil, nil, ProduceDispatcherConfig{})
	zzWP6StartRun(t, d)
	lats := zzWP6LocalLatencies(t, m, c, 30, 50*time.Millisecond, 2*time.Second)
	if mean := zzWP6Mean(lats); mean > 100*time.Millisecond {
		t.Fatalf("orders accept-to-commit mean %v while a new topic has no owners, want no backoff", mean)
	}
	// Once the new topic's partitions are assigned, its record goes
	// out promptly instead of after a one-second backoff.
	for p := range 3 {
		if err := store.AssignPartition(context.Background(), "fresh", p, "node-self"); err != nil {
			t.Fatal(err)
		}
	}
	assigned := time.Now()
	for {
		if _, ok := c.when(0); ok {
			break
		}
		if time.Since(assigned) > 500*time.Millisecond {
			t.Fatal("the new topic's record was not dispatched within 500ms of its partitions getting owners")
		}
		time.Sleep(time.Millisecond)
	}
}

// Batches in flight to a hung owner stay in memory until their RPCs
// give up, but they must not fill the window: with several of the
// owner's partitions hung, each holding a full batch, the dispatcher
// used to stop reading until the 30 s deadline, and the node's own
// partitions with it.
func TestZZWP6HungBatchesDoNotFillTheWindow(t *testing.T) {
	store := newTestStore(t)
	const remote = 8
	zzWP6SeedTwoOwners(t, store, 1, remote)
	m := newDispatchIngressManagerLargeSegments(t)
	c := &zzWP6TimedCommitter{}
	peer := fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, _ nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		<-ctx.Done()
		return nodewire.Response{}, ctx.Err()
	}}
	// A 256-record window: a quarter of it per destination, so four
	// hung partitions with full batches in flight would fill it.
	const window = 256
	for i := range remote * window / 4 {
		if _, err := m.AcceptProduce(context.Background(), "orders", "k", 1+i%remote, []byte(`{"r":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	d := NewProduceDispatcher(m, store, "node-self", c, peer, nil, ProduceDispatcherConfig{BatchSize: window})
	zzWP6StartRun(t, d)
	zzWP6LocalLatencies(t, m, c, 5, 50*time.Millisecond, 4*time.Second)
}
