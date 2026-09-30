package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// These drive Run's slow-commit path one step at a time, on a settable
// clock: markSlow returning a slow commit's queue to the WAL, and the
// targeted rescan (rescanFrom, Run only) that reads it back once the
// commit lands, or the reroute once it fails past the grace.

// zzShipSlowPeer is a remote owner whose partition 1 commits block
// until the test sends their outcome on release; every other partition
// commits at once. It records the {"i":N} payloads it committed, per
// partition, in order.
type zzShipSlowPeer struct {
	mu      sync.Mutex
	got     map[int][]int
	calls   chan int
	release chan error
}

func (p *zzShipSlowPeer) client() fakePeerClient {
	return fakePeerClient{commitProduceBatchFn: func(ctx context.Context, _ string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
		part := req.Records[0].TargetPartition
		p.calls <- part
		if part == 1 {
			select {
			case err := <-p.release:
				if err != nil {
					return nodewire.Response{}, err
				}
			case <-ctx.Done():
				return nodewire.Response{}, ctx.Err()
			}
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, r := range req.Records {
			var i int
			if _, err := fmt.Sscanf(string(r.Payload), `{"i":%d}`, &i); err != nil {
				return nodewire.Response{}, err
			}
			p.got[r.TargetPartition] = append(p.got[r.TargetPartition], i)
		}
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
}

func (p *zzShipSlowPeer) committed(part int) []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.got[part])
}

func zzShipSlowWaitCall(t *testing.T, p *zzShipSlowPeer, want int) {
	t.Helper()
	select {
	case part := <-p.calls:
		if part != want {
			t.Fatalf("commit went to partition %d, want %d", part, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no commit reached partition %d", want)
	}
}

// zzShipSlowMerge merges the next finished commit, as Run's select does.
func zzShipSlowMerge(t *testing.T, d *ProduceDispatcher) {
	t.Helper()
	select {
	case res := <-d.results:
		d.finish(context.Background(), d.state, res)
	case <-time.After(5 * time.Second):
		t.Fatal("commit never reported back")
	}
}

// zzShipSlowSetup builds a Run-mode dispatcher (manual false) for
// "orders" with partition 0 here and remote partitions 1.. on the slow
// peer, and returns an accept(part, from, to) that produces {"i":from}
// through {"i":to-1} to one partition.
func zzShipSlowSetup(t *testing.T, remote int) (*ProduceDispatcher, *zzShipSlowPeer, *zzWP6Clock, func(part, from, to int)) {
	t.Helper()
	store := newTestStore(t)
	zzWP6SeedTwoOwners(t, store, 1, remote)
	m := newDispatchIngressManagerLargeSegments(t)
	peer := &zzShipSlowPeer{got: map[int][]int{}, calls: make(chan int, 16), release: make(chan error)}
	d := NewProduceDispatcher(m, store, "node-self", &fakeProduceCommitter{}, peer.client(), nil, ProduceDispatcherConfig{})
	clock := &zzWP6Clock{now: time.Unix(1_700_000_000, 0)}
	d.now = clock.Now
	if err := d.loadCursor(); err != nil {
		t.Fatal(err)
	}
	accept := func(part, from, to int) {
		for i := from; i < to; i++ {
			if _, err := m.AcceptProduce(context.Background(), "orders", "k", part, fmt.Appendf(nil, `{"i":%d}`, i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	return d, peer, clock, accept
}

// zzShipSlowPrefix puts {"i":0} in flight to partition 1, queues 1..3
// behind it, lets the commit turn slow so markSlow returns them to the
// WAL, and accepts 4..5, which must stay in the WAL behind them.
func zzShipSlowPrefix(t *testing.T, d *ProduceDispatcher, peer *zzShipSlowPeer, clock *zzWP6Clock, accept func(int, int, int)) *dispatchDest {
	t.Helper()
	ctx := context.Background()
	st := d.state
	accept(1, 0, 1)
	d.step(ctx, st)
	zzShipSlowWaitCall(t, peer, 1)
	accept(1, 1, 4)
	d.step(ctx, st)
	p1 := st.dests[dispatchDestKey{topic: "orders", partition: 1}]
	if p1 == nil || len(p1.queue) != 3 || !p1.inflight {
		t.Fatalf("want 3 records queued behind the in-flight commit, got %+v", p1)
	}
	clock.Advance(produceDispatchSlowAfter)
	d.step(ctx, st)
	if !p1.slow || p1.skipped != 3 || len(p1.queue) != 0 || st.slowHeld != 1 {
		t.Fatalf("after markSlow: slow=%v skipped=%d queue=%d slowHeld=%d; want the 3 queued records back in the WAL", p1.slow, p1.skipped, len(p1.queue), st.slowHeld)
	}
	accept(1, 4, 6)
	d.step(ctx, st)
	if p1.skipped != 5 || len(p1.queue) != 0 {
		t.Fatalf("after more accepts: skipped=%d queue=%d, want 5 and 0 (behind the released ones)", p1.skipped, len(p1.queue))
	}
	return p1
}

// A slow commit that lands asks for a targeted rescan; the records
// markSlow released, and those skipped behind them, go out next, in WAL
// order, and the checkpoint reaches the durable frontier.
func TestSlowCommitLandsAndReleasedRecordsFollowInOrder(t *testing.T) {
	d, peer, clock, accept := zzShipSlowSetup(t, 1)
	ctx := context.Background()
	st := d.state
	p1 := zzShipSlowPrefix(t, d, peer, clock, accept)

	peer.release <- nil
	zzShipSlowMerge(t, d)
	if !st.rescanDue || !st.rescanSet || st.manual {
		t.Fatalf("want a targeted Run-mode rescan requested: due=%v set=%v manual=%v", st.rescanDue, st.rescanSet, st.manual)
	}
	d.step(ctx, st)
	zzShipSlowWaitCall(t, peer, 1)
	peer.release <- nil
	zzShipSlowMerge(t, d)
	d.step(ctx, st)

	if got, want := peer.committed(1), []int{0, 1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("partition 1 got %v, want %v", got, want)
	}
	if p1.skipped != 0 || st.skipped != 0 {
		t.Fatalf("skipped left: dest %d state %d", p1.skipped, st.skipped)
	}
	if next := d.ingress.DurableProduceNext(); st.nextSeq != next {
		t.Fatalf("checkpoint %d, want the durable frontier %d", st.nextSeq, next)
	}
}

// A slow commit that fails past the reroute grace sends the failed
// record, the ones markSlow released and those skipped behind them to a
// live sibling, in order, and the checkpoint reaches the durable
// frontier.
func TestSlowCommitFailsPastGraceAndReleasedRecordsGoToSibling(t *testing.T) {
	d, peer, clock, accept := zzShipSlowSetup(t, 2)
	ctx := context.Background()
	st := d.state
	zzShipSlowPrefix(t, d, peer, clock, accept)

	clock.Advance(produceDispatchRerouteGrace)
	d.step(ctx, st)
	peer.release <- errors.New("owner stopped answering")
	zzShipSlowMerge(t, d)
	d.step(ctx, st)
	zzShipSlowWaitCall(t, peer, 2)
	zzShipSlowMerge(t, d)
	d.step(ctx, st)

	if got, want := peer.committed(2), []int{0, 1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("sibling partition 2 got %v, want %v (partition 1 got %v)", got, want, peer.committed(1))
	}
	if st.skipped != 0 {
		t.Fatalf("skipped left: %d", st.skipped)
	}
	if next := d.ingress.DurableProduceNext(); st.nextSeq != next {
		t.Fatalf("checkpoint %d, want the durable frontier %d", st.nextSeq, next)
	}
}
