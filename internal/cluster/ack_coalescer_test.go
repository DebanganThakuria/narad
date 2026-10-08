package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// ackTestOwnerAddr is the owner of partition 1 of "orders" in
// seedTopicRouteState.
const ackTestOwnerAddr = "remote.example:7942"

// ackOwnerFake is a partition owner behind the ack coalescer. It takes
// OpAckBatch as well as single acks, applies every record the moment
// its RPC arrives (as a real owner does once the request holds a
// handler slot), and replies when answer's channel for the RPC's first
// record closes; nil replies at once. A reply that comes later than the
// call's budget fails with a deadline error, as the transport does,
// although the owner applied the records.
type ackOwnerFake struct {
	fakePeerClient
	answer func(offset int64) <-chan struct{}

	mu      sync.Mutex
	applied []int64
	budgets map[int64]time.Duration
}

func (f *ackOwnerFake) call(ctx context.Context, timeout time.Duration, offsets []int64) error {
	f.mu.Lock()
	f.applied = append(f.applied, offsets...)
	if f.budgets == nil {
		f.budgets = make(map[int64]time.Duration)
	}
	for _, off := range offsets {
		f.budgets[off] = timeout
	}
	f.mu.Unlock()
	var ready <-chan struct{}
	if f.answer != nil {
		ready = f.answer(offsets[0])
	}
	if ready == nil {
		return nil
	}
	budget := time.NewTimer(timeout)
	defer budget.Stop()
	select {
	case <-ready:
		return nil
	case <-budget.C:
		return fmt.Errorf("cluster rpc request timed out: %w", context.DeadlineExceeded)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *ackOwnerFake) AckWithin(ctx context.Context, _ string, timeout time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	if err := f.call(ctx, timeout, []int64{req.Offset}); err != nil {
		return nodewire.Response{}, err
	}
	return nodewire.Response{Status: http.StatusNoContent}, nil
}

func (f *ackOwnerFake) AckBatchWithin(ctx context.Context, _ string, timeout time.Duration, req nodewire.AckBatchRequest) (nodewire.Response, error) {
	offsets := make([]int64, len(req.Items))
	results := make([]nodewire.AckResult, len(req.Items))
	for i, item := range req.Items {
		offsets[i] = item.Offset
		results[i] = nodewire.AckResult{Status: http.StatusNoContent}
	}
	if err := f.call(ctx, timeout, offsets); err != nil {
		return nodewire.Response{}, err
	}
	body, err := nodewire.AppendAckBatchReply(nil, results)
	return nodewire.Response{Status: http.StatusOK, Body: body}, err
}

func (f *ackOwnerFake) appliedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := slices.Clone(f.applied)
	slices.Sort(out)
	return out
}

func (f *ackOwnerFake) budget(offset int64) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.budgets[offset]
}

// ackTestRouter is a router whose acks for partition 1 of "orders" go
// to owner, with limit ack RPCs to it in flight before acks queue.
func ackTestRouter(t *testing.T, owner *ackOwnerFake, limit int) (*Router, *ackOwner) {
	t.Helper()
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	rt := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	rt.peer = owner
	o := &ackOwner{limit: limit}
	rt.acks.owners.Store(ackTestOwnerAddr, o)
	return rt, o
}

// routeTestAck acks the record at offset of partition 1 through rt.
func routeTestAck(ctx context.Context, rt *Router, offset int64) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	rt.RouteAck(ctx, rec, nil, "orders", consumer.Handle{Partition: 1, Offset: offset, Nonce: 7})
	return rec
}

// waitApplied waits until owner has applied n records.
func waitApplied(t *testing.T, owner *ackOwnerFake, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if len(owner.appliedOffsets()) >= n {
			return
		}
	}
	t.Fatalf("owner applied %v, want %d records", owner.appliedOffsets(), n)
}

// waitQueued waits until n acks wait in o's queue.
func waitQueued(t *testing.T, o *ackOwner, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		o.mu.Lock()
		queued := 0
		for _, b := range o.queue {
			queued += len(b.items)
		}
		o.mu.Unlock()
		if queued >= n {
			return
		}
	}
	t.Fatalf("fewer than %d acks queued", n)
}

func after(d time.Duration) <-chan struct{} {
	c := make(chan struct{})
	time.AfterFunc(d, func() { close(c) })
	return c
}

// queueBehindHeldSlot sends ack 1, which holds the only slot until
// release closes, then queues acks 2 and 3 behind it, and returns
// their recorders once both are answered, plus when they joined.
func queueBehindHeldSlot(t *testing.T, rt *Router, o *ackOwner, owner *ackOwnerFake) (func() []*httptest.ResponseRecorder, time.Time) {
	t.Helper()
	go routeTestAck(context.Background(), rt, 1)
	waitApplied(t, owner, 1)
	joined := time.Now()
	recs := make([]*httptest.ResponseRecorder, 2)
	var wg sync.WaitGroup
	for i := range recs {
		wg.Go(func() { recs[i] = routeTestAck(context.Background(), rt, int64(i+2)) })
	}
	waitQueued(t, o, 2)
	return func() []*httptest.ResponseRecorder {
		wg.Wait()
		return recs
	}, joined
}

// A queued ack whose batch left just before the ack's queue wait ran
// out must get the batch's answer, not an error: once the batch is on
// the wire the owner applies it, and a 502 then tells the client the
// ack failed when it landed.
func TestQueuedAckIsAnsweredByItsBatchAfterTheBatchLeft(t *testing.T) {
	release := make(chan struct{})
	owner := &ackOwnerFake{answer: func(offset int64) <-chan struct{} {
		if offset == 1 {
			return release
		}
		// The batch's reply comes 300ms after it left, past the queued
		// acks' 2s queue wait.
		return after(300 * time.Millisecond)
	}}
	rt, o := ackTestRouter(t, owner, 1)
	wait, joined := queueBehindHeldSlot(t, rt, o, owner)
	time.Sleep(time.Until(joined.Add(ackForwardTimeout - 150*time.Millisecond)))
	close(release)

	for i, rec := range wait() {
		if rec.Code != http.StatusNoContent {
			t.Errorf("queued ack %d: status %d (%q), want 204: the owner applied it", i+2, rec.Code, rec.Body.String())
		}
	}
	if got, want := owner.appliedOffsets(), []int64{1, 2, 3}; !slices.Equal(got, want) {
		t.Fatalf("owner applied %v, want %v, each once", got, want)
	}
}

// A batch that leaves late still gets the whole round-trip budget, not
// what is left of the queue wait: a remnant of 100ms makes the reply
// time out after the owner applied the batch.
func TestBatchLeavingLateGetsAFullRoundTripBudget(t *testing.T) {
	release := make(chan struct{})
	owner := &ackOwnerFake{answer: func(offset int64) <-chan struct{} {
		if offset == 1 {
			return release
		}
		return nil
	}}
	rt, o := ackTestRouter(t, owner, 1)
	wait, joined := queueBehindHeldSlot(t, rt, o, owner)
	time.Sleep(time.Until(joined.Add(ackForwardTimeout - 100*time.Millisecond)))
	close(release)
	wait()

	for _, off := range []int64{2, 3} {
		if got := owner.budget(off); got != ackForwardTimeout {
			t.Errorf("record %d left with a budget of %s, want %s", off, got, ackForwardTimeout)
		}
	}
}
