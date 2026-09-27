package cluster

import (
	"context"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// At low load nothing changes: every ack is its own OpAck.
func TestZZWP12CoalescerIdleSendsSingles(t *testing.T) {
	peer := &zzWP12Peer{}
	router := zzWP12Router(t, peer)
	for i := range 20 {
		w := httptest.NewRecorder()
		router.RouteAck(context.Background(), w, nil, "orders", consumer.Handle{Partition: 0, Offset: int64(i), Nonce: 1})
		if w.Code != http.StatusNoContent {
			t.Fatalf("ack %d: status %d", i, w.Code)
		}
	}
	for _, c := range peer.snapshot() {
		if c.batch {
			t.Fatalf("an idle owner got a batch: %+v", c)
		}
	}
}

// zzWP12Gate makes the fake owner's singles block until released,
// counting how many are held.
type zzWP12Gate struct {
	held    atomic.Int32
	release chan struct{}
}

func newZZWP12Gate() *zzWP12Gate { return &zzWP12Gate{release: make(chan struct{})} }

func (g *zzWP12Gate) single(_ context.Context, _ string, item nodewire.AckBatchItem) (nodewire.Response, error) {
	g.held.Add(1)
	<-g.release
	return zzWP12Owner(item), nil
}

func zzWP12Eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func zzWP12Queued(router *Router, addr string) int {
	o := router.acks.owner(addr)
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, b := range o.queue {
		n += len(b.items)
	}
	return n
}

// zzWP12Result is one RouteAck's response.
type zzWP12Result struct {
	nonce       int64
	code        int
	contentType string
	body        string
}

func zzWP12Ack(router *Router, ctx context.Context, h consumer.Handle, out chan<- zzWP12Result) {
	w := httptest.NewRecorder()
	router.RouteAck(ctx, w, nil, "orders", h)
	out <- zzWP12Result{nonce: h.Nonce, code: w.Code, contentType: w.Header().Get("Content-Type"), body: w.Body.String()}
}

// With every slot to an owner busy, further acks wait and leave as one
// OpAckBatch the moment a slot frees, and each gets exactly the response
// its own OpAck would have: the bare 204, or the owner's 410 body byte
// for byte.
func TestZZWP12CoalescerBatchesWhenSlotsBusy(t *testing.T) {
	gate := newZZWP12Gate()
	peer := &zzWP12Peer{single: gate.single}
	router := zzWP12Router(t, peer)
	slots := router.acks.owner(zzWP12AddrA).limit
	results := make(chan zzWP12Result, 32)
	for i := range slots {
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(i), Nonce: 1}, results)
	}
	zzWP12Eventually(t, "every slot to be busy", func() bool { return int(gate.held.Load()) == slots })
	const queued = 10
	for i := range queued {
		nonce := int64(100 + i)
		if i == 3 {
			nonce = zzWP12StaleNonce
		}
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(50 + i), Nonce: nonce}, results)
	}
	zzWP12Eventually(t, "the acks to queue", func() bool { return zzWP12Queued(router, zzWP12AddrA) == queued })
	close(gate.release)

	var stale zzWP12Result
	for range slots + queued {
		r := <-results
		if r.nonce == zzWP12StaleNonce {
			stale = r
			continue
		}
		if r.code != http.StatusNoContent || r.body != "" || r.contentType != "" {
			t.Fatalf("ack nonce %d answered %d %q %q, want a bare 204", r.nonce, r.code, r.contentType, r.body)
		}
	}
	calls := peer.snapshot()
	var batch []nodewire.AckBatchItem
	for _, c := range calls {
		if c.batch {
			if batch != nil {
				t.Fatalf("more than one batch: %+v", calls)
			}
			batch = c.items
		}
	}
	if len(batch) != queued {
		t.Fatalf("batch carried %d records, want the %d queued acks: %+v", len(batch), queued, calls)
	}
	if len(calls) != slots+1 {
		t.Fatalf("%d RPCs for %d acks, want %d singles and one batch", len(calls), slots+queued, slots)
	}

	// The stale ack's response is what a single OpAck of it produces.
	gate2 := &zzWP12Peer{}
	single := httptest.NewRecorder()
	zzWP12Router(t, gate2).RouteAck(context.Background(), single, nil, "orders", consumer.Handle{Partition: 0, Nonce: zzWP12StaleNonce})
	if stale.code != single.Code || stale.body != single.Body.String() || stale.contentType != single.Header().Get("Content-Type") {
		t.Fatalf("queued stale ack = %d %q %q; single = %d %q %q", stale.code, stale.contentType, stale.body,
			single.Code, single.Header().Get("Content-Type"), single.Body.String())
	}
}

// An owner that refuses the batch op: the queued acks each go out as an
// OpAck, and later acks to it skip the queue altogether.
func TestZZWP12CoalescerLegacyOwner(t *testing.T) {
	gate := newZZWP12Gate()
	peer := &zzWP12Peer{
		single: gate.single,
		batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
			return zzWP12Unsupported(), nil
		},
	}
	router := zzWP12Router(t, peer)
	slots := router.acks.owner(zzWP12AddrA).limit
	results := make(chan zzWP12Result, 32)
	for i := range slots {
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(i), Nonce: 1}, results)
	}
	zzWP12Eventually(t, "every slot to be busy", func() bool { return int(gate.held.Load()) == slots })
	for i := range 5 {
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(50 + i), Nonce: 2}, results)
	}
	zzWP12Eventually(t, "the acks to queue", func() bool { return zzWP12Queued(router, zzWP12AddrA) == 5 })
	close(gate.release)
	for range slots + 5 {
		if r := <-results; r.code != http.StatusNoContent {
			t.Fatalf("ack answered %d %q", r.code, r.body)
		}
	}
	if !router.acks.legacy.is(zzWP12AddrA) {
		t.Fatal("the refusing owner was not remembered")
	}
	before := len(peer.snapshot())
	w := httptest.NewRecorder()
	router.RouteAck(context.Background(), w, nil, "orders", consumer.Handle{Partition: 4, Nonce: 3})
	calls := peer.snapshot()
	if w.Code != http.StatusNoContent || len(calls) != before+1 || calls[len(calls)-1].batch {
		t.Fatalf("ack to a legacy owner: %d, calls %+v", w.Code, calls[before:])
	}
}

// A queued caller that gives up is answered at once, and its record is
// not sent: the requester already told its client it failed.
func TestZZWP12CoalescerQueuedCallerLeaves(t *testing.T) {
	gate := newZZWP12Gate()
	peer := &zzWP12Peer{single: gate.single}
	router := zzWP12Router(t, peer)
	slots := router.acks.owner(zzWP12AddrA).limit
	results := make(chan zzWP12Result, 32)
	for i := range slots {
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(i), Nonce: 1}, results)
	}
	zzWP12Eventually(t, "every slot to be busy", func() bool { return int(gate.held.Load()) == slots })
	ctx, cancel := context.WithCancel(context.Background())
	go zzWP12Ack(router, ctx, consumer.Handle{Partition: 0, Offset: 70, Nonce: 70}, results)
	go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: 71, Nonce: 71}, results)
	go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: 72, Nonce: 72}, results)
	zzWP12Eventually(t, "the acks to queue", func() bool { return zzWP12Queued(router, zzWP12AddrA) == 3 })
	cancel()
	r := <-results
	if r.nonce != 70 || r.code != http.StatusBadGateway {
		t.Fatalf("first answer = %+v, want the cancelled ack's 502", r)
	}
	close(gate.release)
	for range slots + 2 {
		if r := <-results; r.code != http.StatusNoContent {
			t.Fatalf("ack %d answered %d", r.nonce, r.code)
		}
	}
	for _, c := range peer.snapshot() {
		for _, item := range c.items {
			if item.Nonce == 70 {
				t.Fatalf("the abandoned ack was sent: %+v", c)
			}
		}
	}
}

// Many callers acking through the coalescer at once, against owners
// with jittery latency: every ack is sent exactly once, singly or in a
// batch, and every caller gets its own record's outcome (the owners
// answer 410 for a negative nonce).
func TestZZWP12CoalescerRace(t *testing.T) {
	owner := func(item nodewire.AckBatchItem) nodewire.Response {
		if item.Nonce < 0 {
			return errorResponse(http.StatusGone, "receipt handle is stale")
		}
		return nodewire.Response{Status: http.StatusNoContent}
	}
	var sent sync.Map // nonce -> count
	count := func(items []nodewire.AckBatchItem) {
		for _, item := range items {
			v, _ := sent.LoadOrStore(item.Nonce, new(atomic.Int32))
			v.(*atomic.Int32).Add(1)
		}
	}
	jitter := func() { time.Sleep(time.Duration(rand.IntN(300)) * time.Microsecond) }
	peer := &zzWP12Peer{
		single: func(_ context.Context, _ string, item nodewire.AckBatchItem) (nodewire.Response, error) {
			jitter()
			count([]nodewire.AckBatchItem{item})
			return owner(item), nil
		},
		batch: func(_ context.Context, _ string, req nodewire.AckBatchRequest) (nodewire.Response, error) {
			jitter()
			count(req.Items)
			return zzWP12BatchReply(req, owner), nil
		},
	}
	router := zzWP12Router(t, peer)
	// A narrow window, so 48 callers keep both owners' queues busy: the
	// queue works the same at any width.
	zzWP12Window(router, 4, zzWP12AddrA, zzWP12AddrB)
	const workers, each = 48, 60
	var wg sync.WaitGroup
	var bad atomic.Int32
	for w := range workers {
		wg.Go(func() {
			for i := range each {
				nonce, want := int64(1000+w*each+i), http.StatusNoContent
				if i%17 == 0 {
					nonce, want = -nonce, http.StatusGone
				}
				item := consumer.Handle{Partition: []int{0, 4, 1}[i%3], Offset: int64(i), Nonce: nonce}
				rec := httptest.NewRecorder()
				router.RouteAck(context.Background(), rec, nil, "orders", item)
				if rec.Code != want {
					bad.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d acks got another record's outcome or failed", n)
	}
	total := 0
	sent.Range(func(k, v any) bool {
		total++
		if n := v.(*atomic.Int32).Load(); n != 1 {
			t.Errorf("nonce %d sent %d times", k, n)
		}
		return true
	})
	if total != workers*each {
		t.Fatalf("%d distinct acks sent, want %d", total, workers*each)
	}
	batches := 0
	for _, c := range peer.snapshot() {
		if c.batch {
			batches++
		}
	}
	if batches == 0 {
		t.Fatal("no batch formed: the run never exercised the queue")
	}
	// Every owner's slots come free again and nothing is left queued. A
	// batch hands its callers their outcomes before it frees its slot,
	// so the last one may still be on its way out.
	for _, addr := range []string{zzWP12AddrA, zzWP12AddrB} {
		o := router.acks.owner(addr)
		zzWP12Eventually(t, "owner "+addr+" to go idle", func() bool {
			o.mu.Lock()
			defer o.mu.Unlock()
			return o.inflight == 0 && len(o.queue) == 0
		})
	}
}

// zzWP12Window sets the in-flight window for the given owners before
// their first ack.
func zzWP12Window(router *Router, slots int, addrs ...string) {
	for _, addr := range addrs {
		router.acks.owners.Store(addr, &ackOwner{limit: slots})
	}
}

// Well past what a gateway usually has in flight to one owner, an ack
// never waits for another one's round trip: it goes out at once, as on
// a release without coalescing. With a window of 4, the 5th of 16
// concurrent acks waited for a whole round trip, which on a 1 ms
// network made forwarded acks up to 93% slower.
func TestZZWP14CoalescerWindowIsWide(t *testing.T) {
	gate := newZZWP12Gate()
	peer := &zzWP12Peer{single: gate.single}
	router := zzWP12Router(t, peer)
	const concurrent = 16
	results := make(chan zzWP12Result, concurrent)
	for i := range concurrent {
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(i), Nonce: 1}, results)
	}
	zzWP12Eventually(t, "every ack to reach the owner", func() bool { return gate.held.Load() == concurrent })
	if n := zzWP12Queued(router, zzWP12AddrA); n != 0 {
		t.Fatalf("%d acks queued with %d in flight", n, concurrent)
	}
	close(gate.release)
	for range concurrent {
		if r := <-results; r.code != http.StatusNoContent {
			t.Fatalf("ack answered %d %q", r.code, r.body)
		}
	}
	for _, c := range peer.snapshot() {
		if c.batch {
			t.Fatalf("an owner below its window got a batch: %+v", c)
		}
	}
	if slots, want := router.acks.owner(zzWP12AddrA).limit, 2*defaultMessagingConcurrency(); slots != want {
		t.Fatalf("window = %d, want twice the owner's messaging bound, %d", slots, want)
	}
}
