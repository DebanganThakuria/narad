package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP9HandedOnWake mimics broker ConsumeWait's external branch when the
// pump handed the waiter a local record in the same instant.
type zzWP9HandedOnWake struct{}

func (zzWP9HandedOnWake) Wait(ctx context.Context, wait time.Duration, external <-chan struct{}) (topic.Message, bool, bool, error) {
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-external:
		return topic.Message{Topic: "orders", Partition: 1, Offset: 3, ReceiptHandle: "1:3:5"}, true, false, nil
	case <-t.C:
		return topic.Message{}, false, false, nil
	case <-ctx.Done():
		return topic.Message{}, false, false, ctx.Err()
	}
}

func (zzWP9HandedOnWake) Release(context.Context, topic.Message) error { return nil }

// zzWP9LocalHit returns a local record after delay.
type zzWP9LocalHit struct{ delay time.Duration }

func (l zzWP9LocalHit) Wait(ctx context.Context, wait time.Duration, external <-chan struct{}) (topic.Message, bool, bool, error) {
	t := time.NewTimer(min(l.delay, wait))
	defer t.Stop()
	select {
	case <-t.C:
		return topic.Message{Topic: "orders", Partition: 1, Offset: 3, ReceiptHandle: "1:3:5"}, true, false, nil
	case <-external:
		return topic.Message{}, false, true, nil
	case <-ctx.Done():
		return topic.Message{}, false, false, ctx.Err()
	}
}

func (zzWP9LocalHit) Release(context.Context, topic.Message) error { return nil }

// zzWP9BlockingWriter holds the response body write until released.
type zzWP9BlockingWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *zzWP9BlockingWriter) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return b.ResponseRecorder.Write(p)
}

// zzWP9Strand: consumer A is woken by an owner's notification while it
// is leaving without claiming, and consumer B stays parked. The owner
// spent this node's token on A, so something must put a token back
// there for B well before B's budget runs out.
func zzWP9Strand(t *testing.T, served bool) {
	reg := newRegistrationLog()
	router := tokenRouter(t, fakePeerClient{registerTokensFn: reg.registerTokens})
	const key = "remote.example:7942/orders"

	aDone := make(chan struct{})
	var bw *zzWP9BlockingWriter
	if served {
		bw = &zzWP9BlockingWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
		go func() {
			defer close(aDone)
			router.RouteConsumeWait(context.Background(), bw, httptest.NewRequest(http.MethodGet, "/", nil), "orders", 5*time.Second, zzWP9LocalHit{delay: 300 * time.Millisecond})
		}()
	} else {
		go func() {
			defer close(aDone)
			router.RouteConsumeWait(context.Background(), httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), "orders", 5*time.Second, zzWP9HandedOnWake{})
		}()
	}
	time.Sleep(50 * time.Millisecond)
	bDone := make(chan struct{})
	go func() {
		defer close(bDone)
		router.RouteConsumeWait(context.Background(), httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), "orders", 4*time.Second, &fakeLocalWaiter{delay: time.Hour})
	}()
	time.Sleep(50 * time.Millisecond)
	before, _ := reg.counts(key)
	if bw != nil {
		<-bw.entered // A has its record and is writing the response
	}
	router.LocalDemand().WakeOneWaiter("orders", "remote.example:7942")
	wokeAt := time.Now()
	if bw != nil {
		close(bw.release)
	}
	<-aDone
	for time.Since(wokeAt) < 1500*time.Millisecond {
		if adds, _ := reg.counts(key); adds > before {
			<-bDone
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the owner's token was spent on a consumer that left, and nothing re-registered for the one still parked")
}

func TestZZWP9WakeOnALeavingConsumerDoesNotStrandOthers(t *testing.T) {
	zzWP9Strand(t, false)
}

func TestZZWP9WakeWhileServingDoesNotStrandOthers(t *testing.T) {
	zzWP9Strand(t, true)
}

// Parking, registering and the wait check must not advance the probe
// cursor: with three remote owners a probe, a park and a registration
// used to advance it by exactly three, pinning every first probe to the
// same owner.
func TestZZWP9ProbeRotationSpreadsAcrossOwners(t *testing.T) {
	var mu sync.Mutex
	first := ""
	peer := fakePeerClient{consumeFn: func(_ context.Context, addr string, _ nodewire.ConsumeRequest) (nodewire.Response, error) {
		mu.Lock()
		if first == "" {
			first = addr
		}
		mu.Unlock()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	router := multiOwnerRouter(t, peer, 3)
	router.SetSelfAddr("node-self.example:7942")
	seen := map[string]bool{}
	for range 6 {
		mu.Lock()
		first = ""
		mu.Unlock()
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=60ms", nil)
		router.RouteConsumeRemote(context.Background(), res, req, "orders")
		router.RouteConsumeWait(context.Background(), res, req, "orders", 60*time.Millisecond, &fakeLocalWaiter{delay: time.Hour})
		mu.Lock()
		seen[first] = true
		mu.Unlock()
	}
	if len(seen) < 3 {
		t.Fatalf("first owner probed over 6 cycles: %v, want the start to rotate across all three", seen)
	}
}

// The remaining owners are not probed once the client is gone.
func TestZZWP9ProbeStopsWhenTheClientLeaves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var probes atomic.Int32
	router := multiOwnerRouter(t, fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		probes.Add(1)
		cancel()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}, 3)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	router.RouteConsumeRemote(ctx, res, req, "orders")
	if n := probes.Load(); n != 1 {
		t.Fatalf("probed %d owners after the client left during the first probe, want 1", n)
	}
}

// A forwarded 204 is written bare: a bodiless reply has nothing to type
// or sniff, and setting the headers cost a header map and its clone on
// every forwarded ack. A reply with a body keeps both headers.
func TestZZWP9ForwardedNoContentIsBare(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{ackFn: func(context.Context, string, nodewire.AckRequest) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	rec := httptest.NewRecorder()
	if !router.RouteAck(context.Background(), rec, nil, "orders", consumer.Handle{Partition: 1, Offset: 3, Nonce: 4}) {
		t.Fatal("ack not forwarded")
	}
	if rec.Code != http.StatusNoContent || len(rec.Header()) != 0 {
		t.Fatalf("forwarded ack: status %d headers %v, want a bare 204", rec.Code, rec.Header())
	}

	rec = httptest.NewRecorder()
	writePeerResponse(rec, nodewire.Response{Status: http.StatusOK, Body: []byte("<html>")})
	if got := rec.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream for an untyped body", got)
	}
	if got := rec.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	rec = httptest.NewRecorder()
	writePeerResponse(rec, nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: []byte("{}")})
	if got := rec.Header().Get("Content-Type"); got != nodewire.ContentTypeJSON {
		t.Fatalf("Content-Type = %q, want %q", got, nodewire.ContentTypeJSON)
	}
}

// zzWP9Budgets is a frameTransport that records, per request, the budget
// the caller handed the transport and whether ctx carried a deadline.
type zzWP9Budgets struct {
	mu       sync.Mutex
	budgets  []time.Duration
	deadline []bool
	reply    []byte
}

func (b *zzWP9Budgets) record(ctx context.Context, budget time.Duration) (clusterwire.StreamFrame, error) {
	_, has := ctx.Deadline()
	b.mu.Lock()
	b.budgets = append(b.budgets, budget)
	b.deadline = append(b.deadline, has)
	b.mu.Unlock()
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, Payload: b.reply}, nil
}

func (b *zzWP9Budgets) RequestOnLane(ctx context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, _ []byte) (clusterwire.StreamFrame, error) {
	return b.record(ctx, 0)
}

func (b *zzWP9Budgets) RequestOnLaneTimeout(ctx context.Context, _ string, _ clusterrpc.Lane, timeout time.Duration, _ clusterwire.StreamFrameType, _ []byte) (clusterwire.StreamFrame, error) {
	return b.record(ctx, timeout)
}

func (b *zzWP9Budgets) take() ([]time.Duration, []bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	budgets, deadline := b.budgets, b.deadline
	b.budgets, b.deadline = nil, nil
	return budgets, deadline
}

// Every forward on the ack, probe and claim paths is bounded by a budget
// handed to the transport, not by a context.WithTimeout derived per
// call: the transport enforces it with the timer it already runs, and
// the caller's context still carries cancellation.
func TestZZWP9ForwardsHandTheirBudgetToTheTransport(t *testing.T) {
	frames := &zzWP9Budgets{reply: zzWP9Reply(t, nodewire.Response{Status: http.StatusNoContent})}
	router := zzWP9Router(t, "remote.example:7942", &PeerClient{frames: frames})
	handle := consumer.Handle{Partition: 0, Offset: 3, Nonce: 4}
	ctx := context.Background()
	router.RouteAck(ctx, httptest.NewRecorder(), nil, "orders", handle)
	router.RouteExtendAck(ctx, httptest.NewRecorder(), nil, "orders", handle)
	router.RouteNack(ctx, httptest.NewRecorder(), nil, "orders", handle)
	router.RouteConsumeRemote(ctx, httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), "orders")
	router.claimFrom(ctx, "remote.example:7942", "orders")
	budgets, deadlines := frames.take()
	want := []time.Duration{ackForwardTimeout, ackForwardTimeout, ackForwardTimeout, consumeProbeTimeout, consumeProbeTimeout}
	if fmt.Sprint(budgets) != fmt.Sprint(want) {
		t.Fatalf("budgets = %v, want %v", budgets, want)
	}
	for i, has := range deadlines {
		if has {
			t.Fatalf("request %d carried a derived context deadline, want the budget only", i)
		}
	}

	// Token registrations and notifications carry theirs too.
	router.SetSelfAddr("node-self.example:7942")
	router.tokens.broadcast(ctx, []string{"remote.example:7942"}, nodewire.TokenDelta{From: "node-self.example:7942", Add: []nodewire.TokenRegistration{{Topic: "orders", TTLNanos: int64(time.Second)}}}, nil)
	holder := newTokenHolder(nil, &PeerClient{frames: frames}, "node-self.example:7942")
	holder.notify(ctx, "remote.example:7942", "orders")
	var got []time.Duration
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		b, _ := frames.take()
		got = append(got, b...)
		if len(got) >= 2 {
			break
		}
	}
	if len(got) != 2 || got[0] <= 0 || got[1] <= 0 {
		t.Fatalf("token budgets = %v, want both bounded by the transport", got)
	}
}
