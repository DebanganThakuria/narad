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

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP9Gateway is a router on a node that owns no partition of "orders"
// (node-remote owns its only partition), with the token protocol on and
// the keeper running. probes counts the non-claim consumes the owner
// sees, adds the token registrations.
type zzWP9Gateway struct {
	router *Router
	probes atomic.Int32
	claims atomic.Int32
	adds   atomic.Int32
	// ready makes the owner's next probe or claim win a record.
	ready atomic.Bool
}

func newZZWP9Gateway(t *testing.T) *zzWP9Gateway {
	t.Helper()
	g := &zzWP9Gateway{}
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 7, Nonce: 99})
	g.router = remoteOnlyConsumeRouter(t, func(_ context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if req.Claim {
			g.claims.Add(1)
		} else {
			g.probes.Add(1)
		}
		if g.ready.CompareAndSwap(true, false) {
			return remoteMessageResponse(0, 7, handle), nil
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	})
	// Production pacing for the polling fallback.
	g.router.consumeReprobeInterval = remoteConsumeReprobeInterval
	g.router.consumeReprobeMaxInterval = remoteConsumeReprobeMaxInterval
	peer := g.router.peer.(fakePeerClient)
	peer.registerTokensFn = func(_ context.Context, _ string, delta nodewire.TokenDelta) (nodewire.Response, error) {
		g.adds.Add(int32(len(delta.Add)))
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}
	g.router.peer = peer
	g.router.SetSelfAddr("node-self.example:7942")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go g.router.RunTokenKeeper(ctx)
	return g
}

func (g *zzWP9Gateway) consume(wait string) (*httptest.ResponseRecorder, bool) {
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait="+wait, nil)
	forwarded, _ := g.router.RouteConsume(context.Background(), res, req, "orders", nil)
	return res, forwarded
}

// A consumer long-polling through a node that owns none of the topic's
// partitions parks on a token instead of re-probing the owner on a
// backoff: an idle wait costs one probe and one registration.
func TestZZWP9GatewayIdleWaitParksOnAToken(t *testing.T) {
	g := newZZWP9Gateway(t)
	start := time.Now()
	res, forwarded := g.consume("1500ms")
	if !forwarded || res.Code != http.StatusNoContent {
		t.Fatalf("forwarded=%v status=%d, want a 204 after the budget", forwarded, res.Code)
	}
	if elapsed := time.Since(start); elapsed < 1400*time.Millisecond {
		t.Fatalf("answered after %s, want the 1.5s budget honoured", elapsed)
	}
	if n := g.probes.Load(); n > 1 {
		t.Fatalf("owner probed %d times in an idle 1.5s wait, want 1 (then parked on a token)", n)
	}
	if n := g.adds.Load(); n < 1 {
		t.Fatal("no token was registered with the owner")
	}
}

// When the owner spends the token, the parked gateway consumer claims
// at once rather than at its next re-probe round.
func TestZZWP9GatewayClaimsAsSoonAsTheOwnerNotifies(t *testing.T) {
	g := newZZWP9Gateway(t)
	var wokeAt atomic.Int64
	go func() {
		time.Sleep(1250 * time.Millisecond)
		g.ready.Store(true)
		for range 2000 {
			if g.router.LocalDemand().WakeOneWaiter("orders", "remote.example:7942") {
				wokeAt.Store(time.Now().UnixNano())
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	res, forwarded := g.consume("5s")
	done := time.Now()
	if !forwarded || res.Code != http.StatusOK {
		t.Fatalf("forwarded=%v status=%d, want the owner's record", forwarded, res.Code)
	}
	if wokeAt.Load() == 0 {
		t.Fatal("the owner's notification found no parked consumer")
	}
	if late := done.Sub(time.Unix(0, wokeAt.Load())); late > 200*time.Millisecond {
		t.Fatalf("record delivered %s after the owner notified, want a claim at once", late)
	}
	if n := g.claims.Load(); n != 1 {
		t.Fatalf("claims = %d, want 1", n)
	}
}

// A gateway keeps polling while any owner of the topic is known not to
// speak the token protocol: a token left there would be discarded, and a
// record on that owner would wait for the consumer's whole budget.
func TestZZWP9GatewayPollsWhileAnOwnerIsLegacy(t *testing.T) {
	g := newZZWP9Gateway(t)
	g.router.tokens.noteRegisterResult("remote.example:7942",
		nodewire.Response{Status: http.StatusBadRequest, Body: []byte("unsupported rpc operation 13")}, nil)
	res, forwarded := g.consume("800ms")
	if !forwarded || res.Code != http.StatusNoContent {
		t.Fatalf("forwarded=%v status=%d, want 204", forwarded, res.Code)
	}
	if n := g.adds.Load(); n != 0 {
		t.Fatalf("registered %d tokens with a legacy owner, want polling", n)
	}
	if n := g.probes.Load(); n < 3 {
		t.Fatalf("probes = %d, want the polling fallback's re-probes", n)
	}
}

// A gateway consumer still re-probes now and then while parked, so a
// token the owner silently lost (a restart, a lost notification) costs
// at most one re-probe interval rather than the keeper's refresh.
func TestZZWP9GatewayReprobesWhileParked(t *testing.T) {
	g := newZZWP9Gateway(t)
	go func() {
		time.Sleep(300 * time.Millisecond)
		g.ready.Store(true) // a record, and no notification ever comes
	}()
	start := time.Now()
	res, forwarded := g.consume("10s")
	if !forwarded || res.Code != http.StatusOK {
		t.Fatalf("forwarded=%v status=%d, want the record found by a re-probe", forwarded, res.Code)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("record found after %s, want within a re-probe interval", elapsed)
	}
}

// Consumers parking one after another on the same topic share the token
// the first one left: the owner hears one registration, not one per
// consumer. The last one leaving does not send a drop of its own.
func TestZZWP9ParkedConsumersShareOneRegistration(t *testing.T) {
	reg := newRegistrationLog()
	router := tokenRouter(t, fakePeerClient{registerTokensFn: reg.registerTokens})
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			res := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
			router.RouteConsumeWait(context.Background(), res, req, "orders", time.Second, &fakeLocalWaiter{delay: time.Hour})
		})
		time.Sleep(15 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	if adds, _ := reg.counts("remote.example:7942/orders"); adds > 2 {
		t.Fatalf("owner received %d registrations for 10 consumers parked within 150ms, want one shared token", adds)
	}
	wg.Wait()

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	router.RouteConsumeWait(context.Background(), res, req, "orders", time.Second, &fakeLocalWaiter{
		delay: 20 * time.Millisecond, found: true,
		msg: topic.Message{Topic: "orders", Partition: 1, Offset: 3, ReceiptHandle: "1:3:5"},
	})
	time.Sleep(100 * time.Millisecond)
	if _, drops := reg.counts("remote.example:7942/orders"); drops != 0 {
		t.Fatalf("the last consumer leaving sent %d drop frames, want the token left to lapse", drops)
	}
}

// A registration older than the sharing window is sent again by the next
// consumer to park, so a token the owner silently discarded is repaired
// by ordinary traffic and not only by the keeper's refresh.
func TestZZWP9StaleRegistrationIsResent(t *testing.T) {
	reg := newRegistrationLog()
	router := tokenRouter(t, fakePeerClient{registerTokensFn: reg.registerTokens})
	park := func() {
		res := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=2s", nil)
		router.RouteConsumeWait(context.Background(), res, req, "orders", 2*time.Second, &fakeLocalWaiter{delay: time.Hour})
	}
	go park()
	time.Sleep(400 * time.Millisecond) // past the sharing window
	go park()
	time.Sleep(50 * time.Millisecond)
	if adds, _ := reg.counts("remote.example:7942/orders"); adds < 2 {
		t.Fatalf("owner received %d registrations, want the second consumer to re-send an old one", adds)
	}
}

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

// zzWP9UnassignedRouter is a router for a topic that exists but has no
// partition assigned yet, the state of a topic created moments ago.
func zzWP9UnassignedRouter(t *testing.T, peer fakePeerClient) (*Router, *metastore.Store) {
	t.Helper()
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = peer
	return router, store
}

// A long-poll on a topic with no partition assigned waits out its
// budget instead of answering 204 at once, which turned a looping
// consumer into a busy poll for as long as the window lasted.
func TestZZWP9UnassignedTopicHonoursTheWait(t *testing.T) {
	router, _ := zzWP9UnassignedRouter(t, fakePeerClient{})
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=300ms", nil)
	start := time.Now()
	forwarded, local := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if !forwarded || local != nil || res.Code != http.StatusNoContent {
		t.Fatalf("RouteConsume() = (%v, %v), status %d; want a written 204", forwarded, local, res.Code)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("answered after %s, want the 300ms budget honoured", elapsed)
	}
}

// Once the partitions are assigned, the waiting consumer is routed to
// the new owner with what is left of its budget.
func TestZZWP9UnassignedTopicRoutesOnceAssigned(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 7, Nonce: 99})
	router, store := zzWP9UnassignedRouter(t, fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		return remoteMessageResponse(0, 7, handle), nil
	}})
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = store.AssignPartition(context.Background(), "orders", 0, "node-remote")
		_ = store.AssignPartition(context.Background(), "orders", 1, "node-remote")
	}()
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	start := time.Now()
	forwarded, _ := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if !forwarded || res.Code != http.StatusOK {
		t.Fatalf("forwarded=%v status=%d, want the new owner's record", forwarded, res.Code)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("served after %s, want soon after the assignment", elapsed)
	}
}

// An unknown topic is still left to the caller at once (it answers 404).
func TestZZWP9UnknownTopicIsNotHeld(t *testing.T) {
	router := NewRouter(newTestStore(t), "node-self", partition.NewHashRoundRobin(), "")
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/nope/consume?wait=2s", nil)
	start := time.Now()
	forwarded, _ := router.RouteConsume(context.Background(), res, req, "nope", nil)
	if forwarded {
		t.Fatal("RouteConsume() handled an unknown topic, want it left to the caller")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("unknown topic held for %s", elapsed)
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

// zzWP9SlotBroker blocks acks and commits until released and counts the
// consumes, nacks and claim notes it sees.
type zzWP9SlotBroker struct {
	broker.Broker
	gate     chan struct{}
	consumes atomic.Int32
	nacks    atomic.Int32
	claims   atomic.Int32
	acks     atomic.Int32
}

func (b *zzWP9SlotBroker) Ack(context.Context, string, consumer.Handle) error {
	b.acks.Add(1)
	<-b.gate
	return nil
}

func (b *zzWP9SlotBroker) CommitAcceptedProduceBatch(context.Context, []ingress.ProduceRecord) ([]int64, error) {
	<-b.gate
	return []int64{1}, nil
}

func (b *zzWP9SlotBroker) Consume(_ context.Context, topicName string, _ brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	b.consumes.Add(1)
	return topic.Message{Topic: topicName, Partition: 0, Offset: 1, Payload: []byte(`{}`), ReceiptHandle: consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 1, Nonce: 7})}, true, nil
}

func (b *zzWP9SlotBroker) Nack(context.Context, string, consumer.Handle) error {
	b.nacks.Add(1)
	return nil
}

func (b *zzWP9SlotBroker) NoteRemoteClaim(string) { b.claims.Add(1) }

// zzWP9QueuedBehindSlot holds the only messaging slot with an ack, then
// sends req with a context cancelled while it waits, and returns the
// reply once the slot frees.
func zzWP9QueuedBehindSlot(t *testing.T, br *zzWP9SlotBroker, req nodewire.ConsumeRequest) nodewire.Response {
	t.Helper()
	s := &RPCServer{broker: br}
	s.SetMessagingConcurrency(1)
	ack, err := nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 7})
	if err != nil {
		t.Fatal(err)
	}
	s.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: ack}, func(clusterwire.StreamFrame) {})
	for br.acks.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	replies := make(chan nodewire.Response, 1)
	payload, err := nodewire.EncodeConsumeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	s.HandleStreamRequest(ctx, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 2, Payload: payload}, func(f clusterwire.StreamFrame) {
		res, _ := nodewire.DecodeResponse(append([]byte(nil), f.Payload...))
		replies <- res
	})
	time.Sleep(20 * time.Millisecond)
	cancel() // the requester's probe budget ran out while it was queued
	time.Sleep(20 * time.Millisecond)
	close(br.gate)
	select {
	case res := <-replies:
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("no reply")
		return nodewire.Response{}
	}
}

// A probe whose requester gave up while it waited for a messaging slot
// is answered without reserving, reading and releasing a record.
func TestZZWP9CancelledProbeSkipsTheBroker(t *testing.T) {
	br := &zzWP9SlotBroker{gate: make(chan struct{})}
	res := zzWP9QueuedBehindSlot(t, br, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true})
	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Status)
	}
	if n := br.consumes.Load(); n != 0 {
		t.Fatalf("a cancelled probe reserved %d records", n)
	}
}

// A claim whose requester gave up still retires the owner's hold, so the
// record goes to somebody else now, but reserves nothing.
func TestZZWP9CancelledClaimRetiresTheHoldWithoutReserving(t *testing.T) {
	br := &zzWP9SlotBroker{gate: make(chan struct{})}
	res := zzWP9QueuedBehindSlot(t, br, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true})
	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Status)
	}
	if n := br.consumes.Load(); n != 0 {
		t.Fatalf("a cancelled claim reserved %d records", n)
	}
	if n := br.claims.Load(); n != 1 {
		t.Fatalf("hold retirements = %d, want 1", n)
	}
}

// Probes and acks do not queue behind commit batches: a commit holds its
// slot across fsyncs, a probe or an ack is in-memory bookkeeping and at
// most one read.
func TestZZWP9MessagingOpsDoNotWaitBehindCommits(t *testing.T) {
	br := &zzWP9SlotBroker{gate: make(chan struct{})}
	defer close(br.gate)
	s := &RPCServer{broker: br}
	s.SetMessagingConcurrency(1)
	commit, err := nodewire.EncodeCommitProduceBatchRequest(nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{{Topic: "orders", Payload: []byte("x")}}})
	if err != nil {
		t.Fatal(err)
	}
	s.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: commit}, func(clusterwire.StreamFrame) {})
	time.Sleep(20 * time.Millisecond)
	probe, err := nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	replies := make(chan struct{}, 1)
	s.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 2, Payload: probe}, func(clusterwire.StreamFrame) { replies <- struct{}{} })
	select {
	case <-replies:
	case <-time.After(2 * time.Second):
		t.Fatal("a probe queued behind a commit batch holding the messaging slot")
	}
}
