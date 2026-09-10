package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// mixedOwnerRouter builds a router on a node that owns partition 1 of
// "orders" while node-remote owns partition 0, with the given fake peer.
func mixedOwnerRouter(t *testing.T, peer fakePeerClient) *Router {
	t.Helper()
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = peer
	return router
}

// fakeLocalWaiter is a scripted local wait: it returns after delay, or
// when its context is cancelled, with the configured message; Release
// records what was given back.
type fakeLocalWaiter struct {
	delay      time.Duration
	msg        topic.Message
	found      bool
	onCancel   bool // deliver the message only if cancelled first
	mu         sync.Mutex
	released   []topic.Message
	waitCalled bool
}

func (f *fakeLocalWaiter) Wait(ctx context.Context, wait time.Duration, external <-chan struct{}) (topic.Message, bool, bool, error) {
	f.mu.Lock()
	f.waitCalled = true
	f.mu.Unlock()
	timer := time.NewTimer(min(f.delay, wait))
	defer timer.Stop()
	select {
	case <-external:
		// The cross-node half woke us; the real broker reports this the
		// same way rather than returning a local message.
		return topic.Message{}, false, true, nil
	case <-ctx.Done():
		return topic.Message{}, false, false, ctx.Err()
	case <-timer.C:
		if f.onCancel {
			return topic.Message{}, false, false, nil
		}
		return f.msg, f.found, false, nil
	}
}

func (f *fakeLocalWaiter) Release(_ context.Context, msg topic.Message) error {
	f.mu.Lock()
	f.released = append(f.released, msg)
	f.mu.Unlock()
	return nil
}

func (f *fakeLocalWaiter) releasedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.released)
}

func remoteMessageResponse(partition int, offset int64, handle string) nodewire.Response {
	msg := topic.Message{Topic: "orders", Partition: partition, Offset: offset, Payload: []byte(`{"from":"remote"}`), ReceiptHandle: handle}
	body := msg.AppendJSON(nil)
	return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: append(body, '\n')}
}

func decodeMessageBody(t *testing.T, body []byte) topic.Message {
	t.Helper()
	var msg topic.Message
	if err := json.Unmarshal(body, &msg); err != nil {
		t.Fatalf("decode body %q: %v", body, err)
	}
	return msg
}

// tokenRouter is mixedOwnerRouter with the token protocol switched on:
// a return address is what lets an owner call back, and without one the
// router declines and the caller runs the local wait alone.
func tokenRouter(t *testing.T, peer fakePeerClient) *Router {
	t.Helper()
	router := mixedOwnerRouter(t, peer)
	router.SetSelfAddr("node-self.example:7942")
	return router
}

// notifyWhenParked delivers a token notification once a consumer is
// actually parked, mimicking an owner spending a token. Polling rather
// than sleeping keeps the test honest under load: a fixed delay can fire
// before the router has parked, and the notification would be lost.
func notifyWhenParked(rt *Router, topicName, addr string) {
	go func() {
		for range 2000 {
			if rt.LocalDemand().WakeOneWaiter(topicName, addr) {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
}

// TestRouteConsumeWaitClaimsWhatAnOwnerOffers is the cross-node happy
// path: an owner spends this node's token, the parked consumer is woken
// with that owner's address, and it claims with one non-blocking
// consume aimed at exactly that node.
func TestRouteConsumeWaitClaimsWhatAnOwnerOffers(t *testing.T) {
	var claims int
	var mu sync.Mutex
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 7, Nonce: 99})
	peer := fakePeerClient{
		consumeFn: func(_ context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
			if !req.LocalOnly || req.WaitNanos != 0 {
				t.Errorf("claim = %+v, want a non-blocking local-only consume", req)
			}
			if addr != "remote.example:7942" {
				t.Errorf("claim aimed at %q, want the owner that notified us", addr)
			}
			mu.Lock()
			claims++
			mu.Unlock()
			return remoteMessageResponse(0, 7, handle), nil
		},
	}
	router := tokenRouter(t, peer)
	local := &fakeLocalWaiter{delay: time.Hour}

	notifyWhenParked(router, "orders", "remote.example:7942")

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	start := time.Now()
	if !router.RouteConsumeWait(context.Background(), res, req, "orders", 5*time.Second, local) {
		t.Fatal("RouteConsumeWait() = false, want handled")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	if got := decodeMessageBody(t, res.Body.Bytes()); got.Offset != 7 {
		t.Fatalf("served %+v, want the claimed message (offset 7)", got)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("took %v, want to return as soon as the owner offered", elapsed)
	}
	mu.Lock()
	defer mu.Unlock()
	if claims != 1 {
		t.Fatalf("made %d claims, want exactly 1", claims)
	}
}

// TestRouteConsumeWaitStaysParkedWhenTheClaimLoses pins the cheapest
// failure in the design: somebody else claimed the record first. That
// costs one round trip and nothing else, because a notification reserves
// nothing. The consumer must stay parked rather than answering 204.
func TestRouteConsumeWaitStaysParkedWhenTheClaimLoses(t *testing.T) {
	var mu sync.Mutex
	var claims int
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 11, Nonce: 5})
	peer := fakePeerClient{
		consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
			mu.Lock()
			claims++
			n := claims
			mu.Unlock()
			if n == 1 {
				// Beaten to it.
				return nodewire.Response{Status: http.StatusNoContent}, nil
			}
			return remoteMessageResponse(0, 11, handle), nil
		},
	}
	router := tokenRouter(t, peer)
	local := &fakeLocalWaiter{delay: time.Hour}

	// Two offers: the first claim loses the race, the second wins.
	notifyWhenParked(router, "orders", "remote.example:7942")
	notifyWhenParked(router, "orders", "remote.example:7942")

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	if !router.RouteConsumeWait(context.Background(), res, req, "orders", 5*time.Second, local) {
		t.Fatal("RouteConsumeWait() = false, want handled")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: a lost claim must not end the wait", res.Code)
	}
	if got := decodeMessageBody(t, res.Body.Bytes()); got.Offset != 11 {
		t.Fatalf("served %+v, want the second offer (offset 11)", got)
	}
}

// TestRouteConsumeWaitLocalWinsAndRetiresTokens pins the drop: when the
// local partitions serve the consumer, the tokens left with remote
// owners are retired so they do not waste a notification on somebody who
// has already been served.
func TestRouteConsumeWaitLocalWinsAndRetiresTokens(t *testing.T) {
	dropped := make(chan string, 4)
	peer := fakePeerClient{
		registerTokensFn: func(_ context.Context, addr string, delta nodewire.TokenDelta) (nodewire.Response, error) {
			for _, topicName := range delta.Drop {
				dropped <- addr + "/" + topicName
			}
			return nodewire.Response{Status: 204}, nil
		},
		consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
			t.Error("no claim should happen: the local side served this consumer")
			return nodewire.Response{Status: http.StatusNoContent}, nil
		},
	}
	router := tokenRouter(t, peer)
	local := &fakeLocalWaiter{
		delay: 40 * time.Millisecond, found: true,
		msg: topic.Message{Topic: "orders", Partition: 1, Offset: 3, ReceiptHandle: "1:3:5"},
	}

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	if !router.RouteConsumeWait(context.Background(), res, req, "orders", 5*time.Second, local) {
		t.Fatal("RouteConsumeWait() = false, want handled")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	if got := decodeMessageBody(t, res.Body.Bytes()); got.Offset != 3 {
		t.Fatalf("served %+v, want the local message (offset 3)", got)
	}
	select {
	case got := <-dropped:
		if got != "remote.example:7942/orders" {
			t.Fatalf("dropped %q, want the token at the remote owner", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the token at the remote owner was never retired")
	}
}

// TestRouteConsumeWaitRegistersWithEveryOwnerAtOnce pins the parallel
// registration: a consumer must not pay a round trip per owner before it
// is even parked.
func TestRouteConsumeWaitRegistersWithEveryOwnerAtOnce(t *testing.T) {
	var mu sync.Mutex
	registered := map[string]int{}
	peer := fakePeerClient{
		registerTokensFn: func(_ context.Context, addr string, delta nodewire.TokenDelta) (nodewire.Response, error) {
			if len(delta.Add) > 0 {
				if delta.From == "" {
					t.Error("registration carried no return address; the owner could never call back")
				}
				if delta.Add[0].TTLNanos <= 0 {
					t.Errorf("registration TTL = %d, want the remaining budget as a duration", delta.Add[0].TTLNanos)
				}
				mu.Lock()
				registered[addr]++
				mu.Unlock()
			}
			return nodewire.Response{Status: 204}, nil
		},
	}
	router := multiOwnerRouter(t, peer, 3)
	router.SetSelfAddr("node-self.example:7942")

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=80ms", nil)
	router.RouteConsumeWait(context.Background(), res, req, "orders", 80*time.Millisecond,
		&fakeLocalWaiter{delay: time.Hour})

	mu.Lock()
	defer mu.Unlock()
	if len(registered) != 3 {
		t.Fatalf("registered with %v, want a token at all three owners", registered)
	}
}

// TestRouteConsumeWaitBothEmptyIsNoContent pins the budget: nobody
// offers anything, so the client gets 204 without waiting longer than it
// asked for.
func TestRouteConsumeWaitBothEmptyIsNoContent(t *testing.T) {
	router := tokenRouter(t, fakePeerClient{})
	local := &fakeLocalWaiter{delay: time.Hour}

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=100ms", nil)
	start := time.Now()
	if !router.RouteConsumeWait(context.Background(), res, req, "orders", 100*time.Millisecond, local) {
		t.Fatal("RouteConsumeWait() = false, want handled")
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Code)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("wait took %v, want about the 100ms budget", elapsed)
	}
}

// TestRouteConsumeWaitDeclinesWithoutAReturnAddress pins the fallback: a
// node that cannot be called back must not leave tokens it can never
// have spent, so it declines and the caller runs the local wait alone.
func TestRouteConsumeWaitDeclinesWithoutAReturnAddress(t *testing.T) {
	router := mixedOwnerRouter(t, fakePeerClient{}) // no SetSelfAddr
	local := &fakeLocalWaiter{}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	if router.RouteConsumeWait(context.Background(), res, req, "orders", time.Second, local) {
		t.Fatal("RouteConsumeWait() = true without a return address, want false")
	}
	if local.waitCalled {
		t.Fatal("the router ran the local wait itself after declining")
	}
}

// TestRouteConsumeWaitDeclinesWithoutRemoteOwners pins the other
// fallback: this node owns every partition, so there is nobody to leave
// a token with.
func TestRouteConsumeWaitDeclinesWithoutRemoteOwners(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.SetSelfAddr("node-self.example:7942")
	local := &fakeLocalWaiter{}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	if router.RouteConsumeWait(context.Background(), res, req, "orders", time.Second, local) {
		t.Fatal("RouteConsumeWait() = true with no remote owners, want false")
	}
	if local.waitCalled {
		t.Fatal("the router ran the local wait itself after declining")
	}
}

// multiOwnerRouter builds a router on a node that owns partition 0 of
// "orders" while remotes own the rest, one partition per remote node.
func multiOwnerRouter(t *testing.T, peer fakePeerClient, remotes int) *Router {
	t.Helper()
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: remotes + 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	for i := 1; i <= remotes; i++ {
		id := fmt.Sprintf("node-remote-%d", i)
		addr := fmt.Sprintf("remote-%d.example:7942", i)
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: addr, Status: metastore.MemberAlive}); err != nil {
			t.Fatalf("RegisterMember(%s) error = %v", id, err)
		}
		if err := store.AssignPartition(ctx, "orders", i, id); err != nil {
			t.Fatalf("AssignPartition(%d) error = %v", i, err)
		}
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = peer
	return router
}

// TestClaimFromFallsBackToAPlainProbeForALegacyOwner pins the rolling-
// upgrade path: an owner that refuses the Claim flag (400) is retried at
// once with a plain probe and remembered, later claims skip the flag
// until the TTL passes, and after it the flag is tried again.
func TestClaimFromFallsBackToAPlainProbeForALegacyOwner(t *testing.T) {
	var mu sync.Mutex
	var seen []bool // Claim flag of each request, in order
	acceptsFlag := false
	peer := fakePeerClient{
		consumeFn: func(_ context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
			mu.Lock()
			seen = append(seen, req.Claim)
			accepts := acceptsFlag
			mu.Unlock()
			if req.Claim && !accepts {
				// What an owner on the previous release answers.
				return nodewire.Response{Status: http.StatusBadRequest, Body: []byte("invalid consume request: trailing node rpc payload data")}, nil
			}
			return remoteMessageResponse(0, 7, consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 7, Nonce: 99})), nil
		},
	}
	router := tokenRouter(t, peer)
	const owner = "old.example:7942"

	if _, ok := router.claimFrom(context.Background(), owner, "orders"); !ok {
		t.Fatal("first claim: the plain fallback should have succeeded")
	}
	if _, ok := router.claimFrom(context.Background(), owner, "orders"); !ok {
		t.Fatal("second claim: the remembered owner should be claimed unflagged")
	}
	mu.Lock()
	got := append([]bool(nil), seen...)
	mu.Unlock()
	if want := []bool{true, false, false}; len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("Claim flags sent = %v, want %v (flagged, refused, then unflagged; then unflagged from the cache)", got, want)
	}

	// The TTL passes and the owner has been upgraded: the flag is tried
	// again and sticks.
	router.legacyClaim.Store(owner, time.Now().Add(-time.Second))
	mu.Lock()
	acceptsFlag = true
	mu.Unlock()
	if _, ok := router.claimFrom(context.Background(), owner, "orders"); !ok {
		t.Fatal("claim after the TTL: the flagged claim should have succeeded")
	}
	mu.Lock()
	last := seen[len(seen)-1]
	mu.Unlock()
	if !last {
		t.Fatal("after the TTL the claim went out unflagged: the legacy entry never expires")
	}
	if _, still := router.legacyClaim.Load(owner); still {
		t.Fatal("an expired legacy entry was kept")
	}
}
