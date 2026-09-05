package cluster

import (
	"context"
	"encoding/json"
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

func (f *fakeLocalWaiter) Wait(ctx context.Context, wait time.Duration) (topic.Message, bool, error) {
	f.mu.Lock()
	f.waitCalled = true
	f.mu.Unlock()
	timer := time.NewTimer(min(f.delay, wait))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		if f.onCancel && f.found {
			// Reserved a message just as the cancel arrived: the classic
			// loser-with-a-delivery case.
			return f.msg, true, nil
		}
		return topic.Message{}, false, ctx.Err()
	case <-timer.C:
		if f.onCancel {
			return topic.Message{}, false, nil
		}
		return f.msg, f.found, nil
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

// TestRouteConsumeWaitRemoteWinsReleasesLateLocalDelivery pins race
// order one: the remote owner delivers first; the local wait, cancelled,
// still comes back with a message it reserved in the meantime, and that
// message is given back (Release) while the remote message is served.
func TestRouteConsumeWaitRemoteWinsReleasesLateLocalDelivery(t *testing.T) {
	var nacks int
	var mu sync.Mutex
	remoteHandle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 7, Nonce: 99})
	peer := fakePeerClient{
		consumeFn: func(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
			if !req.LocalOnly || req.WaitNanos <= 0 {
				t.Errorf("forwarded wait = %+v, want local-only with a positive wait", req)
			}
			time.Sleep(20 * time.Millisecond)
			return remoteMessageResponse(0, 7, remoteHandle), nil
		},
		nackFn: func(context.Context, string, nodewire.AckRequest) (nodewire.Response, error) {
			mu.Lock()
			nacks++
			mu.Unlock()
			return nodewire.Response{Status: http.StatusNoContent}, nil
		},
	}
	router := mixedOwnerRouter(t, peer)
	local := &fakeLocalWaiter{
		delay: time.Hour, onCancel: true, found: true,
		msg: topic.Message{Topic: "orders", Partition: 1, Offset: 3, ReceiptHandle: "1:3:5"},
	}

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=2s", nil)
	if !router.RouteConsumeWait(context.Background(), res, req, "orders", 2*time.Second, local) {
		t.Fatal("RouteConsumeWait() = false, want handled")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	if got := decodeMessageBody(t, res.Body.Bytes()); got.Partition != 0 || got.Offset != 7 {
		t.Fatalf("served %+v, want the remote message (partition 0 offset 7)", got)
	}
	if local.releasedCount() != 1 || local.released[0].Offset != 3 {
		t.Fatalf("local releases = %+v, want the late local delivery (offset 3) nacked", local.released)
	}
	mu.Lock()
	defer mu.Unlock()
	if nacks != 0 {
		t.Fatalf("remote nacks = %d, want 0 (the remote message was served)", nacks)
	}
}

// TestRouteConsumeWaitLocalWinsNacksLateRemoteDelivery pins race order
// two: the local wait delivers first; the forwarded poll is cancelled,
// but its reply still arrives carrying a message, which is nacked on the
// remote owner while the local message is served.
func TestRouteConsumeWaitLocalWinsNacksLateRemoteDelivery(t *testing.T) {
	var nacked []nodewire.AckRequest
	var cancelled bool
	var mu sync.Mutex
	remoteHandle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 11, Nonce: 42})
	peer := fakePeerClient{
		consumeFn: func(ctx context.Context, _ string, _ nodewire.ConsumeRequest) (nodewire.Response, error) {
			<-ctx.Done() // the local win cancels the RPC...
			mu.Lock()
			cancelled = true
			mu.Unlock()
			// ...but a reply carrying a message had already left the owner.
			return remoteMessageResponse(0, 11, remoteHandle), nil
		},
		nackFn: func(_ context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
			mu.Lock()
			nacked = append(nacked, req)
			mu.Unlock()
			return nodewire.Response{Status: http.StatusNoContent}, nil
		},
	}
	router := mixedOwnerRouter(t, peer)
	local := &fakeLocalWaiter{
		delay: 20 * time.Millisecond, found: true,
		msg: topic.Message{Topic: "orders", Partition: 1, Offset: 3, Payload: []byte(`{"from":"local"}`), ReceiptHandle: "1:3:5"},
	}

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=2s", nil)
	if !router.RouteConsumeWait(context.Background(), res, req, "orders", 2*time.Second, local) {
		t.Fatal("RouteConsumeWait() = false, want handled")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	if got := decodeMessageBody(t, res.Body.Bytes()); got.Partition != 1 || got.Offset != 3 || got.ReceiptHandle != "1:3:5" {
		t.Fatalf("served %+v, want the local message (partition 1 offset 3)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if !cancelled {
		t.Fatal("the forwarded long-poll was not cancelled after the local win")
	}
	if len(nacked) != 1 || nacked[0].Partition != 0 || nacked[0].Offset != 11 || nacked[0].Nonce != 42 {
		t.Fatalf("remote nacks = %+v, want exactly the late remote delivery (0:11:42)", nacked)
	}
	if local.releasedCount() != 0 {
		t.Fatalf("local releases = %d, want 0", local.releasedCount())
	}
}

// TestRouteConsumeWaitRemoteEmptyThenLocalDelivers pins that an early
// empty remote reply does not end the wait: the local wait keeps running
// and its later delivery is served, with nothing given back anywhere.
func TestRouteConsumeWaitRemoteEmptyThenLocalDelivers(t *testing.T) {
	var polls int
	var mu sync.Mutex
	peer := fakePeerClient{
		consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
			mu.Lock()
			polls++
			mu.Unlock()
			return nodewire.Response{Status: http.StatusNoContent}, nil
		},
		nackFn: func(context.Context, string, nodewire.AckRequest) (nodewire.Response, error) {
			t.Error("nothing should be nacked")
			return nodewire.Response{Status: http.StatusNoContent}, nil
		},
	}
	router := mixedOwnerRouter(t, peer)
	local := &fakeLocalWaiter{
		delay: 150 * time.Millisecond, found: true,
		msg: topic.Message{Topic: "orders", Partition: 1, Offset: 5, ReceiptHandle: "1:5:9"},
	}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=2s", nil)
	if !router.RouteConsumeWait(context.Background(), res, req, "orders", 2*time.Second, local) {
		t.Fatal("RouteConsumeWait() = false, want handled")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Code)
	}
	if got := decodeMessageBody(t, res.Body.Bytes()); got.Offset != 5 {
		t.Fatalf("served %+v, want the local message (offset 5)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if polls == 0 {
		t.Fatal("the remote owner was never polled")
	}
	if local.releasedCount() != 0 {
		t.Fatal("a local delivery was released for no reason")
	}
}

// TestRouteConsumeWaitBothEmptyIsNoContent pins the timeout: neither
// side delivers within the wait, the client gets 204.
func TestRouteConsumeWaitBothEmptyIsNoContent(t *testing.T) {
	peer := fakePeerClient{
		consumeFn: func(ctx context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
			select {
			case <-ctx.Done():
			case <-time.After(time.Duration(req.WaitNanos)):
			}
			return nodewire.Response{Status: http.StatusNoContent}, nil
		},
	}
	router := mixedOwnerRouter(t, peer)
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
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("wait took %v, want about the 100ms budget", elapsed)
	}
}

// TestRouteConsumeWaitDeclinesWithoutRemoteOwners pins the fallback:
// with no remote owner to ask (single owner, or this node owns every
// partition) the router declines and the handler runs the local wait.
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

// TestRouteConsumeWaitGoroutineBound pins the per-process bound: with
// every slot taken the router declines rather than spawning another
// forwarded poll.
func TestRouteConsumeWaitGoroutineBound(t *testing.T) {
	router := mixedOwnerRouter(t, fakePeerClient{})
	remoteLongPollSlotsOnce.Do(func() {})
	if remoteLongPollSlots == nil {
		remoteLongPollSlots = make(chan struct{}, maxRemoteLongPolls)
	}
	var taken []func()
	for {
		release, ok := remoteLongPollSlot()
		if !ok {
			break
		}
		taken = append(taken, release)
	}
	defer func() {
		for _, release := range taken {
			release()
		}
	}()
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	if router.RouteConsumeWait(context.Background(), res, req, "orders", time.Second, &fakeLocalWaiter{}) {
		t.Fatal("RouteConsumeWait() = true with every slot taken, want false")
	}
}
