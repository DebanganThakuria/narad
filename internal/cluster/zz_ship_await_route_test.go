package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A partition that becomes this node's while a long-poll waits for a
// route is answered 204 at once, so the client's next poll takes the
// local path with its full wait, not a remote owner's.
func TestAwaitConsumeRouteTurnsLocal(t *testing.T) {
	var calls atomic.Int32
	router, store := zzWP9UnassignedRouter(t, fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		calls.Add(1)
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}})
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = store.AssignPartition(ctx, "orders", 0, "node-self")
	}()
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	start := time.Now()
	forwarded, local := router.RouteConsume(ctx, res, req, "orders", nil)
	if !forwarded || local != nil || res.Code != http.StatusNoContent {
		t.Fatalf("RouteConsume() = (%v, %v), status %d; want a written 204 so the next poll goes local", forwarded, local, res.Code)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("answered after %s, want soon after the local assignment", elapsed)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("%d remote consumes for a partition that became this node's", n)
	}
}

// An owner that comes back while a long-poll waits (a members change,
// not an assignment change) gets the consumer, with what is left of
// its budget.
func TestAwaitConsumeRouteOwnerComesBack(t *testing.T) {
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 7, Nonce: 99})
	router, store := zzWP9UnassignedRouter(t, fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		return remoteMessageResponse(0, 7, handle), nil
	}})
	ctx := context.Background()
	for p := range 2 {
		if err := store.AssignPartition(ctx, "orders", p, "node-remote"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkMemberDead(ctx, "node-remote"); err != nil {
		t.Fatal(err)
	}
	var revived atomic.Int64
	go func() {
		time.Sleep(200 * time.Millisecond)
		revived.Store(time.Now().UnixNano())
		_ = store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive})
	}()
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	forwarded, _ := router.RouteConsume(ctx, res, req, "orders", nil)
	if !forwarded || res.Code != http.StatusOK {
		t.Fatalf("forwarded=%v status=%d, want the revived owner's record", forwarded, res.Code)
	}
	at := revived.Load()
	if at == 0 {
		t.Fatal("served before the owner came back")
	}
	if after := time.Since(time.Unix(0, at)); after > 2*time.Second {
		t.Fatalf("served %s after the owner came back, want soon after", after)
	}
	if msg := decodeMessageBody(t, res.Body.Bytes()); msg.Offset != 7 {
		t.Fatalf("served offset %d, want the owner's record at 7", msg.Offset)
	}
}

// A batch long-poll (?max=N) held for a route carries max to the owner
// it is routed to once one is assigned, and reports the batch reply.
func TestAwaitConsumeRouteBatchCarriesMax(t *testing.T) {
	var gotMax atomic.Int64
	handle := consumer.EncodeHandle(consumer.Handle{Partition: 0, Offset: 7, Nonce: 99})
	router, store := zzWP9UnassignedRouter(t, fakePeerClient{consumeFn: func(_ context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		gotMax.Store(int64(req.Max))
		msg := topic.Message{Topic: "orders", Partition: 0, Offset: 7, Payload: []byte(`{"from":"remote"}`), ReceiptHandle: handle}
		body := append([]byte(`{"messages":[`), msg.AppendJSON(nil)...)
		return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: append(body, "]}\n"...)}, nil
	}})
	ctx := context.Background()
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = store.AssignPartition(ctx, "orders", 0, "node-remote")
		_ = store.AssignPartition(ctx, "orders", 1, "node-remote")
	}()
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?max=10&wait=5s", nil)
	start := time.Now()
	forwarded, batch, local := router.RouteConsumeBatch(ctx, res, req, "orders", nil, 10)
	if !forwarded || !batch || local != nil || res.Code != http.StatusOK {
		t.Fatalf("RouteConsumeBatch() = (%v, %v, %v), status %d; want the owner's batch", forwarded, batch, local, res.Code)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("served after %s, want soon after the assignment", elapsed)
	}
	if n := gotMax.Load(); n != 10 {
		t.Fatalf("forwarded consume carried Max %d, want 10", n)
	}
	msgs := zzWP18Batch(t, nodewire.Response{Status: res.Code, Body: res.Body.Bytes()})
	if len(msgs) != 1 || msgs[0].Offset != 7 {
		t.Fatalf("batch reply = %+v, want the owner's record", msgs)
	}
}

// A consumer parked on tokens at a node that owns none of the topic's
// partitions re-probes the owners every gatewayReprobeInterval. If
// ownership moved away from every remote owner meanwhile, it leaves the
// queue and answers 204, so the next poll routes afresh, rather than
// waiting out its budget on owners that no longer have the topic.
func TestGatewayReprobeWithNoOwnersLeft(t *testing.T) {
	var probes atomic.Int32
	router, store := zzWP9UnassignedRouter(t, fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		probes.Add(1)
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}})
	router.SetSelfAddr("node-self.example:7942")
	keeperCtx, stopKeeper := context.WithCancel(context.Background())
	defer stopKeeper()
	go router.RunTokenKeeper(keeperCtx)
	ctx := context.Background()
	for p := range 2 {
		if err := store.AssignPartition(ctx, "orders", p, "node-remote"); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		time.Sleep(200 * time.Millisecond)
		for p := range 2 {
			_ = store.AssignPartition(ctx, "orders", p, "node-self")
		}
	}()
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=10s", nil)
	start := time.Now()
	forwarded, local := router.RouteConsume(ctx, res, req, "orders", nil)
	elapsed := time.Since(start)
	if !forwarded || local != nil || res.Code != http.StatusNoContent || res.Body.Len() != 0 {
		t.Fatalf("RouteConsume() = (%v, %v), %d %q; want a bare 204", forwarded, local, res.Code, res.Body)
	}
	if elapsed < gatewayReprobeInterval/2 || elapsed > gatewayReprobeInterval+3*time.Second {
		t.Fatalf("answered after %s, want at the first re-probe (%s), not the 10s budget", elapsed, gatewayReprobeInterval)
	}
	if n := probes.Load(); n != 1 {
		t.Fatalf("%d probes, want only the opening one: the re-probe found no owner to ask", n)
	}
	if router.LocalDemand().WakeOneWaiter("orders", "remote.example:7942") {
		t.Fatal("the consumer that answered was still parked")
	}
}
