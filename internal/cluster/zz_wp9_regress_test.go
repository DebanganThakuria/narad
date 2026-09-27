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
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

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
