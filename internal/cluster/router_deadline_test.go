package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A remote owner that never answers a non-blocking probe must be skipped
// within the probe deadline, not the transport's 5s fallback.
func TestRouteConsumeRemoteSkipsBlockedPeerWithinProbeDeadline(t *testing.T) {
	var sawDeadline bool
	router := remoteOnlyConsumeRouter(t, func(ctx context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if !req.LocalOnly || req.WaitNanos != 0 {
			t.Fatalf("probe request = %+v, want non-blocking local-only scan", req)
		}
		_, sawDeadline = ctx.Deadline()
		<-ctx.Done() // block until the router gives up
		return nodewire.Response{}, ctx.Err()
	})
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)

	start := time.Now()
	forwarded, hadCandidates := router.RouteConsumeRemote(context.Background(), res, req, "orders")
	elapsed := time.Since(start)
	if forwarded || !hadCandidates {
		t.Fatalf("RouteConsumeRemote() = (%v, %v), want (false, true)", forwarded, hadCandidates)
	}
	if !sawDeadline {
		t.Fatal("probe context carried no deadline")
	}
	if elapsed > time.Second {
		t.Fatalf("blocked probe took %s, want ~%s", elapsed, consumeProbeTimeout)
	}
	if elapsed < consumeProbeTimeout/2 {
		t.Fatalf("probe returned after %s, before the %s deadline could have fired", elapsed, consumeProbeTimeout)
	}
}

// The pinned long-poll forward is not a probe: it must keep the wait-sized
// deadline rather than the 500ms probe clamp.
func TestRouteConsumePinnedForwardIsNotProbeClamped(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	var deadline time.Time
	router.peer = fakePeerClient{consumeFn: func(ctx context.Context, _ string, _ nodewire.ConsumeRequest) (nodewire.Response, error) {
		deadline, _ = ctx.Deadline()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	start := time.Now()
	if forwarded, _ := router.RouteConsume(context.Background(), res, req, "orders", new(1)); !forwarded {
		t.Fatal("RouteConsume() = false, want true")
	}
	if remaining := deadline.Sub(start); remaining < 5*time.Second {
		t.Fatalf("pinned forward deadline is %s away, want at least the 5s wait", remaining)
	}
}

// Forwarded ack, extend, and nack carry a 2s deadline. Acks are
// idempotent by nonce, so timing out and retrying is safe.
func TestRouteAckFamilyForwardsCarryDeadline(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")

	var deadlines []time.Duration
	capture := func(ctx context.Context, _ string, _ nodewire.AckRequest) (nodewire.Response, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("forwarded ack carries no deadline")
		}
		deadlines = append(deadlines, time.Until(deadline))
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}
	router.peer = fakePeerClient{ackFn: capture, extendAckFn: capture, nackFn: capture}
	handle := consumer.Handle{Partition: 1, Offset: 3, Nonce: 4}
	ctx := context.Background()
	for name, call := range map[string]func(http.ResponseWriter) bool{
		"ack":    func(w http.ResponseWriter) bool { return router.RouteAck(ctx, w, nil, "orders", handle) },
		"extend": func(w http.ResponseWriter) bool { return router.RouteExtendAck(ctx, w, nil, "orders", handle) },
		"nack":   func(w http.ResponseWriter) bool { return router.RouteNack(ctx, w, nil, "orders", handle) },
	} {
		rec := httptest.NewRecorder()
		if !call(rec) {
			t.Fatalf("%s: not forwarded", name)
		}
		if rec.Code != http.StatusNoContent {
			t.Fatalf("%s: status = %d", name, rec.Code)
		}
	}
	if len(deadlines) != 3 {
		t.Fatalf("captured %d deadlines, want 3", len(deadlines))
	}
	for _, remaining := range deadlines {
		if remaining <= 0 || remaining > ackForwardTimeout {
			t.Fatalf("forwarded ack deadline %s away, want within %s", remaining, ackForwardTimeout)
		}
	}
	// A shorter caller deadline wins over the ack timeout.
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	deadlines = nil
	router.RouteAck(short, httptest.NewRecorder(), nil, "orders", handle)
	if len(deadlines) != 1 || deadlines[0] > 100*time.Millisecond {
		t.Fatalf("caller deadline not honored: %v", deadlines)
	}
}

// The re-probe loop backs off between empty rounds (doubling from the
// initial interval to the cap), so an idle topic with waiting clients
// costs far fewer probes than a fixed interval would.
func TestLongPollReprobeBacksOff(t *testing.T) {
	var probes []time.Time
	router := remoteOnlyConsumeRouter(t, func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		probes = append(probes, time.Now())
		return nodewire.Response{Status: http.StatusNoContent}, nil
	})
	router.consumeReprobeInterval = 10 * time.Millisecond
	router.consumeReprobeMaxInterval = 80 * time.Millisecond
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=400ms", nil)

	forwarded, _ := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if !forwarded || res.Code != http.StatusNoContent {
		t.Fatalf("RouteConsume() = %v, status %d; want forwarded 204", forwarded, res.Code)
	}
	// Fixed 10ms pacing would take ~40 rounds in 400ms; doubling to an
	// 80ms cap takes the initial probe plus roughly 10-70-150-230-310-390.
	if n := len(probes); n < 4 || n > 12 {
		t.Fatalf("probes = %d, want backoff pacing (roughly 5-8 rounds in 400ms)", n)
	}
	// Gaps must be non-decreasing (allowing scheduler jitter) until the cap.
	var prev time.Duration
	for i := 1; i < len(probes) && i < 4; i++ {
		gap := probes[i].Sub(probes[i-1])
		if gap+5*time.Millisecond < prev {
			t.Fatalf("re-probe gap shrank: %s after %s", gap, prev)
		}
		prev = gap
	}
}

// The re-probe loop's owner list is rebuilt only when the route table's
// versions change.
func TestReprobeRemoteReusesCandidateListUntilRoutesChange(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	for _, member := range []metastore.Member{
		{ID: "node-a", Addr: "a.example:7942", Status: metastore.MemberAlive},
		{ID: "node-b", Addr: "b.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, member); err != nil {
			t.Fatalf("RegisterMember() error = %v", err)
		}
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-a"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	var probed []string
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, _ nodewire.ConsumeRequest) (nodewire.Response, error) {
		probed = append(probed, addr)
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}

	var cache remoteCandidateCache
	if forwarded, had := router.reprobeRemote(ctx, httptest.NewRecorder(), "orders", &cache); forwarded || !had {
		t.Fatalf("reprobeRemote() = (%v, %v), want (false, true)", forwarded, had)
	}
	first := cache.addrs
	if len(first) != 1 || first[0] != "a.example:7942" {
		t.Fatalf("candidates = %v, want [a.example:7942]", first)
	}
	router.reprobeRemote(ctx, httptest.NewRecorder(), "orders", &cache)
	if &cache.addrs[0] != &first[0] {
		t.Fatal("candidate list was reallocated although the route table did not change")
	}

	if err := store.AssignPartition(ctx, "orders", 1, "node-b"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router.reprobeRemote(ctx, httptest.NewRecorder(), "orders", &cache)
	if len(cache.addrs) != 2 {
		t.Fatalf("candidates after reassignment = %v, want both owners", cache.addrs)
	}
	if len(probed) != 4 {
		t.Fatalf("probes = %v, want 1+1+2", probed)
	}
}

func TestRouterSetPeerClient(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	original := router.peer
	router.SetPeerClient(nil)
	if router.peer != original {
		t.Fatal("SetPeerClient(nil) replaced the client")
	}
	shared := NewPeerClient(time.Second, "")
	defer shared.Close()
	router.SetPeerClient(shared)
	if router.peer != shared {
		t.Fatal("SetPeerClient did not install the shared client")
	}
}
