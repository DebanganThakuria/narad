package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
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

// budgetRecorder is a frameTransport that records the budget each call
// handed the transport and the deadline its context carried.
type budgetRecorder struct {
	budgets   []time.Duration
	deadlines []time.Time
}

func (r *budgetRecorder) RequestOnLane(ctx context.Context, addr string, lane clusterrpc.Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return r.RequestOnLaneTimeout(ctx, addr, lane, 0, frameType, payload)
}

func (r *budgetRecorder) RequestOnLaneTimeout(ctx context.Context, _ string, _ clusterrpc.Lane, timeout time.Duration, _ clusterwire.StreamFrameType, _ []byte) (clusterwire.StreamFrame, error) {
	deadline, _ := ctx.Deadline()
	r.budgets = append(r.budgets, timeout)
	r.deadlines = append(r.deadlines, deadline)
	payload, err := nodewire.EncodeResponse(nodewire.Response{Status: http.StatusNoContent})
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, Payload: payload}, err
}

// Forwarded ack, extend, and nack are each bounded by 2s, handed to the
// transport as the call's budget (it covers the dial and stream open as
// well as the reply wait). Acks are idempotent by nonce, so timing out
// and retrying is safe.
func TestRouteAckFamilyForwardsCarryDeadline(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	frames := &budgetRecorder{}
	router.peer = &PeerClient{frames: frames}
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
	if len(frames.budgets) != 3 {
		t.Fatalf("captured %d forwards, want 3", len(frames.budgets))
	}
	for i, budget := range frames.budgets {
		if budget != ackForwardTimeout {
			t.Fatalf("forward %d budget = %s, want %s", i, budget, ackForwardTimeout)
		}
	}
	// The caller's own deadline still reaches the transport, which ends
	// the call at whichever comes first.
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	want, _ := short.Deadline()
	frames.budgets, frames.deadlines = nil, nil
	router.RouteAck(short, httptest.NewRecorder(), nil, "orders", handle)
	if len(frames.deadlines) != 1 || !frames.deadlines[0].Equal(want) || frames.budgets[0] != ackForwardTimeout {
		t.Fatalf("caller deadline not passed through: deadlines %v budgets %v", frames.deadlines, frames.budgets)
	}
}

// failingFrames is a frameTransport whose every request fails with err,
// or with the caller's context error once the context has ended.
type failingFrames struct{ err error }

func (f failingFrames) RequestOnLane(ctx context.Context, addr string, lane clusterrpc.Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return f.RequestOnLaneTimeout(ctx, addr, lane, 0, frameType, payload)
}

func (f failingFrames) RequestOnLaneTimeout(ctx context.Context, _ string, _ clusterrpc.Lane, _ time.Duration, _ clusterwire.StreamFrameType, _ []byte) (clusterwire.StreamFrame, error) {
	if err := ctx.Err(); err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{}, f.err
}

// A forwarded ack's failure tells the client what it can know: 503 with
// Retry-After when the request never left this node (nothing was
// applied), 502 when it may have reached the owner, and 499 when the
// client itself went away. No body names the owner or the transport
// error.
func TestForwardedAckOutcomeFollowsTheTransportFailure(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	handle := consumer.Handle{Partition: 1, Offset: 3, Nonce: 4}
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name       string
		ctx        context.Context
		err        error
		status     int
		retryAfter string
	}{
		{"never sent", context.Background(), fmt.Errorf("%w: dial remote.example:7942: connection refused", clusterrpc.ErrNotSent), http.StatusServiceUnavailable, "1"},
		{"no reply in time", context.Background(), fmt.Errorf("cluster rpc request timed out: %w", context.DeadlineExceeded), http.StatusBadGateway, ""},
		{"stream failed after the write", context.Background(), errors.New("stream reset by remote.example:7942"), http.StatusBadGateway, ""},
		{"client went away", gone, nil, 499, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router.peer = &PeerClient{frames: failingFrames{err: tc.err}}
			for op, route := range map[string]func(context.Context, http.ResponseWriter) bool{
				"ack": func(ctx context.Context, w http.ResponseWriter) bool {
					return router.RouteAck(ctx, w, nil, "orders", handle)
				},
				"extend": func(ctx context.Context, w http.ResponseWriter) bool {
					return router.RouteExtendAck(ctx, w, nil, "orders", handle)
				},
				"nack": func(ctx context.Context, w http.ResponseWriter) bool {
					return router.RouteNack(ctx, w, nil, "orders", handle)
				},
			} {
				rec := httptest.NewRecorder()
				if !route(tc.ctx, rec) {
					t.Fatalf("%s: not forwarded", op)
				}
				if rec.Code != tc.status {
					t.Errorf("%s: status %d (%q), want %d", op, rec.Code, rec.Body.String(), tc.status)
				}
				if got := rec.Header().Get("Retry-After"); got != tc.retryAfter {
					t.Errorf("%s: Retry-After %q, want %q", op, got, tc.retryAfter)
				}
				if body := rec.Body.String(); strings.Contains(body, "remote.example") || strings.Contains(body, "deadline") || strings.Contains(body, "refused") || strings.Contains(body, "reset") {
					t.Errorf("%s: body %q names the owner or the transport error", op, body)
				}
			}
		})
	}
}

// An ack whose owner is down is answered 503 with Retry-After, so a
// client backs off instead of hammering a recovering cluster.
func TestRouteAckOwnerDownCarriesRetryAfter(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	if err := store.MarkMemberDead(context.Background(), "node-remote"); err != nil {
		t.Fatalf("MarkMemberDead() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	rec := httptest.NewRecorder()
	if !router.RouteAck(context.Background(), rec, nil, "orders", consumer.Handle{Partition: 1, Offset: 3, Nonce: 4}) {
		t.Fatal("not answered")
	}
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("status %d, Retry-After %q; want 503 with Retry-After 1", rec.Code, rec.Header().Get("Retry-After"))
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
