package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const zzWP20OwnerAddr = "remote.example:7942"

// zzWP20Peer is the owner of every partition of "orders" as a gateway's
// peer client sees it: each consume is recorded and served by a real
// engine behind an RPC server. Until the owner is opened (open, or
// the request after the first openAfter) it answers every consume 204,
// so a test can park a consumer before any record is taken. legacy
// makes it an owner on an earlier release: 1 refuses the Max field, 2
// refuses the Claim flag as well; either is refused as trailing data,
// as the real decoder refuses it.
type zzWP20Peer struct {
	fakePeerClient
	owner     *zzWP18Owner
	legacy    int
	openAfter int
	open      atomic.Bool

	mu   sync.Mutex
	reqs []nodewire.ConsumeRequest
}

func (p *zzWP20Peer) ConsumeWithin(ctx context.Context, _ string, _ time.Duration, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	p.mu.Lock()
	p.reqs = append(p.reqs, req)
	n := len(p.reqs)
	p.mu.Unlock()
	if (p.legacy >= 1 && req.Max > 1) || (p.legacy >= 2 && req.Claim) {
		return errorResponse(http.StatusBadRequest, "invalid consume request: trailing node rpc payload data"), nil
	}
	if !p.open.Load() && (p.openAfter == 0 || n <= p.openAfter) {
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}
	payload, err := nodewire.EncodeConsumeRequest(req)
	if err != nil {
		return nodewire.Response{}, err
	}
	return p.owner.server.handleConsume(ctx, requestKey{stream: 1, request: uint64(n)}, payload), nil
}

func (p *zzWP20Peer) Consume(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	return p.ConsumeWithin(ctx, addr, 0, req)
}

func (p *zzWP20Peer) requests() []nodewire.ConsumeRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]nodewire.ConsumeRequest(nil), p.reqs...)
}

// zzWP20Gateway is a router on a node that owns none of "orders" (2
// partitions, all on the owner behind peer), with the token protocol on
// when tokens is set.
func zzWP20Gateway(t *testing.T, peer *zzWP20Peer, tokens bool) *Router {
	t.Helper()
	router := newZZWP18Gateway(t, zzWP20OwnerAddr, peer, 2)
	router.consumeReprobeInterval = 20 * time.Millisecond
	if tokens {
		router.SetSelfAddr("node-gw.example:7942")
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go router.RunTokenKeeper(ctx)
	}
	return router
}

// zzWP20OpenWhenParked opens the owner and notifies the parked consumer
// once one is parked on the gateway, as an owner spending the gateway's
// token does. before runs just ahead of the notification.
func zzWP20OpenWhenParked(t *testing.T, router *Router, peer *zzWP20Peer, before func()) {
	t.Helper()
	go func() {
		for range 2000 {
			d := router.tokens.demandFor("orders")
			d.mu.Lock()
			parked := len(d.waiters) > 0
			d.mu.Unlock()
			if parked {
				if before != nil {
					before()
				}
				peer.open.Store(true)
				router.LocalDemand().WakeOneWaiter("orders", zzWP20OwnerAddr)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
}

// zzWP20RouteBatch runs a batch consume (max 10, wait 5 s) on the
// gateway and returns what RouteConsumeBatch reported and wrote.
func zzWP20RouteBatch(t *testing.T, router *Router) (batch bool, res *httptest.ResponseRecorder) {
	t.Helper()
	res = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?max=10&wait=5s", nil)
	forwarded, batch, local := router.RouteConsumeBatch(context.Background(), res, req, "orders", nil, 10)
	if !forwarded || local != nil {
		t.Fatalf("RouteConsumeBatch() forwarded=%v local=%v, want the gateway to serve it", forwarded, local)
	}
	return batch, res
}

func zzWP20Messages(t *testing.T, res *httptest.ResponseRecorder) []topic.Message {
	t.Helper()
	if res.Code != http.StatusOK {
		t.Fatalf("status %d (%s), want 200", res.Code, res.Body)
	}
	var reply struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &reply); err != nil {
		t.Fatalf("reply %q: %v", res.Body, err)
	}
	return reply.Messages
}

// TestZZWP20GatewayClaimTakesABatch checks that a batch consume parked
// on a node that owns none of the topic's partitions claims up to max
// records when an owner spends its token. The wait phase used to claim
// one record, which the handler wrapped as a batch of one, so a gateway
// consumer drained a backlog one record per round trip once it had
// parked.
func TestZZWP20GatewayClaimTakesABatch(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	owner.fill(t, 2, 3, []byte(`{"k":"v"}`))
	peer := &zzWP20Peer{owner: owner}
	router := zzWP20Gateway(t, peer, true)
	zzWP20OpenWhenParked(t, router, peer, nil)

	batch, res := zzWP20RouteBatch(t, router)
	if msgs := zzWP20Messages(t, res); !batch || len(msgs) != 6 {
		t.Fatalf("parked batch consume: batch=%v with %d records, want the owner's batch of 6", batch, len(msgs))
	}
	reqs := peer.requests()
	claim := reqs[len(reqs)-1]
	if !claim.Claim || !claim.LocalOnly || claim.WaitNanos != 0 || claim.Max != 10 {
		t.Fatalf("claim = %+v, want a non-blocking local-only claim for max 10", claim)
	}
}

// TestZZWP20GatewaySingleClaimUnchanged checks a single consume's claim
// on the same path is the request it always was: no Max on the wire.
func TestZZWP20GatewaySingleClaimUnchanged(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	owner.fill(t, 2, 3, []byte(`{"k":"v"}`))
	peer := &zzWP20Peer{owner: owner}
	router := zzWP20Gateway(t, peer, true)
	zzWP20OpenWhenParked(t, router, peer, nil)

	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	if forwarded, _ := router.RouteConsume(context.Background(), res, req, "orders", nil); !forwarded || res.Code != http.StatusOK {
		t.Fatalf("single consume: forwarded=%v status %d, want the claimed record", forwarded, res.Code)
	}
	if got := decodeMessageBody(t, res.Body.Bytes()); got.ReceiptHandle == "" {
		t.Fatalf("single consume served %+v, want one record", got)
	}
	want, err := nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range peer.requests() {
		if r.Max != 0 {
			t.Fatalf("a single consume sent %+v, want no Max", r)
		}
	}
	reqs := peer.requests()
	got, err := nodewire.EncodeConsumeRequest(reqs[len(reqs)-1])
	if err != nil || string(got) != string(want) {
		t.Fatalf("single claim on the wire = %x, want %x", got, want)
	}
}

// TestZZWP20GatewayPollTakesABatch checks the polling form of the wait
// phase (no return address for tokens): its re-probes ask for max too.
func TestZZWP20GatewayPollTakesABatch(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	owner.fill(t, 2, 3, []byte(`{"k":"v"}`))
	peer := &zzWP20Peer{owner: owner, openAfter: 1}
	router := zzWP20Gateway(t, peer, false)

	batch, res := zzWP20RouteBatch(t, router)
	if msgs := zzWP20Messages(t, res); !batch || len(msgs) != 6 {
		t.Fatalf("polling batch consume: batch=%v with %d records, want the owner's batch of 6", batch, len(msgs))
	}
	for _, r := range peer.requests() {
		if r.Max != 10 || r.Claim {
			t.Fatalf("a re-probe sent %+v, want an unclaimed probe for max 10", r)
		}
	}
}

// TestZZWP20GatewayReprobeTakesABatch checks the re-probe a consumer
// parked on tokens makes when no notification comes (a lost one, or a
// token an owner dropped) asks for max too.
func TestZZWP20GatewayReprobeTakesABatch(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	owner.fill(t, 2, 3, []byte(`{"k":"v"}`))
	peer := &zzWP20Peer{owner: owner, openAfter: 1}
	router := zzWP20Gateway(t, peer, true)

	start := time.Now()
	batch, res := zzWP20RouteBatch(t, router)
	if msgs := zzWP20Messages(t, res); !batch || len(msgs) != 6 {
		t.Fatalf("re-probed batch consume: batch=%v with %d records, want the owner's batch of 6", batch, len(msgs))
	}
	if elapsed := time.Since(start); elapsed < gatewayReprobeInterval/2 {
		t.Fatalf("served after %v, want it from the parked consumer's re-probe", elapsed)
	}
	for _, r := range peer.requests() {
		if r.Max != 10 {
			t.Fatalf("a probe sent %+v, want max 10", r)
		}
	}
}

// TestZZWP20GatewayClaimLegacyOwner checks the claim's rolling-upgrade
// fallback. An owner that predates Max refuses a claim carrying it: the
// claim goes again with the Claim flag alone, and an owner that predates
// the flag too gets a plain probe. Each refusal is remembered, the
// consumer still gets the record (one, for the handler to wrap), and
// the whole exchange stays within one claim budget.
func TestZZWP20GatewayClaimLegacyOwner(t *testing.T) {
	cases := []struct {
		name   string
		legacy int
		claims []nodewire.ConsumeRequest
	}{
		{"predates Max", 1, []nodewire.ConsumeRequest{
			{Topic: "orders", LocalOnly: true, Claim: true, Max: 10},
			{Topic: "orders", LocalOnly: true, Claim: true},
		}},
		{"predates Claim", 2, []nodewire.ConsumeRequest{
			{Topic: "orders", LocalOnly: true, Claim: true, Max: 10},
			{Topic: "orders", LocalOnly: true, Claim: true},
			{Topic: "orders", LocalOnly: true},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			owner := newZZWP18Owner(t, 2)
			owner.fill(t, 2, 3, []byte(`{"k":"v"}`))
			peer := &zzWP20Peer{owner: owner, legacy: tc.legacy}
			router := zzWP20Gateway(t, peer, true)
			var parkedAt int
			zzWP20OpenWhenParked(t, router, peer, func() {
				// The opening probe already found the owner too old for
				// Max. Let that memory lapse, as it does after its TTL,
				// so the claim meets the refusal itself.
				router.legacyBatchConsume.Delete(zzWP20OwnerAddr)
				parkedAt = len(peer.requests())
			})

			batch, res := zzWP20RouteBatch(t, router)
			if batch || res.Code != http.StatusOK {
				t.Fatalf("legacy owner: batch=%v status %d, want one record for the handler to wrap", batch, res.Code)
			}
			if got := decodeMessageBody(t, res.Body.Bytes()); got.ReceiptHandle == "" {
				t.Fatalf("legacy owner served %+v, want the claimed record", got)
			}
			claims := peer.requests()[parkedAt:]
			if len(claims) != len(tc.claims) {
				t.Fatalf("claims = %+v, want %+v", claims, tc.claims)
			}
			for i := range claims {
				if claims[i] != tc.claims[i] {
					t.Fatalf("claim %d = %+v, want %+v", i, claims[i], tc.claims[i])
				}
			}
			if !router.legacyBatchOwner(zzWP20OwnerAddr) {
				t.Fatal("the owner's refusal of Max was not remembered")
			}
			if got := router.legacyOwner(zzWP20OwnerAddr); got != (tc.legacy >= 2) {
				t.Fatalf("legacy claim owner remembered = %v, want %v", got, tc.legacy >= 2)
			}
		})
	}
}
