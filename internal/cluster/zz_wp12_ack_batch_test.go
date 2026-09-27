package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Owners in zzWP12Router's cluster.
const (
	zzWP12AddrA    = "a.example:7942"
	zzWP12AddrB    = "b.example:7942"
	zzWP12AddrDown = "down.example:7942"
	// zzWP12StaleNonce marks a handle the fake owners answer 410 for.
	zzWP12StaleNonce = 13
)

// zzWP12Router builds a router on node-self for "orders" with five
// partitions: 0 and 4 on node-a, 1 on node-b, 2 local, 3 on a dead node.
func zzWP12Router(tb testing.TB, peer peerClient) *Router {
	tb.Helper()
	store := zzWP9Store(tb)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 5}); err != nil {
		tb.Fatal(err)
	}
	for _, m := range []metastore.Member{
		{ID: "node-a", Addr: zzWP12AddrA, Status: metastore.MemberAlive},
		{ID: "node-b", Addr: zzWP12AddrB, Status: metastore.MemberAlive},
		{ID: "node-down", Addr: zzWP12AddrDown, Status: metastore.MemberDead},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			tb.Fatal(err)
		}
	}
	for p, owner := range []string{"node-a", "node-b", "node-self", "node-down", "node-a"} {
		if err := store.AssignPartition(ctx, "orders", p, owner); err != nil {
			tb.Fatal(err)
		}
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = peer
	return router
}

// zzWP12Call is one ack-shaped RPC the fake peer saw.
type zzWP12Call struct {
	addr  string
	items []nodewire.AckBatchItem
	batch bool
}

// zzWP12Peer is a peer client whose owners answer acks the way a real
// owner would: 204, or 410 with the owner's error body for a handle with
// zzWP12StaleNonce. It records every ack-shaped RPC.
type zzWP12Peer struct {
	fakePeerClient
	mu    sync.Mutex
	calls []zzWP12Call
	// single, when set, answers single ack-shaped RPCs instead.
	single func(ctx context.Context, addr string, item nodewire.AckBatchItem) (nodewire.Response, error)
	// batch, when set, answers OpAckBatch instead.
	batch func(ctx context.Context, addr string, req nodewire.AckBatchRequest) (nodewire.Response, error)
}

// zzWP12Owner is a fake owner's answer to one record.
func zzWP12Owner(item nodewire.AckBatchItem) nodewire.Response {
	if item.Nonce == zzWP12StaleNonce {
		return errorResponse(http.StatusGone, "receipt handle is stale: reservation lapsed or superseded")
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (p *zzWP12Peer) record(c zzWP12Call) {
	p.mu.Lock()
	p.calls = append(p.calls, c)
	p.mu.Unlock()
}

func (p *zzWP12Peer) snapshot() []zzWP12Call {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.calls)
}

func (p *zzWP12Peer) singleAck(ctx context.Context, addr string, mode nodewire.AckMode, req nodewire.AckRequest) (nodewire.Response, error) {
	item := nodewire.AckBatchItem{Topic: req.Topic, Partition: req.Partition, Offset: req.Offset, Nonce: req.Nonce, Mode: mode}
	p.record(zzWP12Call{addr: addr, items: []nodewire.AckBatchItem{item}})
	if p.single != nil {
		return p.single(ctx, addr, item)
	}
	return zzWP12Owner(item), nil
}

func (p *zzWP12Peer) AckWithin(ctx context.Context, addr string, _ time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	return p.singleAck(ctx, addr, nodewire.AckModeAck, req)
}

func (p *zzWP12Peer) ExtendAckWithin(ctx context.Context, addr string, _ time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	return p.singleAck(ctx, addr, nodewire.AckModeExtend, req)
}

func (p *zzWP12Peer) NackWithin(ctx context.Context, addr string, _ time.Duration, req nodewire.AckRequest) (nodewire.Response, error) {
	return p.singleAck(ctx, addr, nodewire.AckModeNack, req)
}

func (p *zzWP12Peer) AckBatchWithin(ctx context.Context, addr string, _ time.Duration, req nodewire.AckBatchRequest) (nodewire.Response, error) {
	p.record(zzWP12Call{addr: addr, items: slices.Clone(req.Items), batch: true})
	if p.batch != nil {
		return p.batch(ctx, addr, req)
	}
	return zzWP12BatchReply(req, zzWP12Owner), nil
}

// zzWP12BatchReply is what a real owner whose single answers are owner
// answers an OpAckBatch with.
func zzWP12BatchReply(req nodewire.AckBatchRequest, owner func(nodewire.AckBatchItem) nodewire.Response) nodewire.Response {
	results := make([]nodewire.AckResult, len(req.Items))
	for i, item := range req.Items {
		res := owner(item)
		results[i].Status = res.Status
		if res.Status >= http.StatusMultipleChoices {
			results[i].Error = errorBodyText(res.ContentType, res.Body)
		}
	}
	body, err := nodewire.AppendAckBatchReply(nil, results)
	if err != nil {
		panic(err)
	}
	return nodewire.Response{Status: http.StatusOK, ContentType: "application/octet-stream", Body: body}
}

func zzWP12Unsupported() nodewire.Response {
	return errorResponse(http.StatusBadRequest, fmt.Sprintf("unsupported rpc operation %d", nodewire.OpAckBatch))
}

func zzWP12Handles(nonces ...int64) []consumer.Handle {
	// Partition i%5, so the handles spread over every kind of owner.
	out := make([]consumer.Handle, len(nonces))
	for i, n := range nonces {
		out[i] = consumer.Handle{Partition: i % 5, Offset: int64(100 + i), Nonce: n}
	}
	return out
}

// ---- RouteAckBatch ---------------------------------------------------------

// One RPC per owner; a handle this node owns is left to the caller; a
// dead owner's handle is 503; each record keeps its own outcome; an
// owner with a single record gets a plain OpAck.
func TestZZWP12RouteAckBatchGroupsByOwner(t *testing.T) {
	peer := &zzWP12Peer{}
	router := zzWP12Router(t, peer)
	// Partitions 0..4, then 0 again with a stale handle.
	handles := zzWP12Handles(1, 2, 3, 4, 5, zzWP12StaleNonce)
	statuses := make([]int, len(handles))
	msgs := make([]string, len(handles))
	router.RouteAckBatch(context.Background(), "orders", "ack", handles, statuses, msgs)

	want := []int{http.StatusNoContent, http.StatusNoContent, 0, http.StatusServiceUnavailable, http.StatusNoContent, http.StatusGone}
	if !slices.Equal(statuses, want) {
		t.Fatalf("statuses = %v, want %v", statuses, want)
	}
	if msgs[3] != ownerDownMessage || msgs[5] == "" || msgs[0] != "" {
		t.Fatalf("msgs = %q", msgs)
	}
	calls := peer.snapshot()
	var batches, singles int
	for _, c := range calls {
		switch {
		case c.batch && c.addr == zzWP12AddrA && len(c.items) == 3:
			batches++
		case !c.batch && c.addr == zzWP12AddrB && len(c.items) == 1:
			singles++
		default:
			t.Fatalf("unexpected call %+v", c)
		}
	}
	if batches != 1 || singles != 1 {
		t.Fatalf("calls = %+v; want one batch of 3 to node-a and one single to node-b", calls)
	}
}

// The stale record's message is the one a single forwarded ack of the
// same handle carries, so a client sees the same words either way.
func TestZZWP12RouteAckBatchMatchesSingleOutcome(t *testing.T) {
	peer := &zzWP12Peer{}
	router := zzWP12Router(t, peer)
	h := consumer.Handle{Partition: 0, Offset: 5, Nonce: zzWP12StaleNonce}
	w := httptest.NewRecorder()
	if !router.RouteAck(context.Background(), w, nil, "orders", h) {
		t.Fatal("RouteAck() did not forward")
	}
	single := errorBodyText(w.Header().Get("Content-Type"), w.Body.Bytes())

	handles := []consumer.Handle{h, {Partition: 4, Offset: 6, Nonce: 1}}
	statuses, msgs := make([]int, 2), make([]string, 2)
	router.RouteAckBatch(context.Background(), "orders", "ack", handles, statuses, msgs)
	if statuses[0] != w.Code || msgs[0] != single {
		t.Fatalf("batch outcome = %d %q, single = %d %q", statuses[0], msgs[0], w.Code, single)
	}
}

// One owner failing costs only its own records.
func TestZZWP12RouteAckBatchOwnerFailureIsolated(t *testing.T) {
	peer := &zzWP12Peer{
		batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
			return nodewire.Response{}, errors.New("peer reset")
		},
		single: func(_ context.Context, addr string, item nodewire.AckBatchItem) (nodewire.Response, error) {
			if addr == zzWP12AddrA {
				return nodewire.Response{}, errors.New("peer reset")
			}
			return zzWP12Owner(item), nil
		},
	}
	router := zzWP12Router(t, peer)
	// Two records for node-a (a batch), one for node-b (a single).
	handles := []consumer.Handle{{Partition: 0, Nonce: 1}, {Partition: 4, Nonce: 2}, {Partition: 1, Nonce: 3}}
	statuses, msgs := make([]int, 3), make([]string, 3)
	router.RouteAckBatch(context.Background(), "orders", "ack", handles, statuses, msgs)
	want := []int{http.StatusBadGateway, http.StatusBadGateway, http.StatusNoContent}
	if !slices.Equal(statuses, want) {
		t.Fatalf("statuses = %v, want %v", statuses, want)
	}
	if msgs[0] != "peer reset" {
		t.Fatalf("msgs[0] = %q, want the transport error", msgs[0])
	}
}

// An owner on a release before OpAckBatch refuses it: its records go
// out one at a time, and it is remembered, so the next batch skips the
// refused round trip.
func TestZZWP12RouteAckBatchLegacyOwnerFallsBack(t *testing.T) {
	peer := &zzWP12Peer{
		batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
			return zzWP12Unsupported(), nil
		},
	}
	router := zzWP12Router(t, peer)
	handles := []consumer.Handle{{Partition: 0, Nonce: 1}, {Partition: 4, Nonce: zzWP12StaleNonce}}
	for round := range 2 {
		statuses, msgs := make([]int, 2), make([]string, 2)
		router.RouteAckBatch(context.Background(), "orders", "nack", handles, statuses, msgs)
		if !slices.Equal(statuses, []int{http.StatusNoContent, http.StatusGone}) || msgs[1] == "" {
			t.Fatalf("round %d: statuses = %v msgs = %q", round, statuses, msgs)
		}
	}
	var batches, singles int
	for _, c := range peer.snapshot() {
		if c.batch {
			batches++
			continue
		}
		singles++
		if c.items[0].Mode != nodewire.AckModeNack {
			t.Fatalf("fallback single carried mode %d, want nack", c.items[0].Mode)
		}
	}
	if batches != 1 || singles != 4 {
		t.Fatalf("batches = %d, singles = %d; want the one refused batch and 4 singles", batches, singles)
	}
	// Once the TTL passes the owner is asked again.
	router.acks.legacy.peers.Store(zzWP12AddrA, time.Now().Add(-time.Second))
	if router.acks.legacy.is(zzWP12AddrA) || router.acks.legacy.count.Load() != 0 {
		t.Fatal("an expired legacy entry still counts")
	}
}

// Handles the caller already answered (a malformed one) are not sent.
func TestZZWP12RouteAckBatchSkipsAnsweredHandles(t *testing.T) {
	peer := &zzWP12Peer{}
	router := zzWP12Router(t, peer)
	handles := []consumer.Handle{{Partition: 0, Nonce: 1}, {Partition: 0, Nonce: 2}}
	statuses, msgs := []int{http.StatusBadRequest, 0}, []string{"invalid receipt handle", ""}
	router.RouteAckBatch(context.Background(), "orders", "ack", handles, statuses, msgs)
	if statuses[0] != http.StatusBadRequest || msgs[0] != "invalid receipt handle" || statuses[1] != http.StatusNoContent {
		t.Fatalf("statuses = %v msgs = %q", statuses, msgs)
	}
	if calls := peer.snapshot(); len(calls) != 1 || calls[0].batch || calls[0].items[0].Nonce != 2 {
		t.Fatalf("calls = %+v; want one single for the second handle", calls)
	}
}

// ---- owner side ------------------------------------------------------------

// zzWP12Broker answers ack-shaped calls by nonce: 5 is stale, 6 names a
// partition this node does not own, anything else applies.
type zzWP12Broker struct {
	broker.Broker
	mu    sync.Mutex
	calls []string
}

func (b *zzWP12Broker) apply(op string, h consumer.Handle) error {
	b.mu.Lock()
	b.calls = append(b.calls, fmt.Sprintf("%s:%d", op, h.Nonce))
	b.mu.Unlock()
	switch h.Nonce {
	case 5:
		return fmt.Errorf("%w: reservation lapsed", errs.ErrHandleStale)
	case 6:
		return errs.ErrNotPartitionOwner
	}
	return nil
}

func (b *zzWP12Broker) Ack(_ context.Context, _ string, h consumer.Handle) error {
	return b.apply("ack", h)
}

func (b *zzWP12Broker) ExtendAck(_ context.Context, _ string, h consumer.Handle) error {
	return b.apply("extend", h)
}

func (b *zzWP12Broker) Nack(_ context.Context, _ string, h consumer.Handle) error {
	return b.apply("nack", h)
}

func zzWP12Serve(s *RPCServer, ctx context.Context, payload []byte) nodewire.Response {
	done := make(chan nodewire.Response, 1)
	s.HandleStreamRequest(ctx, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}, func(f clusterwire.StreamFrame) {
		res, err := nodewire.DecodeResponse(f.Payload)
		if err != nil {
			panic(err)
		}
		// The reply buffer is recycled once respond returns.
		res.Body = slices.Clone(res.Body)
		done <- res
	})
	return <-done
}

// The owner applies each record of a batch with its own mode and
// answers each with the status and message the single op would have.
func TestZZWP12ServerAckBatchPerRecord(t *testing.T) {
	br := &zzWP12Broker{}
	s := NewRPCServer(br, nil, nil)
	items := []nodewire.AckBatchItem{
		{Topic: "orders", Partition: 0, Offset: 1, Nonce: 1, Mode: nodewire.AckModeAck},
		{Topic: "orders", Partition: 0, Offset: 2, Nonce: 5, Mode: nodewire.AckModeAck},
		{Topic: "orders", Partition: 1, Offset: 3, Nonce: 6, Mode: nodewire.AckModeExtend},
		{Topic: "orders", Partition: 2, Offset: 4, Nonce: 1, Mode: nodewire.AckModeNack},
	}
	payload, err := nodewire.EncodeAckBatchRequest(nodewire.AckBatchRequest{Items: items})
	if err != nil {
		t.Fatal(err)
	}
	res := zzWP12Serve(s, context.Background(), payload)
	if res.Status != http.StatusOK {
		t.Fatalf("batch status = %d %s", res.Status, res.Body)
	}
	results, err := nodewire.DecodeAckBatchReply(res.Body, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, item := range items {
		single, err := map[nodewire.AckMode]func(nodewire.AckRequest) ([]byte, error){
			nodewire.AckModeAck:    nodewire.EncodeAckRequest,
			nodewire.AckModeExtend: nodewire.EncodeExtendAckRequest,
			nodewire.AckModeNack:   nodewire.EncodeNackRequest,
		}[item.Mode](nodewire.AckRequest{Topic: item.Topic, Partition: item.Partition, Offset: item.Offset, Nonce: item.Nonce})
		if err != nil {
			t.Fatal(err)
		}
		want := zzWP12Serve(s, context.Background(), single)
		got := ackResultResponse(results[i])
		if got.Status != want.Status || string(got.Body) != string(want.Body) || got.ContentType != want.ContentType {
			t.Fatalf("record %d: batch gives %d %q %q, single gives %d %q %q", i,
				got.Status, got.ContentType, got.Body, want.Status, want.ContentType, want.Body)
		}
	}
	wantCalls := []string{"ack:1", "ack:5", "extend:6", "nack:1"}
	if !slices.Equal(br.calls[:4], wantCalls) {
		t.Fatalf("broker calls = %v, want %v first", br.calls, wantCalls)
	}
}

// A batch whose requester gave up while it waited for a messaging slot
// is answered 503 unapplied, and a malformed one 400.
func TestZZWP12ServerAckBatchRefusals(t *testing.T) {
	br := &zzWP12Broker{}
	s := NewRPCServer(br, nil, nil)
	s.SetMessagingConcurrency(1)
	s.messagingSem <- struct{}{} // every slot taken
	payload, err := nodewire.EncodeAckBatchRequest(nodewire.AckBatchRequest{Items: []nodewire.AckBatchItem{{Topic: "orders", Nonce: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if res := zzWP12Serve(s, ctx, payload); res.Status != http.StatusServiceUnavailable {
		t.Fatalf("cancelled batch: status %d, want 503", res.Status)
	}
	if len(br.calls) != 0 {
		t.Fatalf("a cancelled batch reached the broker: %v", br.calls)
	}
	<-s.messagingSem
	if res := zzWP12Serve(s, context.Background(), payload[:len(payload)-3]); res.Status != http.StatusBadRequest {
		t.Fatalf("truncated batch: status %d, want 400", res.Status)
	}
}

// End to end over real QUIC: the client's batch, the owner's per-record
// answers, and the router's coalesced acks all agree.
func TestZZWP12AckBatchOverQUIC(t *testing.T) {
	br := &zzWP12Broker{}
	addr, client := zzWP9QUICPeer(t, br)
	res, err := client.AckBatchWithin(context.Background(), addr, 2*time.Second, nodewire.AckBatchRequest{Items: []nodewire.AckBatchItem{
		{Topic: "orders", Nonce: 1}, {Topic: "orders", Nonce: 5}, {Topic: "orders", Nonce: 6, Mode: nodewire.AckModeNack},
	}})
	if err != nil || res.Status != http.StatusOK {
		t.Fatalf("AckBatchWithin() = %d, %v", res.Status, err)
	}
	results, err := nodewire.DecodeAckBatchReply(res.Body, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := []int{results[0].Status, results[1].Status, results[2].Status}
	if !slices.Equal(got, []int{http.StatusNoContent, http.StatusGone, http.StatusMisdirectedRequest}) {
		t.Fatalf("statuses = %v", got)
	}

	router := zzWP9Router(t, addr, client)
	var wg sync.WaitGroup
	var bad atomic.Int32
	for range 64 {
		wg.Go(func() {
			for range 50 {
				w := &zzWP9Writer{h: make(http.Header)}
				router.RouteAck(context.Background(), w, nil, "orders", consumer.Handle{Partition: 0, Offset: 1, Nonce: 1})
				if w.status != http.StatusNoContent {
					bad.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d concurrent forwarded acks failed", n)
	}
}

// The HTTP batch ack asserts the router for this method set (its
// batchAckRouter); a signature drift would silently route every batch
// handle one at a time.
var _ interface {
	RouteAckBatch(ctx context.Context, topicName, op string, handles []consumer.Handle, statuses []int, msgs []string)
} = (*Router)(nil)
