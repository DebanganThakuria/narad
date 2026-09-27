package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func zzWP18Batch(t *testing.T, res nodewire.Response) []topic.Message {
	t.Helper()
	if res.Status != http.StatusOK {
		t.Fatalf("reply status %d (%s), want 200", res.Status, res.Body)
	}
	var reply struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.Unmarshal(res.Body, &reply); err != nil {
		t.Fatalf("reply %q: %v", res.Body, err)
	}
	return reply.Messages
}

func zzWP18Serve(t *testing.T, s *RPCServer, ctx context.Context, req nodewire.ConsumeRequest) nodewire.Response {
	t.Helper()
	payload, err := nodewire.EncodeConsumeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	return s.handleConsume(ctx, requestKey{stream: 1, request: 1}, payload)
}

// TestZZWP18ServerBatchConsume is the owner's side of a forwarded batch:
// up to Max records in one reply, each encoded exactly as a single
// consume's reply is, capped at maxForwardedConsumeBatch, and only from
// the pinned partition when one is pinned.
func TestZZWP18ServerBatchConsume(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	owner.fill(t, 2, 80, []byte(`{"k":"v"}`))
	ctx := context.Background()

	msgs := zzWP18Batch(t, zzWP18Serve(t, owner.server, ctx, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 7}))
	if len(msgs) != 7 {
		t.Fatalf("Max 7 returned %d records", len(msgs))
	}
	res := zzWP18Serve(t, owner.server, ctx, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 3})
	var want bytes.Buffer
	want.WriteString(`{"messages":[`)
	for i, m := range zzWP18Batch(t, res) {
		if i > 0 {
			want.WriteByte(',')
		}
		want.Write(m.AppendJSON(nil))
	}
	want.WriteString("]}\n")
	if !bytes.Equal(res.Body, want.Bytes()) || res.ContentType != nodewire.ContentTypeJSON {
		t.Fatalf("batch reply %q (%s), want each record as a single consume encodes it: %q", res.Body, res.ContentType, want.Bytes())
	}

	msgs = zzWP18Batch(t, zzWP18Serve(t, owner.server, ctx, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 1000}))
	if len(msgs) != maxForwardedConsumeBatch {
		t.Fatalf("Max 1000 returned %d records, want the cap %d", len(msgs), maxForwardedConsumeBatch)
	}

	msgs = zzWP18Batch(t, zzWP18Serve(t, owner.server, ctx, nodewire.ConsumeRequest{Topic: "orders", Partition: 1, HasPartition: true, Max: 50}))
	if len(msgs) == 0 {
		t.Fatal("pinned batch returned nothing")
	}
	for _, m := range msgs {
		if m.Partition != 1 {
			t.Fatalf("pinned batch returned a record of partition %d", m.Partition)
		}
	}
}

// TestZZWP18ServerBatchConsumeByteBound checks a forwarded batch stops
// at forwardedConsumeBatchBytes, so its reply fits in one RPC frame
// however large the records are, and leaves the rest for the next one.
func TestZZWP18ServerBatchConsumeByteBound(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	big := []byte(`"` + strings.Repeat("x", 900<<10) + `"`)
	owner.fill(t, 1, 12, big)
	res := zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 100})
	msgs := zzWP18Batch(t, res)
	perRecord := len(big)
	want := (forwardedConsumeBatchBytes + perRecord - 1) / perRecord
	if len(msgs) != want {
		t.Fatalf("a batch of %d-byte records took %d, want %d (the byte bound)", perRecord, len(msgs), want)
	}
	if len(res.Body) > 8<<20 {
		t.Fatalf("batch reply is %d bytes, want it well inside one RPC frame", len(res.Body))
	}
	// What the bound left behind is still there for the next batches.
	taken := len(msgs)
	for taken < 12 {
		more := zzWP18Batch(t, zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 100}))
		if len(more) == 0 || len(more) > want {
			t.Fatalf("a later batch took %d with %d left, want between 1 and %d", len(more), 12-taken, want)
		}
		taken += len(more)
	}
}

// TestZZWP18ServerBatchConsumeWaitTopsUp checks a batch that finds
// nothing parks like a single consume and, woken by the first record of
// a burst, takes the rest of the burst with it.
func TestZZWP18ServerBatchConsumeWaitTopsUp(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		owner.fill(t, 1, 5, []byte(`{"k":"v"}`))
	}()
	start := time.Now()
	res := zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{Topic: "orders", Partition: 0, HasPartition: true, WaitNanos: int64(5 * time.Second), Max: 10})
	msgs := zzWP18Batch(t, res)
	if len(msgs) != 5 || time.Since(start) > 4*time.Second {
		t.Fatalf("a waiting batch returned %d records after %v, want the burst of 5 as soon as it landed", len(msgs), time.Since(start))
	}
}

// TestZZWP18ServerBatchConsumeGoneRequester checks that a batch whose
// requester is gone by the time the records are reserved gives them all
// back at once instead of hiding them until their leases lapse.
func TestZZWP18ServerBatchConsumeGoneRequester(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	owner.fill(t, 1, 5, []byte(`{"k":"v"}`))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Called past the messaging slot, as a long-poll batch is: the slot
	// check is what normally turns a gone requester away first.
	req := nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 5}
	if res := owner.server.consume(ctx, requestKey{stream: 1, request: 9}, &req, 0); res.Status != http.StatusNoContent {
		t.Fatalf("batch for a gone requester = %d, want 204", res.Status)
	}
	msgs := zzWP18Batch(t, zzWP18Serve(t, owner.server, context.Background(), nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 5}))
	if len(msgs) != 5 {
		t.Fatalf("after a gone requester's batch, %d of 5 records are available, want all", len(msgs))
	}
}

// TestZZWP18ServerBatchConsumeClaim checks a claim that wins records
// resolves its notification, as a single claim does, and an empty one
// does not.
func TestZZWP18ServerBatchConsumeClaim(t *testing.T) {
	owner := newZZWP18Owner(t, 1)
	claim := nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true, Max: 4}
	if res := zzWP18Serve(t, owner.server, context.Background(), claim); res.Status != http.StatusNoContent {
		t.Fatalf("empty claim = %d, want 204", res.Status)
	}
	if n := owner.broker.claims.Load(); n != 0 {
		t.Fatalf("an empty claim resolved %d notifications, want 0", n)
	}
	owner.fill(t, 1, 3, []byte(`{"k":"v"}`))
	if msgs := zzWP18Batch(t, zzWP18Serve(t, owner.server, context.Background(), claim)); len(msgs) != 3 {
		t.Fatalf("claim returned %d records, want 3", len(msgs))
	}
	if n := owner.broker.claims.Load(); n != 1 {
		t.Fatalf("a winning claim resolved %d notifications, want 1", n)
	}
}

// TestZZWP18ServerBatchConsumeSingleBroker checks a broker without batch
// consume answers a batch request with one record in the batch shape.
func TestZZWP18ServerBatchConsumeSingleBroker(t *testing.T) {
	msg := topic.Message{Topic: "orders", Partition: 0, Offset: 7, Payload: []byte(`{"k":"v"}`), ReceiptHandle: "0:7:9"}
	s := NewRPCServer(&zzWP9Broker{msg: msg, found: true}, nil, nil)
	res := zzWP18Serve(t, s, context.Background(), nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 10})
	want := `{"messages":[` + string(msg.AppendJSON(nil)) + "]}\n"
	if res.Status != http.StatusOK || string(res.Body) != want {
		t.Fatalf("batch from a single-record broker = %d %q, want %q", res.Status, res.Body, want)
	}
}

// zzWP18LegacyPeer is an owner on a release before Max: a consume
// request that carries it is refused as trailing data, one that does
// not gets one record.
type zzWP18LegacyPeer struct {
	fakePeerClient
	mu   sync.Mutex
	maxs []int
}

func (p *zzWP18LegacyPeer) ConsumeWithin(_ context.Context, _ string, _ time.Duration, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	p.mu.Lock()
	p.maxs = append(p.maxs, req.Max)
	p.mu.Unlock()
	if req.Max > 1 {
		return errorResponse(http.StatusBadRequest, "invalid consume request: trailing node rpc payload data"), nil
	}
	msg := topic.Message{Topic: "orders", Partition: 0, Offset: 1, Payload: []byte(`{}`), ReceiptHandle: "0:1:2"}
	return nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: append(msg.AppendJSON(nil), '\n')}, nil
}

func (p *zzWP18LegacyPeer) Consume(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	return p.ConsumeWithin(ctx, addr, 0, req)
}

func (p *zzWP18LegacyPeer) calls() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.maxs...)
}

// TestZZWP18RouteConsumeBatchLegacyOwner checks the rolling-upgrade
// fallback: an owner that refuses Max is asked again for one record (the
// consumer still gets it, as a single message to wrap), is remembered so
// later batches ask it for one record straight away, and is tried with
// Max again once the memory expires.
func TestZZWP18RouteConsumeBatchLegacyOwner(t *testing.T) {
	peer := &zzWP18LegacyPeer{}
	router := newZZWP18Gateway(t, "remote.example:7942", peer, 1)
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?max=10", nil)
	route := func() (bool, bool, string) {
		w := httptest.NewRecorder()
		forwarded, batch, local := router.RouteConsumeBatch(context.Background(), w, req, "orders", nil, 10)
		if local != nil {
			t.Fatal("a node that owns nothing reported a local partition")
		}
		return forwarded, batch, w.Body.String()
	}

	forwarded, batch, body := route()
	if !forwarded || batch || !strings.Contains(body, `"receipt_handle":"0:1:2"`) {
		t.Fatalf("legacy owner: forwarded=%v batch=%v body %q, want its single record, unbatched", forwarded, batch, body)
	}
	if got := peer.calls(); len(got) != 2 || got[0] != 10 || got[1] != 0 {
		t.Fatalf("requests to a legacy owner carried Max %v, want [10 0]", got)
	}
	route()
	if got := peer.calls(); len(got) != 3 || got[2] != 0 {
		t.Fatalf("a remembered legacy owner was asked with Max %v, want one request without it", got[2:])
	}

	// Once the memory lapses, Max is tried again.
	router.legacyBatchConsume.Store("remote.example:7942", time.Now().Add(-time.Second))
	route()
	if got := peer.calls(); len(got) != 5 || got[3] != 10 || got[4] != 0 {
		t.Fatalf("after the memory lapsed, requests carried Max %v, want [10 0]", got[3:])
	}
}

// TestZZWP18RouteConsumeBatchPinned checks a pinned batch consume on a
// node that does not own the partition asks its owner for the batch, over
// real QUIC, and that the pinned partition's backlog comes back whole.
func TestZZWP18RouteConsumeBatchPinned(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	owner.fill(t, 2, 6, []byte(`{"k":"v"}`))
	addr, client := owner.serveQUIC(t)
	router := newZZWP18Gateway(t, addr, client, 2)
	pinned := 1
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?partition=1&max=10", nil)
	forwarded, batch, _ := router.RouteConsumeBatch(context.Background(), w, req, "orders", &pinned, 10)
	if !forwarded || !batch {
		t.Fatalf("pinned batch: forwarded=%v batch=%v, want a forwarded batch", forwarded, batch)
	}
	msgs := zzWP18Batch(t, nodewire.Response{Status: w.Code, Body: w.Body.Bytes()})
	if len(msgs) != 6 {
		t.Fatalf("pinned batch returned %d records, want partition 1's 6", len(msgs))
	}
	for _, m := range msgs {
		if m.Partition != 1 {
			t.Fatalf("pinned batch returned a record of partition %d", m.Partition)
		}
	}
}

// Compile-time check: the test broker keeps the batch surface the owner
// asserts for.
var _ interface {
	ConsumeBatch(context.Context, string, brokermsg.ConsumeOpts, int, []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error)
} = (*zzWP18EngineBroker)(nil)
