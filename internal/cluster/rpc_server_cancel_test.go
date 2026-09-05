package cluster

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// cancelBroker delivers one message per consume and records nacks.
type cancelBroker struct {
	broker.Broker
	mu      sync.Mutex
	nacked  []consumer.Handle
	consume func(ctx context.Context) (topic.Message, bool, error)
}

func (b *cancelBroker) Consume(ctx context.Context, _ string, _ brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return b.consume(ctx)
}

func (b *cancelBroker) Nack(_ context.Context, _ string, h consumer.Handle) error {
	b.mu.Lock()
	b.nacked = append(b.nacked, h)
	b.mu.Unlock()
	return nil
}

func (b *cancelBroker) nacks() []consumer.Handle {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]consumer.Handle(nil), b.nacked...)
}

var deliveredMsg = topic.Message{Topic: "orders", Partition: 2, Offset: 41, Payload: []byte(`{}`), ReceiptHandle: "2:41:777"}

func consumeReq(t *testing.T) []byte {
	t.Helper()
	return encodeConsumeReq(t, nodewire.ConsumeRequest{Topic: "orders", WaitNanos: int64(time.Second)})
}

// A cancel that lands AFTER the reply was written gives the delivered
// message back: the client sent the cancel because it stopped waiting,
// so it will never read that reply.
func TestConsumeCancelAfterReplyReleasesDelivery(t *testing.T) {
	br := &cancelBroker{consume: func(context.Context) (topic.Message, bool, error) { return deliveredMsg, true, nil }}
	s := &RPCServer{broker: br}
	replies := make(chan clusterwire.StreamFrame, 1)
	if !s.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 7, Payload: consumeReq(t)}, func(f clusterwire.StreamFrame) { replies <- f }) {
		t.Fatal("HandleStreamRequest returned false")
	}
	reply := <-replies
	res, err := nodewire.DecodeResponse(reply.Payload)
	if err != nil || res.Status != http.StatusOK {
		t.Fatalf("reply = %+v err=%v, want 200", res, err)
	}
	if n := br.nacks(); len(n) != 0 {
		t.Fatalf("nacked before any cancel: %v", n)
	}

	s.HandleStreamCancel(0, 7)
	want := consumer.Handle{Partition: 2, Offset: 41, Nonce: 777}
	if n := br.nacks(); len(n) != 1 || n[0] != want {
		t.Fatalf("nacks after cancel = %v, want [%+v]", n, want)
	}
	s.HandleStreamCancel(0, 7) // idempotent: the record was consumed
	if n := br.nacks(); len(n) != 1 {
		t.Fatalf("second cancel nacked again: %v", n)
	}
}

// A cancel that lands WHILE the consume is reserving (the request
// context is already done when the broker returns a message) releases
// the message and answers 204 instead of handing it to a client that is
// gone.
func TestConsumeCancelledBeforeReplyReleasesDelivery(t *testing.T) {
	br := &cancelBroker{consume: func(ctx context.Context) (topic.Message, bool, error) {
		<-ctx.Done() // simulate a reservation racing the cancel
		return deliveredMsg, true, nil
	}}
	s := &RPCServer{broker: br}
	ctx, cancel := context.WithCancel(context.Background())
	replies := make(chan clusterwire.StreamFrame, 1)
	s.HandleStreamRequest(ctx, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 8, Payload: consumeReq(t)}, func(f clusterwire.StreamFrame) { replies <- f })
	cancel()
	reply := <-replies
	res, err := nodewire.DecodeResponse(reply.Payload)
	if err != nil || res.Status != http.StatusNoContent {
		t.Fatalf("reply = %+v err=%v, want 204 for a cancelled consume", res, err)
	}
	if n := br.nacks(); len(n) != 1 {
		t.Fatalf("nacks = %v, want the reserved message released once", n)
	}
	s.HandleStreamCancel(0, 8)
	if n := br.nacks(); len(n) != 1 {
		t.Fatalf("late cancel released it twice: %v", n)
	}
}

// A consume whose client did read the reply (no cancel) leaves the
// reservation alone.
func TestConsumeWithoutCancelKeepsDelivery(t *testing.T) {
	br := &cancelBroker{consume: func(context.Context) (topic.Message, bool, error) { return deliveredMsg, true, nil }}
	s := &RPCServer{broker: br}
	replies := make(chan clusterwire.StreamFrame, 1)
	s.HandleStreamRequest(context.Background(), clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 9, Payload: consumeReq(t)}, func(f clusterwire.StreamFrame) { replies <- f })
	<-replies
	if n := br.nacks(); len(n) != 0 {
		t.Fatalf("nacked without a cancel: %v", n)
	}
}

// Delivery records live only for the cancel grace after the reply and
// are expired from the front of a deadline queue, never by scanning the
// map: under load a node hands out tens of thousands of forwarded
// messages per second and the previous 30 s TTL plus full-map sweep
// was 8 to 11% of broker CPU with every forwarded consume serialised
// behind it.
func TestDeliveryRecordsExpireAfterGraceWithoutScan(t *testing.T) {
	br := &cancelBroker{consume: func(context.Context) (topic.Message, bool, error) { return deliveredMsg, true, nil }}
	now := time.Unix(1_700_000_000, 0)
	s := &RPCServer{broker: br, now: func() time.Time { return now }}
	handle := consumer.Handle{Partition: 2, Offset: 41, Nonce: 777}

	for i := uint64(1); i <= 3; i++ {
		s.rememberDelivery(requestKey{stream: 1, request: i}, "orders", handle)
	}
	if got := len(s.deliveries); got != 3 {
		t.Fatalf("records after 3 deliveries = %d, want 3", got)
	}

	// Within the grace a cancel still gives the message back.
	s.HandleStreamCancel(1, 2)
	if n := br.nacks(); len(n) != 1 {
		t.Fatalf("cancel inside the grace: nacks = %v, want 1", n)
	}

	// Past the grace the remaining records are gone (expired lazily by
	// the next remember) and a late cancel is a no-op.
	now = now.Add(deliveryCancelGrace + time.Millisecond)
	s.rememberDelivery(requestKey{stream: 1, request: 4}, "orders", handle)
	if got := len(s.deliveries); got != 1 {
		t.Fatalf("records after the grace elapsed = %d, want 1 (only the new one)", got)
	}
	if got := len(s.deliveryExpiry); got != 1 {
		t.Fatalf("expiry queue length = %d, want 1", got)
	}
	s.HandleStreamCancel(1, 1)
	s.HandleStreamCancel(1, 3)
	if n := br.nacks(); len(n) != 1 {
		t.Fatalf("late cancels released expired records: nacks = %v", n)
	}

	// A record re-remembered under the same key before it expired keeps
	// its newer deadline: the stale queue entry must not delete it.
	s.rememberDelivery(requestKey{stream: 1, request: 5}, "orders", handle)
	now = now.Add(deliveryCancelGrace / 2)
	s.rememberDelivery(requestKey{stream: 1, request: 5}, "orders", handle) // refreshed
	now = now.Add(deliveryCancelGrace/2 + time.Millisecond)                   // first deadline passed, second not
	s.rememberDelivery(requestKey{stream: 1, request: 6}, "orders", handle)
	if _, ok := s.deliveries[requestKey{stream: 1, request: 5}]; !ok {
		t.Fatal("refreshed record was expired by its stale queue entry")
	}
}

// Remembering many deliveries stays cheap: no call walks the whole map.
func BenchmarkRememberDelivery(b *testing.B) {
	s := &RPCServer{}
	handle := consumer.Handle{Partition: 2, Offset: 41, Nonce: 777}
	for i := 0; b.Loop(); i++ {
		s.rememberDelivery(requestKey{stream: 1, request: uint64(i)}, "orders", handle)
	}
}
