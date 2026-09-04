package cluster

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// roundTripRPC pushes one request frame through HandleStreamFrame and
// decodes the reply, exercising the full server-side encode path.
func roundTripRPC(t *testing.T, s *RPCServer, payload []byte) nodewire.Response {
	t.Helper()
	replies := make(chan clusterwire.StreamFrame, 1)
	if !s.HandleStreamFrame(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 9, Payload: payload}, func(f clusterwire.StreamFrame) { replies <- f }) {
		t.Fatal("HandleStreamFrame() = false for a node request")
	}
	select {
	case reply := <-replies:
		if reply.Type != clusterwire.StreamFrameNodeReply || reply.RequestID != 9 {
			t.Fatalf("reply frame = %+v", reply)
		}
		res, err := nodewire.DecodeResponse(reply.Payload)
		if err != nil {
			t.Fatalf("DecodeResponse() error = %v", err)
		}
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("no reply")
		return nodewire.Response{}
	}
}

// A forwarded consume must return exactly the bytes a local consume would:
// the message's own append-style encoding with the JSON payload embedded
// verbatim, internal whitespace included. Routing the reply through
// json.Marshal compacted the payload and made the two paths differ.
func TestRPCServerConsumeReplyIsByteIdenticalToLocalEncoding(t *testing.T) {
	payload := []byte("{ \"id\" : 1,\n  \"tags\": [ \"a\",  \"b\" ] }")
	msg := topic.Message{
		Topic:         "orders",
		Partition:     1,
		Offset:        7,
		Key:           "k",
		Payload:       payload,
		Timestamp:     123,
		ReceiptHandle: "1:7:99",
	}
	br := &consumeOnlyBroker{consumeFn: func(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		return msg, true, nil
	}}
	s := &RPCServer{broker: br}

	res := roundTripRPC(t, s, encodeConsumeReq(t, nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true}))
	if res.Status != http.StatusOK || res.ContentType != nodewire.ContentTypeJSON {
		t.Fatalf("status/content-type = %d/%q", res.Status, res.ContentType)
	}
	want := append(msg.AppendJSON(nil), '\n')
	if !bytes.Equal(res.Body, want) {
		t.Fatalf("body =\n%s\nwant\n%s", res.Body, want)
	}
	if !bytes.Contains(res.Body, payload) {
		t.Fatalf("payload whitespace was not preserved verbatim in %s", res.Body)
	}
}

// The RPC-side consume clamp must honor the configured ceiling, not only
// the package constant, so it agrees with the router and HTTP handlers.
func TestRPCServerConsumeHonorsConfiguredMaxWait(t *testing.T) {
	var gotWait time.Duration
	br := &consumeOnlyBroker{consumeFn: func(_ context.Context, _ string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
		gotWait = opts.Wait
		return topic.Message{}, false, nil
	}}
	s := &RPCServer{broker: br}
	s.SetMaxConsumeWait(10 * time.Second)
	s.SetMaxConsumeWait(0) // ignored: values <= 0 keep the current ceiling

	res := s.handleConsume(context.Background(), requestKey{}, encodeConsumeReq(t, nodewire.ConsumeRequest{Topic: "orders", WaitNanos: int64(24 * time.Hour)}))
	if res.Status != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Status, http.StatusNoContent)
	}
	if gotWait != 10*time.Second {
		t.Fatalf("broker wait = %s, want clamped to the configured 10s", gotWait)
	}
}

type commitBatchOnlyBroker struct {
	broker.Broker
	offsets []int64
}

func (b *commitBatchOnlyBroker) CommitAcceptedProduceBatch(context.Context, []ingress.ProduceRecord) ([]int64, error) {
	return b.offsets, nil
}

// Nothing decodes the commit_produce_batch body beyond the status, so it
// is a small summary rather than every offset.
func TestRPCServerCommitBatchReplyIsSummary(t *testing.T) {
	payload, err := nodewire.EncodeCommitProduceBatchRequest(nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{
		{Topic: "orders", Payload: []byte("a")}, {Topic: "orders", Payload: []byte("b")}, {Topic: "orders", Payload: []byte("c")},
	}})
	if err != nil {
		t.Fatalf("EncodeCommitProduceBatchRequest() error = %v", err)
	}
	s := &RPCServer{broker: &commitBatchOnlyBroker{offsets: []int64{40, 41, 42}}}
	res := roundTripRPC(t, s, payload)
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.Status)
	}
	if got := string(res.Body); got != "{\"count\":3,\"first_offset\":40}\n" {
		t.Fatalf("body = %q", got)
	}
	empty := commitBatchResponse(nil)
	if got := string(empty.Body); got != "{\"count\":0}\n" {
		t.Fatalf("empty body = %q", got)
	}
}

// gatedBroker blocks every messaging call until release is closed and
// tracks how many are executing at once.
type gatedBroker struct {
	broker.Broker
	release   chan struct{}
	inFlight  atomic.Int32
	maxSeen   atomic.Int32
	started   chan struct{}
	startOnce sync.Once
}

func (b *gatedBroker) enter() {
	n := b.inFlight.Add(1)
	for {
		seen := b.maxSeen.Load()
		if n <= seen || b.maxSeen.CompareAndSwap(seen, n) {
			break
		}
	}
	b.startOnce.Do(func() { close(b.started) })
	<-b.release
	b.inFlight.Add(-1)
}

func (b *gatedBroker) Ack(context.Context, string, consumer.Handle) error {
	b.enter()
	return nil
}

func (b *gatedBroker) Consume(_ context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	if opts.Wait > 0 {
		// A long-poll must not need a slot: it completes even while the
		// gate is saturated.
		return topic.Message{}, false, nil
	}
	b.enter()
	return topic.Message{}, false, nil
}

// Messaging handlers are bounded by the semaphore; the frame read path
// still answers other ops while the gate is saturated, and blocking
// long-poll consumes bypass it so they cannot starve commits and acks.
func TestRPCServerMessagingGateBoundsConcurrency(t *testing.T) {
	br := &gatedBroker{release: make(chan struct{}), started: make(chan struct{})}
	s := &RPCServer{broker: br}
	s.SetMessagingConcurrency(1)

	ackPayload, err := nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2})
	if err != nil {
		t.Fatalf("EncodeAckRequest() error = %v", err)
	}
	replies := make(chan clusterwire.StreamFrame, 8)
	respond := func(f clusterwire.StreamFrame) { replies <- f }
	for i := range 3 {
		s.HandleStreamFrame(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: uint64(i), Payload: ackPayload}, respond)
	}
	<-br.started
	time.Sleep(50 * time.Millisecond)
	if got := br.inFlight.Load(); got != 1 {
		t.Fatalf("acks executing concurrently = %d, want 1 (gate size)", got)
	}

	// An op outside the gate (unknown opcode -> 400) is answered while the
	// gate is held.
	s.HandleStreamFrame(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 100, Payload: []byte{0xFE}}, respond)
	// A long-poll consume is answered too.
	longPoll := encodeConsumeReq(t, nodewire.ConsumeRequest{Topic: "orders", WaitNanos: int64(time.Second)})
	s.HandleStreamFrame(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 101, Payload: longPoll}, respond)
	got := map[uint64]bool{}
	for len(got) < 2 {
		select {
		case reply := <-replies:
			if reply.RequestID < 100 {
				t.Fatalf("ack %d replied while the gate should hold it", reply.RequestID)
			}
			got[reply.RequestID] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("ungated ops did not reply while the gate was saturated; got %v", got)
		}
	}

	close(br.release)
	for range 3 {
		select {
		case <-replies:
		case <-time.After(2 * time.Second):
			t.Fatal("gated acks did not complete after release")
		}
	}
	if br.maxSeen.Load() != 1 {
		t.Fatalf("max concurrent messaging handlers = %d, want 1", br.maxSeen.Load())
	}
}

func TestRPCServerDefaultsAndDisabledGate(t *testing.T) {
	s := NewRPCServer(nil, nil, nil)
	if s.messagingSem == nil || cap(s.messagingSem) != defaultMessagingConcurrency() {
		t.Fatalf("default gate cap = %d, want %d", cap(s.messagingSem), defaultMessagingConcurrency())
	}
	s.SetMessagingConcurrency(0)
	if s.messagingSem != nil {
		t.Fatal("SetMessagingConcurrency(0) did not disable the gate")
	}
	if s.consumeWaitCeiling() != defaultMaxConsumeWait {
		t.Fatalf("consume wait ceiling = %s, want %s", s.consumeWaitCeiling(), defaultMaxConsumeWait)
	}
}
