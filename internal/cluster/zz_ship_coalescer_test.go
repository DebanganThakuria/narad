package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/consumer"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzShipQueueAcks holds both of node-a's slots with gated singles and
// queues n more acks behind them (nonces 100..100+n-1), so they leave
// together as one batch once gate is released.
func zzShipQueueAcks(t *testing.T, router *Router, gate *zzWP12Gate, n int, results chan zzWP12Result) {
	t.Helper()
	zzWP12Window(router, 2, zzWP12AddrA)
	for i := range 2 {
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(i), Nonce: 1}, results)
	}
	zzWP12Eventually(t, "both slots busy", func() bool { return gate.held.Load() == 2 })
	for i := range n {
		go zzWP12Ack(router, context.Background(), consumer.Handle{Partition: 0, Offset: int64(50 + i), Nonce: int64(100 + i)}, results)
	}
	zzWP12Eventually(t, "the acks to queue", func() bool { return zzWP12Queued(router, zzWP12AddrA) == n })
}

// A coalesced batch that fails as a whole gives every queued caller the
// answer a single ack of its record would have got: a transport error is
// a 502 with its text, an owner's refusal of the whole batch is that
// reply byte for byte, and a reply that does not carry one result per
// record is a 502. The batch goes out once.
func TestCoalescedBatchFailureReachesEveryQueuedCaller(t *testing.T) {
	refused := errorResponse(http.StatusServiceUnavailable, "request cancelled while waiting for a handler slot")
	// What a single forwarded ack answers when the owner refuses it the
	// same way.
	single := httptest.NewRecorder()
	singlePeer := &zzWP12Peer{single: func(context.Context, string, nodewire.AckBatchItem) (nodewire.Response, error) {
		return refused, nil
	}}
	zzWP12Router(t, singlePeer).RouteAck(context.Background(), single, nil, "orders", consumer.Handle{Partition: 0, Nonce: 1})

	for _, tc := range []struct {
		name  string
		batch func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error)
		check func(t *testing.T, r zzWP12Result)
	}{
		{
			name: "transport error",
			batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
				return nodewire.Response{}, errors.New("peer reset")
			},
			check: func(t *testing.T, r zzWP12Result) {
				if r.code != http.StatusBadGateway || r.body != "peer reset\n" {
					t.Fatalf("queued ack %d answered %d %q, want 502 with the transport error", r.nonce, r.code, r.body)
				}
			},
		},
		{
			name: "whole batch refused",
			batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
				return refused, nil
			},
			check: func(t *testing.T, r zzWP12Result) {
				if r.code != single.Code || r.body != single.Body.String() || r.contentType != single.Header().Get("Content-Type") {
					t.Fatalf("queued ack %d answered %d %q %q; a single ack answers %d %q %q", r.nonce, r.code, r.contentType, r.body,
						single.Code, single.Header().Get("Content-Type"), single.Body.String())
				}
			},
		},
		{
			name: "short reply",
			batch: func(_ context.Context, _ string, req nodewire.AckBatchRequest) (nodewire.Response, error) {
				req.Items = req.Items[:len(req.Items)-1]
				return zzWP12BatchReply(req, zzWP12Owner), nil
			},
			check: func(t *testing.T, r zzWP12Result) {
				if r.code != http.StatusBadGateway || !strings.HasPrefix(r.body, "invalid ack batch reply") {
					t.Fatalf("queued ack %d answered %d %q, want 502 invalid ack batch reply", r.nonce, r.code, r.body)
				}
			},
		},
		{
			name: "garbled reply",
			batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
				return nodewire.Response{Status: http.StatusOK, ContentType: "application/octet-stream", Body: []byte{0xff}}, nil
			},
			check: func(t *testing.T, r zzWP12Result) {
				if r.code != http.StatusBadGateway || !strings.HasPrefix(r.body, "invalid ack batch reply") {
					t.Fatalf("queued ack %d answered %d %q, want 502 invalid ack batch reply", r.nonce, r.code, r.body)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := newZZWP12Gate()
			peer := &zzWP12Peer{single: gate.single, batch: tc.batch}
			router := zzWP12Router(t, peer)
			results := make(chan zzWP12Result, 8)
			zzShipQueueAcks(t, router, gate, 3, results)
			close(gate.release)
			queued := 0
			for range 5 {
				r := <-results
				if r.nonce == 1 {
					if r.code != http.StatusNoContent {
						t.Fatalf("gated single answered %d %q", r.code, r.body)
					}
					continue
				}
				queued++
				tc.check(t, r)
			}
			if queued != 3 {
				t.Fatalf("%d queued acks answered, want 3", queued)
			}
			batches := 0
			for _, c := range peer.snapshot() {
				if c.batch {
					batches++
					if len(c.items) != 3 {
						t.Fatalf("batch carried %d records, want the 3 queued", len(c.items))
					}
				}
			}
			if batches != 1 {
				t.Fatalf("%d batch RPCs, want exactly one", batches)
			}
		})
	}
}

// A queued ack whose budget runs out before a slot frees answers a
// deadline failure, as a single forwarded ack does, and its record is
// never sent: its client was already told it failed.
func TestCoalescedAckQueueBudgetExpires(t *testing.T) {
	gate := newZZWP12Gate()
	peer := &zzWP12Peer{single: gate.single}
	router := zzWP12Router(t, peer)
	results := make(chan zzWP12Result, 4)
	zzShipQueueAcks(t, router, gate, 1, results)
	r := <-results // the gate holds both singles, so only the queued ack can answer
	if r.nonce != 100 || r.code != http.StatusBadGateway || !strings.Contains(r.body, context.DeadlineExceeded.Error()) {
		t.Fatalf("first answer = %+v, want the queued ack's 502 deadline failure", r)
	}
	close(gate.release)
	for range 2 {
		if r := <-results; r.code != http.StatusNoContent {
			t.Fatalf("gated single answered %d %q", r.code, r.body)
		}
	}
	o := router.acks.owner(zzWP12AddrA)
	zzWP12Eventually(t, "the owner to go idle", func() bool {
		o.mu.Lock()
		defer o.mu.Unlock()
		return o.inflight == 0 && len(o.queue) == 0
	})
	for _, c := range peer.snapshot() {
		for _, item := range c.items {
			if item.Nonce == 100 {
				t.Fatalf("the expired ack was sent: %+v", c)
			}
		}
	}
}
