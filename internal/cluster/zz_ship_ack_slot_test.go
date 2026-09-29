package cluster

import (
	"context"
	"net/http"
	"strings"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A single ack, extend or nack whose requester gave up while it waited
// for a messaging slot is answered 503 and not applied: the requester
// already answered its client with an error, and a retry is applied on
// its own. With a slot free the same request is applied.
func TestServerSingleAckAbandonedInSlotQueueIsNotApplied(t *testing.T) {
	for _, tc := range []struct {
		op     string
		encode func(nodewire.AckRequest) ([]byte, error)
	}{
		{"ack", nodewire.EncodeAckRequest},
		{"extend", nodewire.EncodeExtendAckRequest},
		{"nack", nodewire.EncodeNackRequest},
	} {
		t.Run(tc.op, func(t *testing.T) {
			br := &zzWP12Broker{}
			s := NewRPCServer(br, nil, nil)
			s.SetMessagingConcurrency(1)
			s.messagingSem <- struct{}{} // every slot taken
			payload, err := tc.encode(nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 3, Nonce: 1})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			res := zzWP12Serve(s, ctx, payload)
			if res.Status != http.StatusServiceUnavailable || !strings.Contains(string(res.Body), "waiting for a handler slot") {
				t.Fatalf("abandoned %s: %d %q, want 503 for the slot wait", tc.op, res.Status, res.Body)
			}
			br.mu.Lock()
			calls := len(br.calls)
			br.mu.Unlock()
			if calls != 0 {
				t.Fatalf("an abandoned %s reached the broker: %v", tc.op, br.calls)
			}

			<-s.messagingSem
			if res := zzWP12Serve(s, context.Background(), payload); res.Status != http.StatusNoContent {
				t.Fatalf("%s with a free slot: %d %q, want 204", tc.op, res.Status, res.Body)
			}
			br.mu.Lock()
			defer br.mu.Unlock()
			if len(br.calls) != 1 || br.calls[0] != tc.op+":1" {
				t.Fatalf("broker calls = %v, want the one %s applied", br.calls, tc.op)
			}
		})
	}
}
