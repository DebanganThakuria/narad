package cluster

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/consumer"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The HTTP layer names the batch op with the same strings; each maps to
// its own mode, and nothing else maps to one.
func TestAckModeOfRejectsUnknownOp(t *testing.T) {
	for op, want := range map[string]nodewire.AckMode{
		ackOpAck:    nodewire.AckModeAck,
		ackOpExtend: nodewire.AckModeExtend,
		ackOpNack:   nodewire.AckModeNack,
	} {
		if got, ok := ackModeOf(op); !ok || got != want {
			t.Fatalf("ackModeOf(%q) = %d, %v; want %d, true", op, got, ok, want)
		}
	}
	for _, op := range []string{"", "extend_ack", "Nack", "commit"} {
		if _, ok := ackModeOf(op); ok {
			t.Fatalf("ackModeOf(%q) mapped to a mode; it would commit the record", op)
		}
	}
}

// An extend batch goes out as extends, batched or single.
func TestRouteAckBatchExtendSendsExtendMode(t *testing.T) {
	peer := &zzWP12Peer{}
	router := zzWP12Router(t, peer)
	// Two records for node-a (a batch), one for node-b (a single).
	handles := []consumer.Handle{{Partition: 0, Nonce: 1}, {Partition: 4, Nonce: 2}, {Partition: 1, Nonce: 3}}
	statuses, msgs := make([]int, 3), make([]string, 3)
	router.RouteAckBatch(context.Background(), "orders", ackOpExtend, handles, statuses, msgs)
	if want := []int{http.StatusNoContent, http.StatusNoContent, http.StatusNoContent}; !slices.Equal(statuses, want) {
		t.Fatalf("statuses = %v msgs = %q, want %v", statuses, msgs, want)
	}
	calls := peer.snapshot()
	if len(calls) != 2 {
		t.Fatalf("calls = %+v; want one batch to node-a and one single to node-b", calls)
	}
	for _, c := range calls {
		for _, item := range c.items {
			if item.Mode != nodewire.AckModeExtend {
				t.Fatalf("call to %s (batch %v) carried mode %d, want extend", c.addr, c.batch, item.Mode)
			}
		}
	}
}

// An op the router does not know settles nothing: no RPC goes out, and
// every handle not already rejected, the local one included, answers
// 500 so the caller does not apply it either.
func TestRouteAckBatchUnknownOpSendsNothing(t *testing.T) {
	peer := &zzWP12Peer{}
	router := zzWP12Router(t, peer)
	// Partitions 0..4: node-a, node-b, local, dead, node-a; then one the
	// caller already rejected.
	handles := zzWP12Handles(1, 2, 3, 4, 5, 6)
	statuses, msgs := make([]int, len(handles)), make([]string, len(handles))
	statuses[5], msgs[5] = http.StatusBadRequest, "invalid receipt handle"
	router.RouteAckBatch(context.Background(), "orders", "extend_ack", handles, statuses, msgs)
	if calls := peer.snapshot(); len(calls) != 0 {
		t.Fatalf("calls = %+v; an unknown op must not reach an owner", calls)
	}
	for i := range 5 {
		if statuses[i] != http.StatusInternalServerError || msgs[i] != "unknown ack op extend_ack" {
			t.Fatalf("handle %d answered %d %q, want 500 naming the op", i, statuses[i], msgs[i])
		}
	}
	if statuses[5] != http.StatusBadRequest || msgs[5] != "invalid receipt handle" {
		t.Fatalf("an already rejected handle was overwritten: %d %q", statuses[5], msgs[5])
	}
}

// An owner's OpAckBatch that fails as a whole gives each of that owner's
// records one shared outcome, and leaves the other owners' records
// alone: a whole-batch refusal is the owner's own status and message,
// and a reply without one result per record is a 502.
func TestRouteAckBatchWholeBatchFailure(t *testing.T) {
	for _, tc := range []struct {
		name       string
		batch      func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error)
		wantStatus int
		wantMsg    string
	}{
		{
			name: "refused",
			batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
				return errorResponse(http.StatusServiceUnavailable, "request cancelled while waiting for a handler slot"), nil
			},
			wantStatus: http.StatusServiceUnavailable,
			wantMsg:    "request cancelled while waiting for a handler slot",
		},
		{
			name: "short reply",
			batch: func(_ context.Context, _ string, req nodewire.AckBatchRequest) (nodewire.Response, error) {
				req.Items = req.Items[:1]
				return zzWP12BatchReply(req, zzWP12Owner), nil
			},
			wantStatus: http.StatusBadGateway,
			wantMsg:    "invalid ack batch reply: wrong record count",
		},
		{
			name: "garbled reply",
			batch: func(context.Context, string, nodewire.AckBatchRequest) (nodewire.Response, error) {
				return nodewire.Response{Status: http.StatusOK, ContentType: "application/octet-stream", Body: []byte{0xff}}, nil
			},
			wantStatus: http.StatusBadGateway,
			wantMsg:    "invalid ack batch reply: ",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := &zzWP12Peer{batch: tc.batch}
			router := zzWP12Router(t, peer)
			// Two records for node-a (a batch), one for node-b (a single).
			handles := []consumer.Handle{{Partition: 0, Nonce: 1}, {Partition: 4, Nonce: 2}, {Partition: 1, Nonce: 3}}
			statuses, msgs := make([]int, 3), make([]string, 3)
			router.RouteAckBatch(context.Background(), "orders", ackOpAck, handles, statuses, msgs)
			for i := range 2 {
				if statuses[i] != tc.wantStatus || !strings.HasPrefix(msgs[i], tc.wantMsg) {
					t.Fatalf("node-a record %d = %d %q, want %d %q", i, statuses[i], msgs[i], tc.wantStatus, tc.wantMsg)
				}
			}
			if statuses[2] != http.StatusNoContent || msgs[2] != "" {
				t.Fatalf("node-b record = %d %q, want its own 204", statuses[2], msgs[2])
			}
		})
	}
}
