package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzShipBatchErrBroker fails every batch consume with err, first running
// during (to end the request while the scan runs, say).
type zzShipBatchErrBroker struct {
	broker.Broker
	err    error
	during func()
}

func (b *zzShipBatchErrBroker) ConsumeBatch(context.Context, string, brokermsg.ConsumeOpts, int, []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	if b.during != nil {
		b.during()
	}
	return nil, nil, b.err
}

// The owner answers a failed batch consume as it answers a failed single
// one: a partition it does not own is an empty 204 for a local-only
// probe and 421 otherwise, broker errors map to their statuses, and a
// request that ended while the scan ran is a quiet 204.
func TestServerBatchConsumeErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		localOnly bool
		cancel    bool
		want      int
		body      string
	}{
		{name: "not owner, local-only probe", err: errs.ErrNotPartitionOwner, localOnly: true, want: http.StatusNoContent},
		{name: "not owner, pinned", err: errs.ErrNotPartitionOwner, want: http.StatusMisdirectedRequest, body: errs.ErrNotPartitionOwner.Error()},
		{name: "topic gone", err: errs.ErrTopicNotFound, localOnly: true, want: http.StatusNotFound, body: "topic not found"},
		{name: "internal", err: errors.New("disk on fire"), localOnly: true, want: http.StatusInternalServerError, body: "consume failed"},
		{name: "requester left", err: context.Canceled, localOnly: true, cancel: true, want: http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			br := &zzShipBatchErrBroker{err: tc.err}
			if tc.cancel {
				br.during = cancel
			}
			s := NewRPCServer(br, nil, nil)
			req := nodewire.ConsumeRequest{Topic: "orders", LocalOnly: tc.localOnly, Max: 5}
			if !tc.localOnly {
				req.Partition, req.HasPartition = 0, true
			}
			res := s.consume(ctx, requestKey{stream: 1, request: 1}, &req, 0)
			if res.Status != tc.want || !strings.Contains(string(res.Body), tc.body) {
				t.Fatalf("status %d body %q, want %d containing %q", res.Status, res.Body, tc.want, tc.body)
			}
			if tc.want == http.StatusNoContent && len(res.Body) != 0 {
				t.Fatalf("204 carried a body: %q", res.Body)
			}
		})
	}
}

// A pinned batch consume the router cannot forward is answered without
// an RPC: 503 for an owner that is down, left to the caller for a
// partition this node owns, and 400 for a query it cannot parse. A
// transport failure on the forward is a 502 with its text.
func TestRouteConsumeBatchPinnedErrors(t *testing.T) {
	var calls atomic.Int32
	router := zzWP12Router(t, fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		calls.Add(1)
		return nodewire.Response{}, errors.New("peer reset")
	}})
	// zzWP12Router: partition 0 on node-a, 2 here, 3 on a dead node.
	for _, tc := range []struct {
		name      string
		partition int
		query     string
		forwarded bool
		want      int
		body      string
	}{
		{name: "owner down", partition: 3, forwarded: true, want: http.StatusServiceUnavailable, body: ownerDownMessage},
		{name: "owned here", partition: 2, want: http.StatusOK},
		{name: "bad query", partition: 0, query: "&wait=soon", forwarded: true, want: http.StatusBadRequest, body: "invalid duration"},
		{name: "transport failure", partition: 0, forwarded: true, want: http.StatusBadGateway, body: "peer reset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?max=10"+tc.query, nil)
			partition := tc.partition
			forwarded, batch, local := router.RouteConsumeBatch(context.Background(), res, req, "orders", &partition, 10)
			if forwarded != tc.forwarded || batch || local != nil {
				t.Fatalf("RouteConsumeBatch() = (%v, %v, %v), want (%v, false, nil)", forwarded, batch, local, tc.forwarded)
			}
			if !forwarded {
				if res.Body.Len() != 0 {
					t.Fatalf("a request left to the caller was answered: %d %q", res.Code, res.Body)
				}
				return
			}
			if res.Code != tc.want || !strings.Contains(res.Body.String(), tc.body) {
				t.Fatalf("status %d body %q, want %d containing %q", res.Code, res.Body, tc.want, tc.body)
			}
		})
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d forwarded consumes, want only the transport-failure case's", n)
	}
}
