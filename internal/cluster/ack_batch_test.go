package cluster

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// batchOwners answers every OpAckBatch with its owner's scripted reply.
type batchOwners struct {
	fakePeerClient
	reply map[string]func(n int) (nodewire.Response, error)
}

func (f batchOwners) AckBatchWithin(_ context.Context, addr string, _ time.Duration, req nodewire.AckBatchRequest) (nodewire.Response, error) {
	return f.reply[addr](len(req.Items))
}

// A batch ack's handles each get the outcome of their own owner's
// round trip: 503 for an owner the batch never reached (nothing was
// applied), 502 for one that did not answer in time (it may have been
// applied), and an owner's own refusal as it answered. Neither failure
// names the owner or the transport error.
func TestRouteAckBatchReportsNotSentPerHandle(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store) // partition 1: remote.example:7942
	ctx := context.Background()
	for p, m := range map[int]metastore.Member{
		0: {ID: "node-stale", Addr: "stale.example:7942", Status: metastore.MemberAlive},
		2: {ID: "node-slow", Addr: "slow.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember() error = %v", err)
		}
		if err := store.AssignPartition(ctx, "orders", p, m.ID); err != nil {
			t.Fatalf("AssignPartition() error = %v", err)
		}
	}
	rt := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	rt.peer = batchOwners{reply: map[string]func(int) (nodewire.Response, error){
		ackTestOwnerAddr: func(int) (nodewire.Response, error) {
			return nodewire.Response{}, fmt.Errorf("%w: dial %s: connection refused", clusterrpc.ErrNotSent, ackTestOwnerAddr)
		},
		"slow.example:7942": func(int) (nodewire.Response, error) {
			return nodewire.Response{}, fmt.Errorf("cluster rpc request timed out: %w", context.DeadlineExceeded)
		},
		"stale.example:7942": func(n int) (nodewire.Response, error) {
			results := make([]nodewire.AckResult, n)
			for i := range results {
				results[i] = nodewire.AckResult{Status: http.StatusGone, Error: "receipt handle is stale"}
			}
			body, err := nodewire.AppendAckBatchReply(nil, results)
			return nodewire.Response{Status: http.StatusOK, Body: body}, err
		},
	}}
	handles := []consumer.Handle{
		{Partition: 1, Offset: 1, Nonce: 1},
		{Partition: 1, Offset: 2, Nonce: 1},
		{Partition: 2, Offset: 1, Nonce: 1},
		{Partition: 2, Offset: 2, Nonce: 1},
		{Partition: 0, Offset: 1, Nonce: 1},
		{Partition: 0, Offset: 2, Nonce: 1},
	}
	statuses := make([]int, len(handles))
	msgs := make([]string, len(handles))
	rt.RouteAckBatch(ctx, "orders", "ack", handles, statuses, msgs)

	want := []int{503, 503, 502, 502, 410, 410}
	for i := range handles {
		if statuses[i] != want[i] {
			t.Errorf("handle %d (partition %d): status %d (%q), want %d", i, handles[i].Partition, statuses[i], msgs[i], want[i])
		}
		if statuses[i] >= 500 && (strings.Contains(msgs[i], ".example") || strings.Contains(msgs[i], "deadline") || strings.Contains(msgs[i], "refused")) {
			t.Errorf("handle %d: message %q names the owner or the transport error", i, msgs[i])
		}
	}
	if msgs[4] != "receipt handle is stale" {
		t.Errorf("owner's 410 message = %q, want it passed through", msgs[4])
	}
}

// A batch ack whose request went away is not reported as a failure of
// the owner.
func TestRouteAckBatchClientGoneIs499(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	rt := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	ctx, cancel := context.WithCancel(context.Background())
	rt.peer = batchOwners{reply: map[string]func(int) (nodewire.Response, error){
		ackTestOwnerAddr: func(int) (nodewire.Response, error) {
			cancel()
			return nodewire.Response{}, context.Canceled
		},
	}}
	handles := []consumer.Handle{{Partition: 1, Offset: 1, Nonce: 1}, {Partition: 1, Offset: 2, Nonce: 1}}
	statuses := make([]int, len(handles))
	msgs := make([]string, len(handles))
	rt.RouteAckBatch(ctx, "orders", "ack", handles, statuses, msgs)
	for i, s := range statuses {
		if s != 499 {
			t.Errorf("handle %d: status %d (%q), want 499", i, s, msgs[i])
		}
	}
}
