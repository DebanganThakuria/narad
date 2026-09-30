package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
)

// zzPerfCGatewayBroker is the broker of node-gw, which owns partition 0
// of "orders" and has nothing in it: every batch scan comes back empty
// with a waiter, and the local wait ends only on the router's external
// wake, the client leaving, or the budget. scans counts the batch scans,
// so a test can tell whether the handler topped a batch up locally.
type zzPerfCGatewayBroker struct {
	broker.Broker
	scans atomic.Int64
}

func (b *zzPerfCGatewayBroker) ConsumeBatch(_ context.Context, _ string, _ brokermsg.ConsumeOpts, _ int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	b.scans.Add(1)
	return dst, &brokermsg.ConsumeWaiter{}, nil
}

func (b *zzPerfCGatewayBroker) ConsumeWait(ctx context.Context, _ *brokermsg.ConsumeWaiter, wait time.Duration, external <-chan struct{}) (topic.Message, bool, bool, error) {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-external:
		return topic.Message{}, false, true, nil
	case <-ctx.Done():
		return topic.Message{}, false, false, nil
	case <-timer.C:
		return topic.Message{}, false, false, nil
	}
}

// zzPerfCGateway builds node-gw: it owns partition 0 of "orders" (two
// partitions), and node-remote at ownerAddr, reached through peer, owns
// partition 1. It returns node-gw's router, its local broker and its
// HTTP consume handler. tokens turns the token protocol on.
func zzPerfCGateway(t *testing.T, ownerAddr string, peer peerClient, tokens bool) (*Router, *zzPerfCGatewayBroker, http.HandlerFunc) {
	t.Helper()
	ctx := context.Background()
	store := zzWP9Store(t)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	for _, m := range []metastore.Member{
		{ID: "node-gw", Addr: "node-gw.example:7942", Status: metastore.MemberAlive},
		{ID: "node-remote", Addr: ownerAddr, Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-gw"); err != nil {
		t.Fatal(err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatal(err)
	}
	router := NewRouter(store, "node-gw", partition.NewHashRoundRobin(), "")
	router.peer = peer
	if tokens {
		router.SetSelfAddr("node-gw.example:7942")
		keeperCtx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go router.RunTokenKeeper(keeperCtx)
	}
	br := &zzPerfCGatewayBroker{}
	set := handlers.New(handlers.Deps{
		Broker: br,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Router: router,
	})
	return router, br, httpmessaging.Consume(set)
}

// zzPerfCFill commits n records, each with its own payload, to one
// partition of the owner's "orders".
func zzPerfCFill(owner *zzWP18Owner, part, n int) error {
	records := make([]ingress.ProduceRecord, n)
	for i := range records {
		records[i] = ingress.ProduceRecord{Topic: "orders", TargetPartition: part, Payload: fmt.Appendf(nil, `{"n":%d}`, i)}
	}
	_, err := owner.engine.CommitAcceptedProduceBatch(context.Background(), records)
	return err
}

// zzPerfCConsume serves GET /v1/topics/orders/consume<query> on node-gw.
func zzPerfCConsume(consume http.HandlerFunc, query string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume"+query, nil)
	req.SetPathValue("topic", "orders")
	rec := httptest.NewRecorder()
	consume(rec, req)
	return rec
}

// zzPerfCAckAll checks msgs are distinct records of partition 1 and acks
// each at its owner through node-gw's router.
func zzPerfCAckAll(t *testing.T, router *Router, msgs []topic.Message) {
	t.Helper()
	seen := map[string]bool{}
	for _, m := range msgs {
		if m.ReceiptHandle == "" || seen[m.ReceiptHandle] || m.Partition != 1 {
			t.Fatalf("record %+v: want a distinct receipt handle on partition 1", m)
		}
		seen[m.ReceiptHandle] = true
		h, err := consumer.DecodeHandle(m.ReceiptHandle)
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		if !router.RouteAck(context.Background(), w, nil, "orders", h) || w.Code != http.StatusNoContent {
			t.Fatalf("forwarded ack of %s = %d, want 204", m.ReceiptHandle, w.Code)
		}
	}
}

// zzPerfCOnParked runs fn once a consumer is parked on node-gw's tokens
// for "orders", as an owner spending the token would.
func zzPerfCOnParked(router *Router, fn func() error) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		d := router.tokens.demandFor("orders")
		d.mu.Lock()
		parked := len(d.waiters) > 0
		d.mu.Unlock()
		if parked {
			return fn()
		}
		time.Sleep(2 * time.Millisecond)
	}
	return errors.New("no consumer parked on node-gw")
}

// TestPerfCLocalOwnerBatchProbeTakesABatch is a batch consume
// (GET /consume?max=20) through the HTTP handler on a node that owns one
// of the topic's partitions, which is empty, while the other owner, over
// real QUIC, holds 30 records. The fallback used to ask the other owners
// for one record, so the batch came back with one; it must ask for up
// to 20 and pass the owner's batch through, each record acking at the
// owner, with nothing lost: the other 10 are there for the next consume.
func TestPerfCLocalOwnerBatchProbeTakesABatch(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	if err := zzPerfCFill(owner, 1, 30); err != nil {
		t.Fatal(err)
	}
	addr, client := owner.serveQUIC(t)
	router, local, consume := zzPerfCGateway(t, addr, client, false)

	msgs := zzWP20Messages(t, zzPerfCConsume(consume, "?max=20"))
	if len(msgs) != 20 {
		t.Fatalf("local-owner batch consume returned %d records, want the owner's batch of 20", len(msgs))
	}
	if n := local.scans.Load(); n != 1 {
		t.Fatalf("node-gw scanned its partitions %d times, want once: an owner's batch goes out as it is", n)
	}
	zzPerfCAckAll(t, router, msgs)

	rest := zzWP20Messages(t, zzPerfCConsume(consume, "?max=20"))
	if len(rest) != 10 {
		t.Fatalf("the next batch consume returned %d records, want the other 10", len(rest))
	}
	zzPerfCAckAll(t, router, rest)
}

// TestPerfCLocalOwnerBatchWaitClaimTakesABatch is a batch long-poll
// (max=20, wait=1s) on a node that owns one empty partition of the
// topic, with the token protocol on. It parks; a burst of 5 lands on the
// other owner, which spends node-gw's token; the claim goes over real
// QUIC. The claim used to ask for one record, which a local scan then
// topped up with nothing; it must ask for up to 20 and pass the owner's
// batch through.
func TestPerfCLocalOwnerBatchWaitClaimTakesABatch(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	addr, client := owner.serveQUIC(t)
	router, local, consume := zzPerfCGateway(t, addr, client, true)

	done := make(chan error, 1)
	go func() {
		done <- zzPerfCOnParked(router, func() error {
			if err := zzPerfCFill(owner, 1, 5); err != nil {
				return err
			}
			if !router.LocalDemand().WakeOneWaiter("orders", addr) {
				return errors.New("no parked consumer took the owner's notification")
			}
			return nil
		})
	}()
	res := zzPerfCConsume(consume, "?max=20&wait=1s")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	msgs := zzWP20Messages(t, res)
	if len(msgs) <= 1 {
		t.Fatalf("parked local-owner batch consume returned %d records, want the burst as a batch", len(msgs))
	}
	if len(msgs) != 5 {
		t.Fatalf("parked local-owner batch consume returned %d records, want the burst of 5", len(msgs))
	}
	if n := local.scans.Load(); n != 1 {
		t.Fatalf("node-gw scanned its partitions %d times, want once: an owner's batch goes out as it is", n)
	}
	zzPerfCAckAll(t, router, msgs)
}

// TestPerfCLocalOwnerBatchLegacyOwner is the rolling-upgrade case: the
// other owner is on a release before Max and refuses it. The probe asks
// again for one record, which goes out as a one-message batch, 200, and
// the refusal is remembered, so the next batch consume asks that owner
// for one record straight away.
func TestPerfCLocalOwnerBatchLegacyOwner(t *testing.T) {
	peer := &zzWP18LegacyPeer{}
	_, _, consume := zzPerfCGateway(t, "remote.example:7942", peer, false)

	for round := range 2 {
		res := zzPerfCConsume(consume, "?max=20")
		msgs := zzWP20Messages(t, res)
		if len(msgs) != 1 || msgs[0].ReceiptHandle != "0:1:2" {
			t.Fatalf("round %d: legacy owner's batch = %+v, want its one record", round, msgs)
		}
		if ct := res.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("round %d: Content-Type %q, want application/json", round, ct)
		}
	}
	if got := peer.calls(); len(got) != 3 || got[0] != 20 || got[1] != 0 || got[2] != 0 {
		t.Fatalf("requests to a legacy owner carried Max %v, want [20 0 0]", got)
	}
}
