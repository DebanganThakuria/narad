package cluster

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
)

// zzWP18EngineBroker is an owner's broker for the RPC server: consumes
// (single, batch, waits), acks and nacks go to a real messaging engine.
type zzWP18EngineBroker struct {
	broker.Broker
	engine *brokermsg.Engine
	claims atomic.Int64
}

func (b *zzWP18EngineBroker) Consume(ctx context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return b.engine.Consume(ctx, topicName, opts)
}

func (b *zzWP18EngineBroker) ConsumeBatch(ctx context.Context, topicName string, opts brokermsg.ConsumeOpts, max int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	return b.engine.ConsumeBatch(ctx, topicName, opts, max, dst)
}

func (b *zzWP18EngineBroker) ConsumeWait(ctx context.Context, w *brokermsg.ConsumeWaiter, wait time.Duration, external <-chan struct{}) (topic.Message, bool, bool, error) {
	return b.engine.ConsumeWait(ctx, w, wait, external)
}

func (b *zzWP18EngineBroker) Ack(ctx context.Context, topicName string, h consumer.Handle) error {
	return b.engine.Ack(ctx, topicName, h)
}

func (b *zzWP18EngineBroker) Nack(ctx context.Context, topicName string, h consumer.Handle) error {
	return b.engine.Nack(ctx, topicName, h)
}

func (b *zzWP18EngineBroker) NoteRemoteClaim(topicName string) {
	b.claims.Add(1)
	b.engine.NoteRemoteClaim(topicName)
}

// zzWP18Owner is a node that owns every partition of "orders" (parts of
// them): a real engine and partition logs behind an RPC server.
type zzWP18Owner struct {
	engine *brokermsg.Engine
	broker *zzWP18EngineBroker
	server *RPCServer
}

func newZZWP18Owner(t *testing.T, parts int) *zzWP18Owner {
	t.Helper()
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: parts, VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 200, MaxAckedAheadPerPartition: 200}); err != nil {
		t.Fatal(err)
	}
	for p := range parts {
		if err := store.AssignPartition(ctx, "orders", p, "node-self"); err != nil {
			t.Fatal(err)
		}
	}
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 200, MaxAckedAhead: 200}, nil
	}, nil)
	engine := brokermsg.NewEngine(store, schema.NewAlwaysValid(), partition.NewHashRoundRobin(),
		offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
	br := &zzWP18EngineBroker{engine: engine}
	return &zzWP18Owner{engine: engine, broker: br, server: NewRPCServer(br, nil, nil)}
}

// fill commits n records to each of the owner's parts partitions.
func (o *zzWP18Owner) fill(t *testing.T, parts, n int, payload []byte) {
	t.Helper()
	for p := range parts {
		records := make([]ingress.ProduceRecord, n)
		for i := range records {
			records[i] = ingress.ProduceRecord{Topic: "orders", TargetPartition: p, Payload: payload}
		}
		if _, err := o.engine.CommitAcceptedProduceBatch(context.Background(), records); err != nil {
			t.Fatal(err)
		}
	}
}

// serveQUIC serves the owner's RPC server over QUIC on loopback and
// returns its address and a client for it.
func (o *zzWP18Owner) serveQUIC(t *testing.T) (string, *PeerClient) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = clusterrpc.ServeQUIC(ctx, addr, "", nil, o.server)
	}()
	client := NewPeerClient(5*time.Second, "")
	t.Cleanup(func() {
		_ = client.Close()
		cancel()
		<-served
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := client.Consume(context.Background(), addr, nodewire.ConsumeRequest{Topic: "absent", LocalOnly: true})
		if err == nil && res.Status != 0 {
			return addr, client
		}
		if time.Now().After(deadline) {
			t.Fatalf("QUIC owner never answered: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// newZZWP18Gateway is a router on node-gw, which owns none of "orders"'
// parts partitions: node-remote at ownerAddr owns them all.
func newZZWP18Gateway(t testing.TB, ownerAddr string, peer peerClient, parts int) *Router {
	t.Helper()
	ctx := context.Background()
	store := zzWP9Store(t)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: parts}); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: ownerAddr, Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}
	for p := range parts {
		if err := store.AssignPartition(ctx, "orders", p, "node-remote"); err != nil {
			t.Fatal(err)
		}
	}
	router := NewRouter(store, "node-gw", partition.NewHashRoundRobin(), "")
	router.peer = peer
	return router
}

// zzWP18GatewayBroker is the broker of a node that owns nothing of the
// topic: it has the batch consume surface, so the handler takes the batch
// path, and is never asked for a record.
type zzWP18GatewayBroker struct{ broker.Broker }

func (zzWP18GatewayBroker) ConsumeBatch(_ context.Context, _ string, _ brokermsg.ConsumeOpts, _ int, dst []topic.Message) ([]topic.Message, *brokermsg.ConsumeWaiter, error) {
	return dst, nil, brokermsg.ErrNotPartitionOwner
}

// TestZZWP18NonOwnerBatchConsumeServesTheBatch is a batch consume
// (GET /consume?max=20) through the HTTP handler on a node that owns none
// of the topic's partitions, forwarded over real QUIC to the owner. It
// used to come back with at most one record, since every forward asked
// the owner for one; the owner has 30 ready, so it must be 20, each
// with its own receipt handle that acks at the owner.
func TestZZWP18NonOwnerBatchConsumeServesTheBatch(t *testing.T) {
	owner := newZZWP18Owner(t, 2)
	owner.fill(t, 2, 15, []byte(`{"k":"v"}`))
	addr, client := owner.serveQUIC(t)
	router := newZZWP18Gateway(t, addr, client, 2)
	set := handlers.New(handlers.Deps{
		Broker: zzWP18GatewayBroker{},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Router: router,
	})
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?max=20", nil)
	req.SetPathValue("topic", "orders")
	rec := httptest.NewRecorder()
	httpmessaging.Consume(set)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("batch consume on a non-owner = %d %s, want 200", rec.Code, rec.Body)
	}
	var reply struct {
		Messages []topic.Message `json:"messages"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &reply); err != nil {
		t.Fatalf("reply %q: %v", rec.Body, err)
	}
	if len(reply.Messages) != 20 {
		t.Fatalf("batch consume on a non-owner returned %d records, want 20", len(reply.Messages))
	}
	seen := map[string]bool{}
	for _, m := range reply.Messages {
		if m.ReceiptHandle == "" || seen[m.ReceiptHandle] || string(m.Payload) != `{"k":"v"}` {
			t.Fatalf("record %+v: want a distinct receipt handle and the produced payload", m)
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
