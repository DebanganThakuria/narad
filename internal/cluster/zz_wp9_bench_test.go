package cluster

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzWP9Store is newTestStore for benchmarks too.
func zzWP9Store(tb testing.TB) *metastore.Store {
	tb.Helper()
	store, err := metastore.New(metastore.Config{NodeID: "node-self", DataDir: tb.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		tb.Fatalf("metastore.New() error = %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			zzWP9AwaitOwnershipView(tb, store)
			return store
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatal("timed out waiting for leader")
	return nil
}

// zzWP9AwaitOwnershipView sets the store's ownership latch and waits
// until it holds, as a node's readiness check does before the node
// takes traffic. The first ownership read sets the latch through a Raft
// barrier. A read that lands while that barrier is committed but not
// yet applied is refused (ErrUnavailable) rather than made to wait, and
// the router then leaves the request to the local broker instead of
// forwarding it. The coalescer tests open with a window's worth of
// concurrent first acks to one remote owner, so now and then one of
// them was not forwarded: the fake owner held one ack fewer than the
// test waited for, and the wait timed out (two tests in one CI run on a
// 2-core runner). Every store here is past the latch before a test
// uses it.
func zzWP9AwaitOwnershipView(tb testing.TB, store *metastore.Store) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !store.OwnershipViewReady() {
		if time.Now().After(deadline) {
			tb.Fatal("timed out waiting for the metastore's ownership view")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// zzWP9Router builds a router on node-self for "orders": partition 1 is
// local, partition 0 lives on node-remote at remoteAddr.
func zzWP9Router(tb testing.TB, remoteAddr string, peer peerClient) *Router {
	tb.Helper()
	store := zzWP9Store(tb)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		tb.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remoteAddr, Status: metastore.MemberAlive}); err != nil {
		tb.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		tb.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self"); err != nil {
		tb.Fatalf("AssignPartition(1) error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = peer
	return router
}

// zzWP9Writer mimics net/http's response writer where it matters for
// allocations: a fresh header map per request, cloned at WriteHeader
// once the handler has touched it.
type zzWP9Writer struct {
	h            http.Header
	calledHeader bool
	cloned       http.Header
	status       int
}

func (w *zzWP9Writer) Header() http.Header { w.calledHeader = true; return w.h }

func (w *zzWP9Writer) WriteHeader(code int) {
	w.status = code
	if w.calledHeader {
		w.cloned = w.h.Clone()
	}
}

func (w *zzWP9Writer) Write(p []byte) (int, error) { return len(p), nil }

// zzWP9Broker answers acks and non-blocking consumes at once.
type zzWP9Broker struct {
	broker.Broker
	msg   topic.Message
	found bool
}

func (b *zzWP9Broker) Ack(context.Context, string, consumer.Handle) error { return nil }

func (b *zzWP9Broker) Consume(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return b.msg, b.found, nil
}

func (b *zzWP9Broker) NoteRemoteClaim(string) {}

// zzWP9QUICPeer serves an RPCServer over real QUIC on loopback and
// returns its address and a client for it.
func zzWP9QUICPeer(tb testing.TB, br broker.Broker) (string, *PeerClient) {
	tb.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	server := NewRPCServer(br, nil, nil)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = clusterrpc.ServeQUIC(ctx, addr, "", nil, server)
	}()
	client := NewPeerClient(5*time.Second, "")
	tb.Cleanup(func() {
		_ = client.Close()
		cancel()
		<-served
	})
	ack := nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2}
	deadline := time.Now().Add(5 * time.Second)
	for {
		res, err := client.Ack(context.Background(), addr, ack)
		if err == nil && res.Status == http.StatusNoContent {
			return addr, client
		}
		if time.Now().After(deadline) {
			tb.Fatalf("QUIC peer never answered: %v %+v", err, res)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// zzWP9Frames is a frameTransport that answers every request with a
// canned reply, watching ctx the way the real client does.
type zzWP9Frames struct{ reply []byte }

func (f zzWP9Frames) RequestOnLane(ctx context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, _ []byte) (clusterwire.StreamFrame, error) {
	select {
	case <-ctx.Done():
		return clusterwire.StreamFrame{}, ctx.Err()
	default:
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, Payload: f.reply}, nil
}

func (f zzWP9Frames) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return f.RequestOnLane(ctx, addr, lane, frameType, payload)
}

func zzWP9Reply(tb testing.TB, res nodewire.Response) []byte {
	tb.Helper()
	payload, err := nodewire.EncodeResponse(res)
	if err != nil {
		tb.Fatal(err)
	}
	return payload
}

// BenchmarkZZWP9RouteAckFake is a forwarded ack through the router and
// PeerClient over an in-memory transport: the router's own per-ack cost.
func BenchmarkZZWP9RouteAckFake(b *testing.B) {
	peer := &PeerClient{frames: zzWP9Frames{reply: zzWP9Reply(b, nodewire.Response{Status: http.StatusNoContent})}}
	router := zzWP9Router(b, "remote.example:7942", peer)
	h := consumer.Handle{Partition: 0, Offset: 42, Nonce: 7}
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	router.RouteAck(base, &zzWP9Writer{h: http.Header{}}, nil, "orders", h)
	b.ReportAllocs()
	for b.Loop() {
		// A per-request cancel context, as net/http gives each request.
		ctx, cancel := context.WithCancel(base)
		router.RouteAck(ctx, &zzWP9Writer{h: make(http.Header)}, nil, "orders", h)
		cancel()
	}
}

// BenchmarkZZWP9RouteAckQUIC is a forwarded ack end to end: router,
// PeerClient, real QUIC on loopback, RPC server, broker.
func BenchmarkZZWP9RouteAckQUIC(b *testing.B) {
	addr, client := zzWP9QUICPeer(b, &zzWP9Broker{})
	router := zzWP9Router(b, addr, client)
	h := consumer.Handle{Partition: 0, Offset: 42, Nonce: 7}
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	router.RouteAck(base, &zzWP9Writer{h: http.Header{}}, nil, "orders", h)
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithCancel(base)
		w := &zzWP9Writer{h: make(http.Header)}
		router.RouteAck(ctx, w, nil, "orders", h)
		cancel()
		if w.status != http.StatusNoContent {
			b.Fatalf("status = %d", w.status)
		}
	}
}

// BenchmarkZZWP9ProbeQUIC is one empty remote probe end to end, the unit
// of work a parked consumer's owner scan costs.
func BenchmarkZZWP9ProbeQUIC(b *testing.B) {
	addr, client := zzWP9QUICPeer(b, &zzWP9Broker{})
	router := zzWP9Router(b, addr, client)
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithCancel(base)
		forwarded, had := router.RouteConsumeRemote(ctx, &zzWP9Writer{h: make(http.Header)}, req, "orders")
		cancel()
		if forwarded || !had {
			b.Fatalf("RouteConsumeRemote() = (%v, %v)", forwarded, had)
		}
	}
}

// BenchmarkZZWP9ForwardedConsumeQUIC is a remote probe that wins a
// record: the reply carries a body, so the content headers are written.
func BenchmarkZZWP9ForwardedConsumeQUIC(b *testing.B) {
	msg := topic.Message{Topic: "orders", Partition: 0, Offset: 7, Payload: []byte(`{"k":"v","n":1234567}`), ReceiptHandle: "0:7:9"}
	addr, client := zzWP9QUICPeer(b, &zzWP9Broker{msg: msg, found: true})
	router := zzWP9Router(b, addr, client)
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	b.ReportAllocs()
	for b.Loop() {
		ctx, cancel := context.WithCancel(base)
		w := &zzWP9Writer{h: make(http.Header)}
		forwarded, _ := router.RouteConsumeRemote(ctx, w, req, "orders")
		cancel()
		if !forwarded || w.status != http.StatusOK {
			b.Fatalf("forwarded=%v status=%d", forwarded, w.status)
		}
	}
}

// BenchmarkZZWP9ServerAck is the owner's side of an ack: dispatch,
// messaging slot, broker, reply encoding.
func BenchmarkZZWP9ServerAck(b *testing.B) {
	s := NewRPCServer(&zzWP9Broker{}, nil, nil)
	payload, err := nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2})
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan int, 1)
	respond := func(f clusterwire.StreamFrame) { done <- len(f.Payload) }
	frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}
	b.ReportAllocs()
	for b.Loop() {
		s.HandleStreamRequest(context.Background(), frame, respond)
		<-done
	}
}

// BenchmarkZZWP9ServerProbe is the owner's side of an empty probe.
func BenchmarkZZWP9ServerProbe(b *testing.B) {
	s := NewRPCServer(&zzWP9Broker{}, nil, nil)
	payload, err := nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan int, 1)
	respond := func(f clusterwire.StreamFrame) { done <- len(f.Payload) }
	frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}
	b.ReportAllocs()
	for b.Loop() {
		s.HandleStreamRequest(context.Background(), frame, respond)
		<-done
	}
}

// zzWP9Found is a local wait that finds a record at once.
type zzWP9Found struct{ msg topic.Message }

func (f zzWP9Found) Wait(context.Context, time.Duration, <-chan struct{}) (topic.Message, bool, bool, error) {
	return f.msg, true, false, nil
}

func (zzWP9Found) Release(context.Context, topic.Message) error { return nil }

// BenchmarkZZWP9ParkServedLocally is the token bookkeeping of a consume
// that parks on an owning node and is then served by its own
// partitions: park, register, unpark (and, before lazy retirement, a
// drop to every other owner).
func BenchmarkZZWP9ParkServedLocally(b *testing.B) {
	peer := &PeerClient{frames: zzWP9Frames{reply: zzWP9Reply(b, nodewire.Response{Status: http.StatusNoContent})}}
	router := zzWP9Router(b, "remote.example:7942", peer)
	router.SetSelfAddr("node-self.example:7942")
	local := zzWP9Found{msg: topic.Message{Topic: "orders", Partition: 1, Offset: 3, Payload: []byte(`{}`), ReceiptHandle: "1:3:5"}}
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)
	b.ReportAllocs()
	for b.Loop() {
		if !router.RouteConsumeWait(context.Background(), &zzWP9Writer{h: make(http.Header)}, req, "orders", 5*time.Second, local) {
			b.Fatal("RouteConsumeWait() = false")
		}
	}
}

// zzWP9ServerParallel drives the owner's side of one op from many
// goroutines at once, the way a busy owner serves its peers.
func zzWP9ServerParallel(b *testing.B, payload []byte) {
	s := NewRPCServer(&zzWP9Broker{}, nil, nil)
	frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		done := make(chan int, 1)
		respond := func(f clusterwire.StreamFrame) { done <- len(f.Payload) }
		for pb.Next() {
			s.HandleStreamRequest(context.Background(), frame, respond)
			<-done
		}
	})
}

func BenchmarkZZWP9ServerAckParallel(b *testing.B) {
	payload, err := nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2})
	if err != nil {
		b.Fatal(err)
	}
	zzWP9ServerParallel(b, payload)
}

func BenchmarkZZWP9ServerProbeParallel(b *testing.B) {
	payload, err := nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	zzWP9ServerParallel(b, payload)
}

// zzWP9EngineBroker serves consumes from a real messaging engine.
type zzWP9EngineBroker struct {
	broker.Broker
	engine *brokermsg.Engine
}

func (b *zzWP9EngineBroker) Consume(ctx context.Context, topicName string, opts brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return b.engine.Consume(ctx, topicName, opts)
}

func (b *zzWP9EngineBroker) Ack(ctx context.Context, topicName string, h consumer.Handle) error {
	return b.engine.Ack(ctx, topicName, h)
}

func (b *zzWP9EngineBroker) NoteRemoteClaim(topicName string) { b.engine.NoteRemoteClaim(topicName) }

// zzWP9Engine builds a real messaging engine over real partition logs
// for "orders", three partitions all owned here, and an RPC server on it.
func zzWP9Engine(b *testing.B) (*RPCServer, *brokermsg.Engine) {
	ctx := context.Background()
	store := zzWP9Store(b)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3, VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64}); err != nil {
		b.Fatal(err)
	}
	for p := range 3 {
		if err := store.AssignPartition(ctx, "orders", p, "node-self"); err != nil {
			b.Fatal(err)
		}
	}
	logs := runtime.NewLogs(b.TempDir(), storage.Options{FlushInterval: time.Millisecond}, store, nil)
	b.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, nil)
	engine := brokermsg.NewEngine(store, schema.NewAlwaysValid(), partition.NewHashRoundRobin(),
		offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
	return NewRPCServer(&zzWP9EngineBroker{engine: engine}, nil, nil), engine
}

// BenchmarkZZWP9ServerProbeEngine is the owner's side of an empty probe
// against a real messaging engine and partition logs, so the request
// goroutine's stack is as deep as it is in production.
func BenchmarkZZWP9ServerProbeEngine(b *testing.B) {
	s, _ := zzWP9Engine(b)
	payload, err := nodewire.EncodeConsumeRequest(nodewire.ConsumeRequest{Topic: "orders", LocalOnly: true})
	if err != nil {
		b.Fatal(err)
	}
	done := make(chan int, 1)
	respond := func(f clusterwire.StreamFrame) { done <- len(f.Payload) }
	frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}
	s.HandleStreamRequest(context.Background(), frame, respond)
	<-done
	b.ReportAllocs()
	for b.Loop() {
		s.HandleStreamRequest(context.Background(), frame, respond)
		<-done
	}
}

// BenchmarkZZWP9ServerAckEngine is the owner's side of a forwarded ack
// against a real engine. Each op also reserves the record it acks,
// directly on the engine, the same in every version measured.
func BenchmarkZZWP9ServerAckEngine(b *testing.B) {
	s, engine := zzWP9Engine(b)
	ctx := context.Background()
	batch := make([]ingress.ProduceRecord, 20_000)
	for i := range batch {
		batch[i] = ingress.ProduceRecord{Topic: "orders", TargetPartition: 0, Payload: []byte(`{}`)}
	}
	done := make(chan int, 1)
	var codes [1]int
	ackOnce := func() {
		msg, found, err := engine.Consume(ctx, "orders", brokermsg.ConsumeOpts{})
		if err != nil {
			b.Fatal(err)
		}
		if !found {
			if _, err := engine.CommitAcceptedProduceBatch(ctx, batch); err != nil {
				b.Fatal(err)
			}
			if msg, found, err = engine.Consume(ctx, "orders", brokermsg.ConsumeOpts{}); err != nil || !found {
				b.Fatalf("Consume() = %v, %v after a produce", found, err)
			}
		}
		h, err := consumer.DecodeHandle(msg.ReceiptHandle)
		if err != nil {
			b.Fatal(err)
		}
		payload, err := nodewire.EncodeAckRequest(nodewire.AckRequest{Topic: "orders", Partition: h.Partition, Offset: h.Offset, Nonce: h.Nonce})
		if err != nil {
			b.Fatal(err)
		}
		s.HandleStreamRequest(ctx, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}, func(f clusterwire.StreamFrame) {
			res, _ := nodewire.DecodeResponse(f.Payload)
			codes[0] = res.Status
			done <- 0
		})
		<-done
		if codes[0] != http.StatusNoContent {
			b.Fatalf("ack status = %d", codes[0])
		}
	}
	ackOnce()
	b.ReportAllocs()
	for b.Loop() {
		ackOnce()
	}
}
