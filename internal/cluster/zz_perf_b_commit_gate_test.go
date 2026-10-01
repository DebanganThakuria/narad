package cluster

import (
	"context"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// zzPerfBCancelWithin is how soon a commit cancelled while it waits for
// a slot must be answered: a waiter that watches its context wakes in
// microseconds, and one that does not stays parked until a slot frees.
const zzPerfBCancelWithin = 50 * time.Millisecond

// zzPerfBBroker counts the commits that reach it. A non-nil hold makes
// every commit wait on it (a stalled disk; the context is ignored, as a
// commit already inside an fsync would) and reports on started as it
// begins. Acks answer at once so zzPerfBQUICPeer can see it is up.
type zzPerfBBroker struct {
	zzWP9Broker
	hold    chan struct{}
	started chan struct{}
	commits atomic.Int32
}

func (b *zzPerfBBroker) CommitAcceptedProduce(ctx context.Context, record ingress.ProduceRecord) (int64, error) {
	offsets, err := b.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{record})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

func (b *zzPerfBBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	b.commits.Add(1)
	if b.hold != nil {
		select {
		case b.started <- struct{}{}:
		default:
		}
		<-b.hold
	}
	return make([]int64, len(records)), nil
}

// zzPerfBDoneCtx counts its Done calls and closes asked on the first,
// which is how a handler parking on it shows itself.
type zzPerfBDoneCtx struct {
	context.Context
	dones atomic.Int32
	once  sync.Once
	asked chan struct{}
}

func zzPerfBNewDoneCtx(parent context.Context) *zzPerfBDoneCtx {
	return &zzPerfBDoneCtx{Context: parent, asked: make(chan struct{})}
}

func (c *zzPerfBDoneCtx) Done() <-chan struct{} {
	c.dones.Add(1)
	c.once.Do(func() { close(c.asked) })
	return c.Context.Done()
}

// zzPerfBServe runs payload through s the way the stream server does
// and returns a channel that carries the decoded reply.
func zzPerfBServe(ctx context.Context, s *RPCServer, payload []byte) <-chan nodewire.Response {
	done := make(chan nodewire.Response, 1)
	s.HandleStreamRequest(ctx, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: payload}, func(f clusterwire.StreamFrame) {
		res, err := nodewire.DecodeResponse(f.Payload)
		if err != nil {
			panic(err)
		}
		// The reply buffer is recycled once respond returns.
		res.Body = slices.Clone(res.Body)
		done <- res
	})
	return done
}

// zzPerfBCommitOps are the two commit ops the owner gates on commitSem,
// each with a one-record request.
func zzPerfBCommitOps(t *testing.T) []struct {
	name    string
	payload []byte
} {
	t.Helper()
	record := nodewire.CommitProduceRequest{Topic: "orders", Key: "k", Payload: []byte(`{"id":1}`)}
	single, err := nodewire.EncodeCommitProduceRequest(record)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := nodewire.EncodeCommitProduceBatchRequest(nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{record}})
	if err != nil {
		t.Fatal(err)
	}
	return []struct {
		name    string
		payload []byte
	}{
		{"commit_produce", single},
		{"commit_produce_batch", batch},
	}
}

// A commit whose requester gives up while it waits for a commit slot is
// answered 503 at once, without reaching the broker, instead of staying
// parked with its records until a slot frees. The slot it waited for is
// intact: once freed, the next commit takes it and is applied.
func TestPerfBCommitSlotRefusesCancelledWaiter(t *testing.T) {
	for _, op := range zzPerfBCommitOps(t) {
		t.Run(op.name, func(t *testing.T) {
			br := &zzPerfBBroker{}
			s := NewRPCServer(br, nil, nil)
			s.SetMessagingConcurrency(1)
			s.commitSem <- struct{}{} // every commit slot taken

			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := zzPerfBNewDoneCtx(parent)
			reply := zzPerfBServe(ctx, s, op.payload)
			select {
			case <-ctx.asked: // parked on the slot, watching ctx
			case <-reply:
				t.Fatal("a commit was answered while every commit slot was taken")
			case <-time.After(time.Second):
				// Not watching ctx: cancel anyway and let the check
				// below show that it stays parked.
			}
			cancel()
			var res nodewire.Response
			select {
			case res = <-reply:
			case <-time.After(zzPerfBCancelWithin):
				<-s.commitSem // let the parked commit through so it does not leak
				t.Fatalf("%s cancelled while waiting for a commit slot was still parked %s later", op.name, zzPerfBCancelWithin)
			}
			if res.Status != http.StatusServiceUnavailable || !strings.Contains(string(res.Body), "waiting for a commit slot") {
				t.Fatalf("cancelled %s: %d %q, want 503 for the commit slot wait", op.name, res.Status, res.Body)
			}
			if n := br.commits.Load(); n != 0 {
				t.Fatalf("a cancelled %s reached the broker %d times", op.name, n)
			}
			if n := len(s.commitSem); n != 1 {
				t.Fatalf("commit slots held after the refusal = %d, want the 1 the test holds", n)
			}

			<-s.commitSem
			select {
			case res = <-zzPerfBServe(context.Background(), s, op.payload):
			case <-time.After(5 * time.Second):
				t.Fatalf("%s with a free slot was never answered", op.name)
			}
			if res.Status != http.StatusOK {
				t.Fatalf("%s with a free slot: %d %q, want 200", op.name, res.Status, res.Body)
			}
			if n := br.commits.Load(); n != 1 {
				t.Fatalf("broker commits = %d, want the 1 with a free slot", n)
			}
			if n := len(s.commitSem); n != 0 {
				t.Fatalf("commit slots held after the commit = %d, want 0", n)
			}
		})
	}
}

// An uncontended commit takes its slot with one non-blocking send and
// never asks the request context for its Done channel, which the
// transport makes only on demand.
func TestPerfBCommitSlotUncontendedDoesNotAskForDone(t *testing.T) {
	for _, op := range zzPerfBCommitOps(t) {
		t.Run(op.name, func(t *testing.T) {
			br := &zzPerfBBroker{}
			s := NewRPCServer(br, nil, nil)
			ctx := zzPerfBNewDoneCtx(context.Background())
			var res nodewire.Response
			select {
			case res = <-zzPerfBServe(ctx, s, op.payload):
			case <-time.After(5 * time.Second):
				t.Fatalf("uncontended %s was never answered", op.name)
			}
			if res.Status != http.StatusOK {
				t.Fatalf("uncontended %s: %d %q, want 200", op.name, res.Status, res.Body)
			}
			if n := br.commits.Load(); n != 1 {
				t.Fatalf("broker commits = %d, want 1", n)
			}
			if n := ctx.dones.Load(); n != 0 {
				t.Fatalf("an uncontended %s asked its context for Done %d times, want 0", op.name, n)
			}
			if n := len(s.commitSem); n != 0 {
				t.Fatalf("commit slots held after the commit = %d, want 0", n)
			}
		})
	}
}

// zzPerfBReplyTap is an RPCServer that also copies every commit reply it
// writes to replies, so a test sees a parked commit handler return even
// though its client has stopped reading.
type zzPerfBReplyTap struct {
	*RPCServer
	replies chan nodewire.Response
}

func (tap *zzPerfBReplyTap) HandleStreamRequest(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if op, err := nodewire.OperationOf(frame.Payload); err != nil || op != nodewire.OpCommitProduceBatch {
		return tap.RPCServer.HandleStreamRequest(ctx, frame, respond)
	}
	return tap.RPCServer.HandleStreamRequest(ctx, frame, func(f clusterwire.StreamFrame) {
		respond(f)
		res, err := nodewire.DecodeResponse(f.Payload)
		if err != nil {
			res = nodewire.Response{Status: -1, Body: []byte(err.Error())}
		}
		// The reply buffer is recycled once respond returns.
		res.Body = slices.Clone(res.Body)
		tap.replies <- res
	})
}

// zzPerfBQUICPeer serves handler over real QUIC on loopback, like
// zzWP9QUICPeer but for a server the test configured, and returns its
// address and a client for it.
func zzPerfBQUICPeer(tb testing.TB, handler clusterrpc.StreamFrameHandler) (string, *PeerClient) {
	tb.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = clusterrpc.ServeQUIC(ctx, addr, "", nil, handler)
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

// Over real QUIC, with one commit slot held by a commit stuck on the
// disk, the commits queued behind it return as soon as their clients'
// budgets run out and the cancels arrive: they do not stay parked, each
// holding its frame, until the disk recovers. Only the slot holder ever
// reaches the broker.
func TestPerfBCancelledRemoteCommitFreesTheOwner(t *testing.T) {
	const (
		queued = 4
		budget = 300 * time.Millisecond
	)
	owner := &zzPerfBBroker{hold: make(chan struct{}), started: make(chan struct{}, 1)}
	server := NewRPCServer(owner, nil, nil)
	server.SetMessagingConcurrency(1)
	tap := &zzPerfBReplyTap{RPCServer: server, replies: make(chan nodewire.Response, queued+1)}
	addr, client := zzPerfBQUICPeer(t, tap)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(owner.hold) }) }
	t.Cleanup(release) // runs before the peer shuts down, so no handler leaks

	req := nodewire.CommitProduceBatchRequest{Records: []nodewire.CommitProduceRequest{{Topic: "orders", Key: "k", Payload: []byte(`{"id":1}`)}}}
	var clients sync.WaitGroup
	send := func() {
		clients.Add(1)
		go func() {
			defer clients.Done()
			res, err := client.CommitProduceBatchWithin(context.Background(), addr, budget, req)
			if err == nil {
				t.Errorf("a commit to a stalled owner answered %d %q within its budget", res.Status, res.Body)
			}
		}()
	}
	send()
	select {
	case <-owner.started: // the slot holder is stuck in the broker
	case <-time.After(5 * time.Second):
		t.Fatal("the owner never saw the first commit")
	}
	for range queued {
		send()
	}
	clients.Wait() // every client's budget has run out and it sent its cancel

	deadline := time.After(2 * time.Second)
	for i := range queued {
		select {
		case res := <-tap.replies:
			if res.Status != http.StatusServiceUnavailable || !strings.Contains(string(res.Body), "waiting for a commit slot") {
				t.Fatalf("queued commit reply %d: %d %q, want 503 for the commit slot wait", i, res.Status, res.Body)
			}
		case <-deadline:
			t.Fatalf("%d of %d queued commits still parked on the owner's commit slot 2s after their clients gave up", queued-i, queued)
		}
	}
	if n := owner.commits.Load(); n != 1 {
		t.Fatalf("broker commits while the slot holder is stuck = %d, want 1", n)
	}

	release()
	select {
	case res := <-tap.replies:
		if res.Status != http.StatusOK {
			t.Fatalf("slot holder's reply: %d %q, want 200", res.Status, res.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the slot holder never answered after the disk recovered")
	}
	if n := owner.commits.Load(); n != 1 {
		t.Fatalf("broker commits = %d, want only the slot holder's", n)
	}
	if n := len(server.commitSem); n != 0 {
		t.Fatalf("commit slots held at the end = %d, want 0", n)
	}
}
