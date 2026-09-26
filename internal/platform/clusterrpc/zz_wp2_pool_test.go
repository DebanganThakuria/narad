package clusterrpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2ParkHandler: payload "park" parks on a goroutine until release is
// closed, "block" blocks the stream's serve loop for blockFor (a slow
// reader), anything else is echoed.
type zzWP2ParkHandler struct {
	release  chan struct{}
	parked   chan struct{}
	once     sync.Once
	blockFor time.Duration
}

func newZZWP2ParkHandler() *zzWP2ParkHandler {
	return &zzWP2ParkHandler{release: make(chan struct{}), parked: make(chan struct{})}
}

func (h *zzWP2ParkHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	switch string(frame.Payload) {
	case "park":
		go func() {
			h.once.Do(func() { close(h.parked) })
			<-h.release
			respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: []byte("unparked")})
		}()
	case "block":
		time.Sleep(h.blockFor)
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID})
	default:
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: frame.Payload})
	}
	return true
}

// zzWP2Server serves h on a real QUIC listener with session-bound auth.
func zzWP2Server(t *testing.T, h StreamFrameHandler) string {
	t.Helper()
	server, err := listenQUIC("127.0.0.1:0", "sekret", false)
	if err != nil {
		t.Fatalf("listenQUIC: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveQUICListener(ctx, server.listener, listenerAuth{secret: "sekret"}, slog.New(slog.NewTextHandler(io.Discard, nil)), h)
	}()
	t.Cleanup(func() { cancel(); server.close(); <-done })
	return server.addr().String()
}

// zzWP2Go runs fn on a goroutine and returns a channel with its error.
func zzWP2Go(fn func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return ch
}

func zzWP2Pending(t *testing.T, ch <-chan error, what string) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("%s finished early: %v", what, err)
	case <-time.After(300 * time.Millisecond):
	}
}

func zzWP2Result(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", what)
		return nil
	}
}

// zzWP2BlockingDial never connects: it returns only when its context
// ends, reporting when that was.
func zzWP2BlockingDial(ended chan<- time.Time) func(context.Context, string) (poolConn, error) {
	return func(ctx context.Context, _ string) (poolConn, error) {
		<-ctx.Done()
		ended <- time.Now()
		return nil, ctx.Err()
	}
}

// The shared dial is bounded by the pool's dial timeout, not by any
// caller: a caller with a long budget still sees an unreachable peer
// fail at the dial timeout, and that failure arms the backoff.
func TestZZWP2DialTimeoutIsThePools(t *testing.T) {
	ended := make(chan time.Time, 1)
	p := newFakePool(t, 5*time.Second, zzWP2BlockingDial(ended))
	p.dialTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := p.getConn(ctx, "peer:1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dial error = %v, want the dial timeout", err)
	}
	if took := (<-ended).Sub(start); took > time.Second {
		t.Fatalf("dial ran %s, want about the 50ms dial timeout", took)
	}
	if _, err := p.getConn(ctx, "peer:1"); err == nil || !strings.Contains(err.Error(), "backing off") {
		t.Fatalf("second dial error = %v, want a cached backoff", err)
	}
}

// A caller whose own deadline is shorter than the dial gives up at its
// deadline; the dial carries on for the others.
func TestZZWP2ShortCallerDoesNotWaitOutTheDial(t *testing.T) {
	ended := make(chan time.Time, 1)
	p := newFakePool(t, 5*time.Second, zzWP2BlockingDial(ended))
	p.dialTimeout = 400 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := p.getConn(ctx, "peer:1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("short caller error = %v, want its own deadline", err)
	}
	if waited := time.Since(start); waited > 300*time.Millisecond {
		t.Fatalf("short caller waited %s for a dial it gave up on", waited)
	}
	select {
	case at := <-ended:
		t.Fatalf("dial ended %s after start, before its own timeout", at.Sub(start))
	case <-time.After(100 * time.Millisecond):
	}
}

// Closing the pool cancels a dial in flight and fails its waiters.
func TestZZWP2CloseCancelsDialInFlight(t *testing.T) {
	ended := make(chan time.Time, 1)
	p := newFakePool(t, 5*time.Second, zzWP2BlockingDial(ended))
	p.dialTimeout = time.Minute

	waiter := zzWP2Go(func() error {
		_, err := p.getConn(context.Background(), "peer:1")
		return err
	})
	time.Sleep(20 * time.Millisecond)
	_ = p.close()
	if err := zzWP2Result(t, waiter, "dial waiter"); !errors.Is(err, errPoolClosed) {
		t.Fatalf("dial waiter error = %v, want errPoolClosed", err)
	}
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("dial kept running after the pool closed")
	}
}

// cluster-rpc-transport#4: a dial leader with an already-cancelled
// context must not arm the shared dial backoff for everyone else.
func TestZZWP2CancelledDialLeaderDoesNotArmBackoff(t *testing.T) {
	addr := zzWP2Server(t, newZZWP2ParkHandler())
	pool := newQUICClientPool(5*time.Second, "sekret", false)
	t.Cleanup(func() { _ = pool.close() })

	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	probeCtx, cancelProbe := context.WithTimeout(parent, 500*time.Millisecond)
	defer cancelProbe()
	if _, err := pool.request(probeCtx, addr, LaneConsume, clusterwire.StreamFrameNodeRequest, []byte("probe")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled leader error = %v, want context.Canceled", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pool.request(ctx, addr, LaneAck, clusterwire.StreamFrameNodeRequest, []byte("ack")); err != nil {
		t.Fatalf("healthy caller after a cancelled leader: %v", err)
	}
}

// cluster-rpc-transport#4: the dial leader is cancelled mid-dial. It gives
// up alone; a follower with a healthy context gets the connection once
// the dial completes, and no failure is cached.
func TestZZWP2DialLeaderCancelMidDialSparesFollower(t *testing.T) {
	conn := newFakePoolConn()
	go serveEcho(conn)
	release := make(chan struct{})
	entered := make(chan struct{}, 4)
	var dials atomic.Int32
	p := newFakePool(t, 5*time.Second, func(ctx context.Context, _ string) (poolConn, error) {
		dials.Add(1)
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return conn, nil
		}
	})

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leader := zzWP2Go(func() error {
		_, err := p.getConn(leaderCtx, "peer:1")
		return err
	})
	<-entered
	follower := zzWP2Go(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := p.getConn(ctx, "peer:1")
		return err
	})
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	if err := zzWP2Result(t, leader, "leader"); !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	close(release)
	if err := zzWP2Result(t, follower, "follower"); err != nil {
		t.Fatalf("follower (healthy context) error = %v", err)
	}
	if _, err := p.getConn(context.Background(), "peer:1"); err != nil {
		t.Fatalf("next caller error = %v", err)
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("dials = %d, want 1", n)
	}
	p.mu.Lock()
	_, cached := p.dialFail["peer:1"]
	p.mu.Unlock()
	if cached {
		t.Fatal("a cancelled leader left a cached dial failure")
	}
}

// cluster-rpc-transport#4: a dial that genuinely fails (the peer is
// unreachable) still arms the backoff.
func TestZZWP2GenuineDialFailureStillBacksOff(t *testing.T) {
	p := newFakePool(t, 5*time.Second, func(context.Context, string) (poolConn, error) {
		return nil, context.DeadlineExceeded
	})
	if _, err := p.getConn(context.Background(), "peer:1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first dial error = %v, want the dial timeout", err)
	}
	_, err := p.getConn(context.Background(), "peer:1")
	if err == nil || !strings.Contains(err.Error(), "backing off") {
		t.Fatalf("second dial error = %v, want a cached backoff", err)
	}
}
