package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// fakePoolConn is an in-memory poolConn: every opened stream is one end
// of a net.Pipe, the other end is handed to the test via serverEnds.
type fakePoolConn struct {
	serverEnds chan net.Conn
	closeOnce  sync.Once
	closedCh   chan struct{}
	closeCause error
}

func newFakePoolConn() *fakePoolConn {
	return &fakePoolConn{serverEnds: make(chan net.Conn, 64), closedCh: make(chan struct{})}
}

func (c *fakePoolConn) openStream(context.Context) (streamConn, error) {
	client, server := net.Pipe()
	c.serverEnds <- server
	return client, nil
}

func (c *fakePoolConn) done() <-chan struct{} { return c.closedCh }

func (c *fakePoolConn) close(cause error) {
	c.closeOnce.Do(func() {
		c.closeCause = cause
		close(c.closedCh)
	})
}

func (c *fakePoolConn) isClosed() bool {
	select {
	case <-c.closedCh:
		return true
	default:
		return false
	}
}

// serveEcho answers every stream the fake connection opens with the
// echo handler, until the connection closes.
func serveEcho(c *fakePoolConn) {
	for {
		select {
		case server := <-c.serverEnds:
			go ServeStreamConn(server, server, "", nil, echoHandler{})
		case <-c.closedCh:
			return
		}
	}
}

// newFakePool returns a pool whose dialer is dial, with a short timeout
// so tests that exercise the fallback path stay fast.
func newFakePool(t *testing.T, timeout time.Duration, dial func(ctx context.Context, addr string) (poolConn, error)) *quicClientPool {
	t.Helper()
	p := newQUICClientPool(timeout, "")
	p.dial = dial
	p.pingTimeout = timeout
	t.Cleanup(func() { _ = p.close() })
	return p
}

// Concurrent requests to an address with no pooled connection must share
// a single dial rather than each dialing on their own.
func TestPoolDialIsSingleflight(t *testing.T) {
	var dials atomic.Int32
	release := make(chan struct{})
	conn := newFakePoolConn()
	go serveEcho(conn)
	p := newFakePool(t, 5*time.Second, func(ctx context.Context, addr string) (poolConn, error) {
		dials.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return conn, nil
	})

	const callers = 8
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Go(func() {
			frame, err := p.request(context.Background(), "peer:1", LaneControl, clusterwire.StreamFrameNodeRequest, []byte("hi"))
			if err == nil && string(frame.Payload) != "hi" {
				err = errors.New("echo mismatch")
			}
			errs[i] = err
		})
	}
	// Let every caller reach the dial before releasing it.
	deadline := time.Now().Add(2 * time.Second)
	for dials.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("caller %d error = %v", i, err)
		}
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("dials = %d, want 1 (singleflight)", n)
	}
}

// After a failed dial, requests fail fast with the cached error until the
// backoff elapses; consecutive failures double the backoff up to the cap,
// and a successful dial clears it.
func TestPoolDialFailureBacksOff(t *testing.T) {
	var dials atomic.Int32
	dialErr := errors.New("connection refused")
	fail := true
	conn := newFakePoolConn()
	go serveEcho(conn)
	p := newFakePool(t, time.Second, func(context.Context, string) (poolConn, error) {
		dials.Add(1)
		if fail {
			return nil, dialErr
		}
		return conn, nil
	})
	now := time.Unix(1_000_000, 0)
	p.now = func() time.Time { return now }

	request := func() error {
		_, err := p.request(context.Background(), "peer:1", LaneControl, clusterwire.StreamFrameNodeRequest, []byte("x"))
		return err
	}
	backoff := func() time.Duration {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.dialFail["peer:1"].backoff
	}

	if err := request(); !errors.Is(err, dialErr) {
		t.Fatalf("first request error = %v, want dial error", err)
	}
	if got := backoff(); got != quicDialBackoffMin {
		t.Fatalf("backoff after first failure = %s, want %s", got, quicDialBackoffMin)
	}
	// Inside the window: no dial, cached error.
	if err := request(); !errors.Is(err, dialErr) {
		t.Fatalf("fail-fast request error = %v, want cached dial error", err)
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("dials inside backoff window = %d, want 1", n)
	}

	want := quicDialBackoffMin
	for range 4 {
		now = now.Add(want + time.Millisecond)
		if err := request(); !errors.Is(err, dialErr) {
			t.Fatalf("request after backoff error = %v, want dial error", err)
		}
		want = min(want*2, quicDialBackoffMax)
		if got := backoff(); got != want {
			t.Fatalf("backoff = %s, want %s", got, want)
		}
	}
	if got := backoff(); got != quicDialBackoffMax {
		t.Fatalf("backoff cap = %s, want %s", got, quicDialBackoffMax)
	}

	fail = false
	now = now.Add(quicDialBackoffMax + time.Millisecond)
	if err := request(); err != nil {
		t.Fatalf("request after recovery error = %v", err)
	}
	if got := backoff(); got != 0 {
		t.Fatalf("backoff after successful dial = %s, want cleared", got)
	}
}

// silentServer reads frames off every stream of conn and never answers
// requests. It answers pings iff answerPings is set.
func silentServer(t *testing.T, conn *fakePoolConn, answerPings bool) {
	t.Helper()
	for {
		select {
		case server := <-conn.serverEnds:
			go func() {
				reader := bufio.NewReader(server)
				for {
					frame, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
					if err != nil {
						return
					}
					if frame.Type == clusterwire.StreamFramePing && answerPings {
						_ = clusterwire.WriteStreamFrame(server, clusterwire.StreamFrame{Type: clusterwire.StreamFramePong, RequestID: frame.RequestID})
					}
				}
			}()
		case <-conn.closedCh:
			return
		}
	}
}

// A reply wait that hits the fallback client timeout triggers a ping on
// the stream. No pong means the connection is dead: the pool closes it
// so the next request re-dials instead of waiting out the idle timeout.
func TestPoolPingOnFallbackTimeoutEvictsDeadConnection(t *testing.T) {
	var dials atomic.Int32
	conns := make(chan *fakePoolConn, 2)
	p := newFakePool(t, 50*time.Millisecond, func(context.Context, string) (poolConn, error) {
		dials.Add(1)
		conn := newFakePoolConn()
		conns <- conn
		go silentServer(t, conn, false)
		return conn, nil
	})

	_, err := p.request(context.Background(), "peer:1", LaneConsume, clusterwire.StreamFrameNodeRequest, []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errFallbackReplyTimeout) {
		t.Fatalf("error = %v, want fallback reply timeout", err)
	}
	first := <-conns
	select {
	case <-first.closedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("dead connection was not closed after the ping went unanswered")
	}
	p.mu.Lock()
	_, pooled := p.conns["peer:1"]
	streams := len(p.streams)
	p.mu.Unlock()
	if pooled || streams != 0 {
		t.Fatalf("pool still holds conn=%v streams=%d after eviction", pooled, streams)
	}

	_, _ = p.request(context.Background(), "peer:1", LaneConsume, clusterwire.StreamFrameNodeRequest, []byte("y"))
	if n := dials.Load(); n != 2 {
		t.Fatalf("dials = %d, want 2 (re-dial after eviction)", n)
	}
}

// A peer that is merely slow answers the ping: the connection must be
// kept, matching the stream client's own no-close-on-timeout contract.
func TestPoolPingOnFallbackTimeoutKeepsLiveConnection(t *testing.T) {
	var dials atomic.Int32
	conn := newFakePoolConn()
	go silentServer(t, conn, true)
	p := newFakePool(t, 50*time.Millisecond, func(context.Context, string) (poolConn, error) {
		dials.Add(1)
		return conn, nil
	})

	for range 2 {
		_, err := p.request(context.Background(), "peer:1", LaneAck, clusterwire.StreamFrameNodeRequest, []byte("x"))
		if !errors.Is(err, errFallbackReplyTimeout) {
			t.Fatalf("error = %v, want fallback reply timeout", err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	if conn.isClosed() {
		t.Fatalf("live connection closed after an answered ping: %v", conn.closeCause)
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("dials = %d, want 1", n)
	}
}

// When a pooled connection dies (its done channel closes), the pool
// evicts it and fails its streams without waiting for a request.
func TestPoolEvictsConnectionWhenItDies(t *testing.T) {
	conn := newFakePoolConn()
	go serveEcho(conn)
	p := newFakePool(t, time.Second, func(context.Context, string) (poolConn, error) { return conn, nil })

	if _, err := p.request(context.Background(), "peer:1", LaneProduce, clusterwire.StreamFrameNodeRequest, []byte("x")); err != nil {
		t.Fatalf("request error = %v", err)
	}
	conn.close(errors.New("peer went away"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		_, pooled := p.conns["peer:1"]
		streams := len(p.streams)
		p.mu.Unlock()
		if !pooled && streams == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("dead connection was not evicted from the pool")
}

func TestLaneNormalizationAndWidths(t *testing.T) {
	if Lane(200).normalize() != LaneControl {
		t.Fatal("out-of-range lane did not normalize to control")
	}
	if LaneProduce.width() != quicProduceLanes || LaneConsume.width() != quicConsumeLanes || LaneAck.width() != quicAckLanes || LaneControl.width() != quicControlLanes {
		t.Fatal("lane widths do not match the per-lane constants")
	}
	if LaneProduce.String() != "produce" || Lane(99).String() != "control" {
		t.Fatal("lane names do not match")
	}
}

func TestQUICAddrNormalization(t *testing.T) {
	for in, want := range map[string]string{
		"10.0.0.1:7942":          "10.0.0.1:7942",
		"http://10.0.0.1:7942":   "10.0.0.1:7942",
		"https://10.0.0.1:7942/": "10.0.0.1:7942",
		" narad-0:7942 ":         "narad-0:7942",
	} {
		if got := quicAddr(in); got != want {
			t.Fatalf("quicAddr(%q) = %q, want %q", in, got, want)
		}
	}
}
