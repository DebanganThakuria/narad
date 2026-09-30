package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2BlackHole accepts every stream of conn, reads everything and
// answers nothing, counting the pings it swallows.
func zzWP2BlackHole(conn *fakePoolConn, pings *atomic.Int32) {
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
					if frame.Type == clusterwire.StreamFramePing {
						pings.Add(1)
					}
				}
			}()
		case <-conn.closedCh:
			return
		}
	}
}

// cluster-rpc-transport#2: callers that carry a deadline (every hot-path
// caller) must also get a black-holed connection probed and evicted,
// instead of it staying pooled until QUIC's idle timeout.
func TestZZWP2DeadlineTimeoutsProbeDeadConnection(t *testing.T) {
	conn := newFakePoolConn()
	var pings atomic.Int32
	go zzWP2BlackHole(conn, &pings)
	p := newFakePool(t, 200*time.Millisecond, func(context.Context, string) (poolConn, error) { return conn, nil })

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			_, _ = p.request(ctx, "peer:1", LaneAck, clusterwire.StreamFrameNodeRequest, []byte("ack"))
		})
	}
	wg.Wait()
	select {
	case <-conn.closedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("black-holed connection still pooled after deadline-bearing timeouts")
	}
	p.mu.Lock()
	_, pooled := p.conns["peer:1"]
	p.mu.Unlock()
	if pooled {
		t.Fatal("dead connection still in the pool")
	}
	if n := pings.Load(); n > 2 {
		t.Fatalf("pings = %d for one burst of timeouts, want the probe single-flighted", n)
	}
}

// cluster-rpc-transport#2: a request's own budget running out also gets
// a dead connection probed and evicted.
func TestZZWP2BudgetTimeoutProbesDeadConnection(t *testing.T) {
	conn := newFakePoolConn()
	var pings atomic.Int32
	go zzWP2BlackHole(conn, &pings)
	p := newFakePool(t, 5*time.Second, func(context.Context, string) (poolConn, error) { return conn, nil })
	p.pingTimeout = 100 * time.Millisecond

	_, err := p.requestWithin(context.Background(), "peer:1", LaneAck, 50*time.Millisecond, clusterwire.StreamFrameNodeRequest, []byte("ack"))
	zzWP2WantBudgetTimeout(t, err, "request to a black hole")
	select {
	case <-conn.closedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("black-holed connection still pooled after a budget timeout")
	}
}

// cluster-rpc-transport#2: the probe runs off the caller's path; a caller
// whose request timed out returns at its timeout, not after the ping.
func TestZZWP2ProbeDoesNotHoldTheCaller(t *testing.T) {
	conn := newFakePoolConn()
	var pings atomic.Int32
	go zzWP2BlackHole(conn, &pings)
	p := newFakePool(t, 50*time.Millisecond, func(context.Context, string) (poolConn, error) { return conn, nil })
	p.pingTimeout = 700 * time.Millisecond

	start := time.Now()
	_, err := p.request(context.Background(), "peer:1", LaneAck, clusterwire.StreamFrameNodeRequest, []byte("ack"))
	if !errors.Is(err, errFallbackReplyTimeout) {
		t.Fatalf("error = %v, want the fallback reply timeout", err)
	}
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Fatalf("caller returned after %s, held by the liveness ping", waited)
	}
	select {
	case <-conn.closedCh:
	case <-time.After(3 * time.Second):
		t.Fatal("dead connection not evicted by the background probe")
	}
}

// zzWP2SelectiveServer answers "fast" requests at once and never answers
// "slow" ones or pings (a pong stuck behind other replies), counting the
// pings.
func zzWP2SelectiveServer(conn *fakePoolConn, pings *atomic.Int32) {
	for {
		select {
		case server := <-conn.serverEnds:
			go func() {
				reader := bufio.NewReader(server)
				var mu sync.Mutex
				for {
					frame, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
					if err != nil {
						return
					}
					switch {
					case frame.Type == clusterwire.StreamFramePing:
						pings.Add(1)
					case frame.Type == clusterwire.StreamFrameNodeRequest && string(frame.Payload) == "fast":
						mu.Lock()
						_ = clusterwire.WriteStreamFrame(server, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: frame.Payload})
						mu.Unlock()
					}
				}
			}()
		case <-conn.closedCh:
			return
		}
	}
}

// cluster-rpc-transport#2: an unanswered ping alone does not condemn a
// connection that is still delivering replies on other streams. Closing
// it would fail every request in flight on a healthy peer.
func TestZZWP2ProbeSparesBusyConnection(t *testing.T) {
	conn := newFakePoolConn()
	var pings atomic.Int32
	go zzWP2SelectiveServer(conn, &pings)
	p := newFakePool(t, 200*time.Millisecond, func(context.Context, string) (poolConn, error) { return conn, nil })

	stop := make(chan struct{})
	var failed atomic.Int32
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := p.request(context.Background(), "peer:1", LaneAck, clusterwire.StreamFrameNodeRequest, []byte("fast")); err != nil {
				failed.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	})
	time.Sleep(20 * time.Millisecond)

	if _, err := p.request(context.Background(), "peer:1", LaneConsume, clusterwire.StreamFrameNodeRequest, []byte("slow")); !errors.Is(err, errFallbackReplyTimeout) {
		t.Fatalf("slow request error = %v, want the fallback reply timeout", err)
	}
	time.Sleep(600 * time.Millisecond)
	close(stop)
	wg.Wait()

	if pings.Load() == 0 {
		t.Fatal("no probe ran; the test proves nothing")
	}
	if conn.isClosed() {
		t.Fatalf("busy connection closed by the probe: %v", conn.closeCause)
	}
	if n := failed.Load(); n != 0 {
		t.Fatalf("%d requests on the busy connection failed", n)
	}
}
