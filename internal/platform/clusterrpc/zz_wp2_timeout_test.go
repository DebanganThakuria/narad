package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2WantBudgetTimeout checks err is how an exhausted per-call budget
// must look: a deadline error, but never the fallback reply timeout
// (which is reserved for callers without a deadline).
func zzWP2WantBudgetTimeout(t *testing.T, err error, what string) {
	t.Helper()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%s error = %v, want one wrapping context.DeadlineExceeded", what, err)
	}
	if errors.Is(err, errFallbackReplyTimeout) {
		t.Fatalf("%s error = %v, must not be the fallback reply timeout", what, err)
	}
}

// cross-cutting#7: the per-call budget bounds the wait for a stream open
// too, not only the reply.
func TestZZWP2RequestTimeoutBoundsStreamOpen(t *testing.T) {
	base := newFakePoolConn()
	go serveEcho(base)
	conn := &zzWP2GateConn{fakePoolConn: base, release: make(chan struct{})}
	defer close(conn.release)
	p := newFakePool(t, 5*time.Second, func(context.Context, string) (poolConn, error) { return conn, nil })

	start := time.Now()
	_, err := p.requestWithin(context.Background(), "peer:1", LaneAck, 50*time.Millisecond, clusterwire.StreamFrameNodeRequest, []byte("x"))
	zzWP2WantBudgetTimeout(t, err, "request stuck behind a stream open")
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("request waited %s, want about its 50ms budget", waited)
	}
	if conn.isClosed() {
		t.Fatal("a caller's exhausted budget closed the connection")
	}
}

// cross-cutting#7: the per-call budget bounds the wait for a dial.
func TestZZWP2RequestTimeoutBoundsDial(t *testing.T) {
	ended := make(chan time.Time, 1)
	p := newFakePool(t, 5*time.Second, zzWP2BlockingDial(ended))
	start := time.Now()
	_, err := p.requestWithin(context.Background(), "peer:1", LaneControl, 50*time.Millisecond, clusterwire.StreamFrameNodeRequest, []byte("x"))
	zzWP2WantBudgetTimeout(t, err, "request stuck behind a dial")
	if waited := time.Since(start); waited > 500*time.Millisecond {
		t.Fatalf("request waited %s, want about its 50ms budget", waited)
	}
}

// cross-cutting#7: the budget bounds the reply wait, well inside the
// client's fallback timeout; the timed-out request is cancelled on the
// server and the shared stream stays up.
func TestZZWP2RequestTimeoutBoundsReplyWait(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	frames := serveFrames(server)

	res := zzWP2Go(func() error {
		_, timedOut, err := client.roundTrip(context.Background(), time.Now().Add(50*time.Millisecond), clusterwire.StreamFrameNodeRequest, []byte("x"))
		if err != nil && !timedOut {
			return errors.New("timeout not reported as timedOut: " + err.Error())
		}
		return err
	})
	req := <-frames
	start := time.Now()
	err := zzWP2Result(t, res, "request")
	zzWP2WantBudgetTimeout(t, err, "request with no reply")
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("request waited %s, want about its 50ms budget", waited)
	}
	select {
	case cancel := <-frames:
		if cancel.Type != clusterwire.StreamFrameCancel || cancel.RequestID != req.RequestID {
			t.Fatalf("frame after timeout = %+v, want a cancel for request %d", cancel, req.RequestID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never told the request was abandoned")
	}
	if client.isClosed() {
		t.Fatal("budget timeout closed the shared stream")
	}
	if n := pendingCount(client); n != 0 {
		t.Fatalf("pending entries = %d, want 0", n)
	}
}

// cross-cutting#7: a budget longer than the client's fallback timeout is
// honoured, like an explicit context deadline.
func TestZZWP2RequestTimeoutOutlivesClientTimeout(t *testing.T) {
	client, server := newTestStreamClient(t, 20*time.Millisecond)
	frames := serveFrames(server)
	res := zzWP2Go(func() error {
		frame, _, err := client.roundTrip(context.Background(), time.Now().Add(5*time.Second), clusterwire.StreamFrameNodeRequest, []byte("x"))
		if err == nil && string(frame.Payload) != "slow-reply" {
			err = errors.New("unexpected reply")
		}
		return err
	})
	req := <-frames
	time.Sleep(150 * time.Millisecond)
	writeReply(t, server, req.RequestID, []byte("slow-reply"))
	if err := zzWP2Result(t, res, "slow request"); err != nil {
		t.Fatalf("slow request within budget error = %v", err)
	}
}

// cross-cutting#7: whichever of ctx's deadline and the budget comes first
// ends the call, and a cancelled ctx still reads as a cancellation.
func TestZZWP2RequestTimeoutAndContextCompose(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	frames := serveFrames(server)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	res := zzWP2Go(func() error {
		_, _, err := client.roundTrip(ctx, time.Now().Add(5*time.Second), clusterwire.StreamFrameNodeRequest, []byte("x"))
		return err
	})
	<-frames
	if err := zzWP2Result(t, res, "request"); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errRequestTimeout) {
		t.Fatalf("earlier ctx deadline: error = %v, want ctx's own", err)
	}
	<-frames // its cancel

	cctx, ccancel := context.WithCancel(context.Background())
	res = zzWP2Go(func() error {
		_, timedOut, err := client.roundTrip(cctx, time.Now().Add(5*time.Second), clusterwire.StreamFrameNodeRequest, []byte("y"))
		if timedOut {
			return errors.New("cancellation reported as a timeout")
		}
		return err
	})
	req := <-frames
	ccancel()
	if err := zzWP2Result(t, res, "cancelled request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error = %v, want context.Canceled", err)
	}
	if cancelFrame := <-frames; cancelFrame.Type != clusterwire.StreamFrameCancel || cancelFrame.RequestID != req.RequestID {
		t.Fatalf("frame after cancel = %+v, want a cancel for request %d", cancelFrame, req.RequestID)
	}
}

// cross-cutting#7: a budget that runs out while the caller is queued for
// the write slot ends the call there, with nothing written and the
// stream intact.
func TestZZWP2RequestTimeoutEndsQueueWait(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	big := zzWP2Go(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, make([]byte, 64<<10))
		return err
	})
	time.Sleep(50 * time.Millisecond) // the big write holds the slot; the server is not reading

	start := time.Now()
	_, timedOut, err := client.roundTrip(context.Background(), time.Now().Add(50*time.Millisecond), clusterwire.StreamFrameNodeRequest, []byte("x"))
	zzWP2WantBudgetTimeout(t, err, "queued request")
	if !timedOut {
		t.Fatal("queue timeout not reported as timedOut")
	}
	if waited := time.Since(start); waited > time.Second {
		t.Fatalf("queued request waited %s, want about its 50ms budget", waited)
	}
	if client.isClosed() {
		t.Fatal("a queued caller's timeout closed the stream")
	}
	reader := bufio.NewReader(server)
	frame, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
	if err != nil || len(frame.Payload) != 64<<10 {
		t.Fatalf("large frame = %d bytes, %v", len(frame.Payload), err)
	}
	writeReply(t, server, frame.RequestID, nil)
	if err := zzWP2Result(t, big, "large request"); err != nil {
		t.Fatalf("large request error = %v", err)
	}
}

// Pooled timers must never deliver a stale expiry: after a request whose
// budget ran out, the next request with a fresh budget on the same stream
// completes normally, over and over.
func TestZZWP2PooledTimersDoNotLeakExpiries(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
	go ServeStreamConn(serverConn, serverConn, nil, zzWP2QuietHandler{})
	client := newStreamClient(clientConn, 5*time.Second)
	go client.readLoop()

	for i := range 200 {
		_, _, err := client.roundTrip(context.Background(), time.Now().Add(time.Millisecond), clusterwire.StreamFrameNodeRequest, []byte("never"))
		if !errors.Is(err, errRequestTimeout) {
			t.Fatalf("round %d: unanswered request error = %v, want errRequestTimeout", i, err)
		}
		frame, _, err := client.roundTrip(context.Background(), time.Now().Add(5*time.Second), clusterwire.StreamFrameNodeRequest, []byte("echo"))
		if err != nil || string(frame.Payload) != "echo" {
			t.Fatalf("round %d: answered request = %q, %v", i, frame.Payload, err)
		}
	}
}

// zzWP2QuietHandler echoes every request except "never", which it drops.
type zzWP2QuietHandler struct{}

func (zzWP2QuietHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	if string(frame.Payload) != "never" {
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: frame.Payload})
	}
	return true
}

// cross-cutting#7 over real QUIC: the public API returns the reply within
// budget, a budget-bounded timeout otherwise, and a nil client fails
// cleanly.
func TestZZWP2RequestOnLaneTimeoutOverQUIC(t *testing.T) {
	h := newZZWP2ParkHandler()
	addr := zzWP2Server(t, h)
	client := NewQUICFrameClient(5*time.Second, "sekret")
	t.Cleanup(func() { close(h.release); _ = client.Close() })

	reply, err := client.RequestOnLaneTimeout(context.Background(), addr, LaneAck, 2*time.Second, clusterwire.StreamFrameNodeRequest, []byte("ping"))
	if err != nil || string(reply.Payload) != "ping" {
		t.Fatalf("reply = %q, %v", reply.Payload, err)
	}
	start := time.Now()
	_, err = client.RequestOnLaneTimeout(context.Background(), addr, LaneConsume, 100*time.Millisecond, clusterwire.StreamFrameNodeRequest, []byte("park"))
	zzWP2WantBudgetTimeout(t, err, "parked request")
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("parked request waited %s, want about its 100ms budget", waited)
	}
	var nilClient *QUICFrameClient
	if _, err := nilClient.RequestOnLaneTimeout(context.Background(), addr, LaneAck, time.Second, clusterwire.StreamFrameNodeRequest, nil); err == nil {
		t.Fatal("nil client request succeeded")
	}
}
