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

// newTestStreamClient wires a streamClient to one end of an in-memory pipe
// and returns the server end for the test to drive.
func newTestStreamClient(t *testing.T, timeout time.Duration) (*streamClient, net.Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	client := &streamClient{
		conn:    clientConn,
		reader:  bufio.NewReader(clientConn),
		timeout: timeout,
		pending: make(map[uint64]chan streamResult),
	}
	go client.readLoop()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	return client, serverConn
}

// serveFrames reads request frames off the server end and forwards them to
// the returned channel until the pipe closes.
func serveFrames(server net.Conn) <-chan clusterwire.StreamFrame {
	frames := make(chan clusterwire.StreamFrame, 16)
	go func() {
		reader := bufio.NewReader(server)
		for {
			frame, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
			if err != nil {
				close(frames)
				return
			}
			frames <- frame
		}
	}()
	return frames
}

func writeReply(t *testing.T, server net.Conn, requestID uint64, payload []byte) {
	t.Helper()
	if err := clusterwire.WriteStreamFrame(server, clusterwire.StreamFrame{
		Type:      clusterwire.StreamFrameNodeReply,
		RequestID: requestID,
		Payload:   payload,
	}); err != nil {
		t.Fatalf("WriteStreamFrame() error = %v", err)
	}
}

func pendingCount(c *streamClient) int {
	c.pendingMu.Lock()
	defer c.pendingMu.Unlock()
	return len(c.pending)
}

// Cancelling one request's context must fail only that request. The stream
// is multiplexed: unrelated in-flight RPCs on the same stream must complete,
// and the late reply for the cancelled request must be dropped harmlessly.
func TestRequestFrameContextCancelKeepsStreamAlive(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	frames := serveFrames(server)

	// Request A: the server never replies before the context is cancelled.
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	errA := make(chan error, 1)
	go func() {
		_, err := client.requestFrame(ctxA, clusterwire.StreamFrameNodeRequest, []byte("a"))
		errA <- err
	}()
	frameA := <-frames

	// Request B: in flight on the same stream while A is cancelled.
	resB := make(chan streamResult, 1)
	go func() {
		frame, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("b"))
		resB <- streamResult{frame: frame, err: err}
	}()
	frameB := <-frames

	cancelA()
	if err := <-errA; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error = %v, want context.Canceled", err)
	}
	if client.isClosed() {
		t.Fatal("stream closed after one request's context was cancelled")
	}

	// The late reply for A hits no waiter and must be discarded without
	// disturbing B's reply, which follows on the same stream.
	writeReply(t, server, frameA.RequestID, []byte("late-a"))
	writeReply(t, server, frameB.RequestID, []byte("reply-b"))

	result := <-resB
	if result.err != nil {
		t.Fatalf("concurrent request error = %v", result.err)
	}
	if string(result.frame.Payload) != "reply-b" {
		t.Fatalf("reply payload = %q, want %q", result.frame.Payload, "reply-b")
	}
	if n := pendingCount(client); n != 0 {
		t.Fatalf("pending entries = %d, want 0 (cancelled waiter leaked)", n)
	}
}

// A caller without a deadline must not block forever on a peer that accepted
// the frame but never replies: the reply wait falls back to the configured
// client timeout, without tearing down the shared stream.
func TestRequestFrameNoDeadlineFallsBackToClientTimeout(t *testing.T) {
	client, server := newTestStreamClient(t, 50*time.Millisecond)
	frames := serveFrames(server)

	done := make(chan error, 1)
	go func() {
		_, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("x"))
		done <- err
	}()
	<-frames // the server reads the request and never replies

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request with no deadline blocked past the fallback timeout")
	}
	if client.isClosed() {
		t.Fatal("fallback timeout must not close the shared stream")
	}
	if n := pendingCount(client); n != 0 {
		t.Fatalf("pending entries = %d, want 0 (timed-out waiter leaked)", n)
	}
}

// An explicit caller deadline overrides the fallback: long waits (e.g.
// long-poll consume forwards) must not be cut short at the client timeout.
func TestRequestFrameExplicitDeadlineOutlivesClientTimeout(t *testing.T) {
	client, server := newTestStreamClient(t, 20*time.Millisecond)
	frames := serveFrames(server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res := make(chan streamResult, 1)
	go func() {
		frame, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, []byte("x"))
		res <- streamResult{frame: frame, err: err}
	}()
	frame := <-frames

	// Reply well after the client timeout but within the caller deadline.
	time.Sleep(150 * time.Millisecond)
	writeReply(t, server, frame.RequestID, []byte("slow-reply"))

	result := <-res
	if result.err != nil {
		t.Fatalf("request with explicit deadline error = %v", result.err)
	}
	if string(result.frame.Payload) != "slow-reply" {
		t.Fatalf("reply payload = %q, want %q", result.frame.Payload, "slow-reply")
	}
}
