package clusterrpc

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// parkingHandler serves node requests by parking until the request's
// context ends, then replying; it records cancellations it is told about.
type parkingHandler struct {
	mu        sync.Mutex
	started   chan struct{}
	ctxDone   chan error
	cancelled []uint64
}

func (h *parkingHandler) HandleStreamFrame(clusterwire.StreamFrame, func(clusterwire.StreamFrame)) bool {
	return false
}

func (h *parkingHandler) HandleStreamRequest(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	go func() {
		close(h.started)
		<-ctx.Done()
		h.ctxDone <- ctx.Err()
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID})
	}()
	return true
}

func (h *parkingHandler) HandleStreamCancel(_ uint64, requestID uint64) {
	h.mu.Lock()
	h.cancelled = append(h.cancelled, requestID)
	h.mu.Unlock()
}

func (h *parkingHandler) cancelledIDs() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]uint64(nil), h.cancelled...)
}

// A client that stops waiting (its context ends) sends a cancel frame;
// the server cancels the request's context and tells the handler, so a
// handler parked on the client's behalf stops right away instead of
// running out its own budget.
func TestClientCancelPropagatesToServerRequestContext(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
	h := &parkingHandler{started: make(chan struct{}), ctxDone: make(chan error, 1)}
	go serveStreamConn(serverConn, bufio.NewReader(serverConn), nil, slog.New(slog.NewTextHandler(io.Discard, nil)), h)

	client := newStreamClient(clientConn, 30*time.Second)
	go client.readLoop()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, []byte("park"))
		errCh <- err
	}()
	select {
	case <-h.started:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler never started")
	}
	cancel()

	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("requestFrame error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("requestFrame did not return after cancel")
	}
	select {
	case err := <-h.ctxDone:
		if err != context.Canceled {
			t.Fatalf("server request context ended with %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server request context was not cancelled by the client's cancel frame")
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(h.cancelledIDs()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if ids := h.cancelledIDs(); len(ids) != 1 {
		t.Fatalf("cancel notifications = %v, want exactly one", ids)
	}
	if client.isClosed() {
		t.Fatal("cancelling one request must not close the multiplexed stream")
	}
}

// When the stream itself ends, every request still running on it is
// cancelled: nobody can receive their replies any more.
func TestStreamEndCancelsInFlightRequests(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	h := &parkingHandler{started: make(chan struct{}), ctxDone: make(chan error, 1)}
	done := make(chan struct{})
	go func() {
		serveStreamConn(serverConn, bufio.NewReader(serverConn), nil, slog.New(slog.NewTextHandler(io.Discard, nil)), h)
		close(done)
	}()
	client := newStreamClient(clientConn, 30*time.Second)
	go client.readLoop()
	go func() {
		_, _ = client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("park"))
	}()
	<-h.started
	_ = clientConn.Close()
	select {
	case err := <-h.ctxDone:
		if err != context.Canceled {
			t.Fatalf("request context ended with %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream end did not cancel the in-flight request")
	}
	<-done
}

// A server that predates the cancel frame answers it with an error
// frame for a request nobody is waiting on; the client must ignore
// that and keep the stream.
func TestCancelFrameIsHarmlessOnOldServers(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
	// echoHandler only implements the old HandleStreamFrame and answers
	// unknown frame types (via the server's fallback) with an error.
	go serveStreamConn(serverConn, bufio.NewReader(serverConn), nil, slog.New(slog.NewTextHandler(io.Discard, nil)), echoHandler{})
	client := newStreamClient(clientConn, 30*time.Second)
	go client.readLoop()

	client.sendCancel(4242)
	reply, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("still-alive"))
	if err != nil {
		t.Fatalf("request after a cancel to an old server failed: %v", err)
	}
	if string(reply.Payload) != "still-alive" {
		t.Fatalf("echo = %q", reply.Payload)
	}
}
