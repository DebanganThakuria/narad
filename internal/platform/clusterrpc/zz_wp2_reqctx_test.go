package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2DiscardConn is a streamConn whose writes go nowhere.
type zzWP2DiscardConn struct{}

func (zzWP2DiscardConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (zzWP2DiscardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (zzWP2DiscardConn) Close() error                     { return nil }
func (zzWP2DiscardConn) SetDeadline(time.Time) error      { return nil }
func (zzWP2DiscardConn) SetReadDeadline(time.Time) error  { return nil }
func (zzWP2DiscardConn) SetWriteDeadline(time.Time) error { return nil }

// zzWP2SyncHandler replies before it returns, as an ack handler does
// once its goroutine runs.
type zzWP2SyncHandler struct{}

func (zzWP2SyncHandler) HandleStreamFrame(clusterwire.StreamFrame, func(clusterwire.StreamFrame)) bool {
	return false
}

func (zzWP2SyncHandler) HandleStreamRequest(_ context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID})
	return true
}

// cluster-rpc-transport#6: serving a request whose handler never asks
// for its context's Done costs one allocation (the request's context),
// not a cancel context, its cancel func and a respond closure.
func TestZZWP2ServerRequestAllocatesOnlyItsContext(t *testing.T) {
	if zzWP2Race {
		t.Skip("allocation counts differ under the race detector")
	}
	c := newStreamServerConn(zzWP2DiscardConn{}, nil, nil, nil, zzWP2SyncHandler{})
	frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1}
	c.handleFrame(frame) // size the staging buffer
	got := testing.AllocsPerRun(200, func() {
		frame.RequestID++
		c.handleFrame(frame)
	})
	if got > 1 {
		t.Fatalf("allocs per served request = %v, want at most 1", got)
	}
}

// zzWP2FuncHandler runs fn on a goroutine for every node request and
// reports cancel notifications on cancelled.
type zzWP2FuncHandler struct {
	fn        func(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame))
	cancelled chan uint64
}

func (h *zzWP2FuncHandler) HandleStreamFrame(clusterwire.StreamFrame, func(clusterwire.StreamFrame)) bool {
	return false
}

func (h *zzWP2FuncHandler) HandleStreamRequest(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	go h.fn(ctx, frame, respond)
	return true
}

func (h *zzWP2FuncHandler) HandleStreamCancel(_, requestID uint64) {
	if h.cancelled != nil {
		h.cancelled <- requestID
	}
}

// zzWP2ServeFunc serves h on one end of a pipe and returns a stream
// client on the other and a channel closed when serving ends.
func zzWP2ServeFunc(t *testing.T, h *zzWP2FuncHandler) (*streamClient, net.Conn, <-chan struct{}) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
	served := make(chan struct{})
	go func() {
		defer close(served)
		serveStreamConn(serverConn, bufio.NewReader(serverConn), nil, slog.New(slog.NewTextHandler(io.Discard, nil)), h)
	}()
	client := newStreamClient(clientConn, 30*time.Second)
	go client.readLoop()
	return client, clientConn, served
}

// zzWP2RequireCancelled checks ctx the way a handler would see a
// cancellation: Err, Cause and a closed Done.
func zzWP2RequireCancelled(ctx context.Context) error {
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		return errors.New("Err() = " + zzWP2ErrString(err) + ", want context.Canceled")
	}
	if err := context.Cause(ctx); !errors.Is(err, context.Canceled) {
		return errors.New("Cause() = " + zzWP2ErrString(err) + ", want context.Canceled")
	}
	select {
	case <-ctx.Done():
		return nil
	default:
		return errors.New("Done() is open on a cancelled context")
	}
}

func zzWP2ErrString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// A client cancel reaches a handler that has not looked at its context
// yet: Err, Cause and Done all report the cancellation afterwards.
func TestZZWP2CancelReachesUntouchedRequestContext(t *testing.T) {
	inspect := make(chan struct{})
	checked := make(chan error, 1)
	h := &zzWP2FuncHandler{cancelled: make(chan uint64, 1)}
	h.fn = func(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) {
		if ctx.Err() != nil {
			checked <- errors.New("context cancelled on arrival")
			return
		}
		<-inspect
		checked <- zzWP2RequireCancelled(ctx)
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID})
	}
	client, _, _ := zzWP2ServeFunc(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	res := zzWP2Go(func() error {
		_, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, []byte("x"))
		return err
	})
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := zzWP2Result(t, res, "cancelled request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("request error = %v, want context.Canceled", err)
	}
	select {
	case <-h.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("server never saw the cancel")
	}
	close(inspect)
	if err := zzWP2Result(t, checked, "handler"); err != nil {
		t.Fatal(err)
	}
}

// Contexts derived from a request context, and context.AfterFunc on it,
// ride the context package's own propagation (no goroutine per derived
// context), carry the stream's identity, and end with the request.
func TestZZWP2DerivedRequestContextsFollowCancel(t *testing.T) {
	const children = 200
	type outcome struct {
		streamID, childStreamID uint64
		goroutines              int
		childErr                error
		afterRan                bool
	}
	started := make(chan int, 1)
	done := make(chan outcome, 1)
	h := &zzWP2FuncHandler{}
	h.fn = func(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) {
		before := runtime.NumGoroutine()
		var cancels []context.CancelFunc
		var last context.Context
		for range children {
			child, cancel := context.WithTimeout(ctx, time.Minute)
			cancels = append(cancels, cancel)
			last = child
		}
		after := make(chan struct{})
		stop := context.AfterFunc(ctx, func() { close(after) })
		defer stop()
		started <- runtime.NumGoroutine() - before
		<-last.Done()
		o := outcome{
			streamID:      StreamIDFromContext(ctx),
			childStreamID: StreamIDFromContext(last),
			childErr:      last.Err(),
		}
		select {
		case <-after:
			o.afterRan = true
		case <-time.After(5 * time.Second):
		}
		for _, cancel := range cancels {
			cancel()
		}
		done <- o
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID})
	}
	client, _, _ := zzWP2ServeFunc(t, h)

	ctx, cancel := context.WithCancel(context.Background())
	res := zzWP2Go(func() error {
		_, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, []byte("x"))
		return err
	})
	if grew := <-started; grew > children/4 {
		t.Fatalf("deriving %d contexts started %d goroutines", children, grew)
	}
	cancel()
	if err := zzWP2Result(t, res, "cancelled request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("request error = %v, want context.Canceled", err)
	}
	var o outcome
	select {
	case o = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a derived context was not cancelled with its request")
	}
	if !errors.Is(o.childErr, context.Canceled) {
		t.Fatalf("derived context error = %v, want context.Canceled", o.childErr)
	}
	if !o.afterRan {
		t.Fatal("context.AfterFunc did not run when the request was cancelled")
	}
	if o.streamID == 0 || o.childStreamID != o.streamID {
		t.Fatalf("stream IDs: request %d, derived %d", o.streamID, o.childStreamID)
	}
}

// When the stream ends, a request whose handler has not looked at its
// context yet is cancelled too.
func TestZZWP2StreamEndCancelsUntouchedRequestContext(t *testing.T) {
	inspect := make(chan struct{})
	arrived := make(chan struct{})
	checked := make(chan error, 1)
	h := &zzWP2FuncHandler{}
	h.fn = func(ctx context.Context, _ clusterwire.StreamFrame, _ func(clusterwire.StreamFrame)) {
		close(arrived)
		<-inspect
		checked <- zzWP2RequireCancelled(ctx)
	}
	client, clientConn, served := zzWP2ServeFunc(t, h)
	go func() {
		_, _ = client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("x"))
	}()
	<-arrived
	_ = clientConn.Close()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("the server kept serving a closed stream")
	}
	close(inspect)
	if err := zzWP2Result(t, checked, "handler"); err != nil {
		t.Fatal(err)
	}
}

// A handler's reply ends its request context, as it always has, and a
// second request that reuses a live request's ID cancels the stale one.
func TestZZWP2ReplyAndReusedIDEndRequestContext(t *testing.T) {
	type seen struct {
		ctx     context.Context
		respond func(clusterwire.StreamFrame)
		frame   clusterwire.StreamFrame
	}
	requests := make(chan seen, 2)
	h := &zzWP2FuncHandler{fn: func(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) {
		requests <- seen{ctx, respond, frame}
	}}
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
	go serveStreamConn(serverConn, nil, nil, nil, h)
	go func() { _, _ = io.Copy(io.Discard, clientConn) }()
	send := func(id uint64) {
		if err := clusterwire.WriteStreamFrame(clientConn, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: id}); err != nil {
			t.Fatal(err)
		}
	}

	send(1)
	first := <-requests
	send(1)
	second := <-requests
	if err := zzWP2RequireCancelled(first.ctx); err != nil {
		t.Fatalf("stale request with a reused ID: %v", err)
	}
	if err := second.ctx.Err(); err != nil {
		t.Fatalf("live request cancelled: %v", err)
	}
	second.respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: second.frame.RequestID})
	if err := zzWP2RequireCancelled(second.ctx); err != nil {
		t.Fatalf("request after its reply: %v", err)
	}
}
