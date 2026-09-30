package clusterrpc

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2CtxHandler replies on a goroutine after using the request's
// context the way a handler shape does: "none" ignores it (an ack, a
// commit), "done" polls Done (a consume that checks for a gone client),
// "derived" derives a timeout context from it.
type zzWP2CtxHandler struct{ mode string }

func (zzWP2CtxHandler) HandleStreamFrame(clusterwire.StreamFrame, func(clusterwire.StreamFrame)) bool {
	return false
}

func (h zzWP2CtxHandler) HandleStreamRequest(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	go func() {
		switch h.mode {
		case "done":
			select {
			case <-ctx.Done():
			default:
			}
		case "derived":
			_, cancel := context.WithTimeout(ctx, time.Second)
			cancel()
		}
		respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID})
	}()
	return true
}

// Server-side request context cost, per handler shape, over a pipe.
func BenchmarkZZWP2ServerRequestContext(b *testing.B) {
	for _, mode := range []string{"none", "done", "derived"} {
		b.Run(mode, func(b *testing.B) {
			clientConn, serverConn := net.Pipe()
			b.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
			go serveStreamConn(serverConn, serverConn, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), zzWP2CtxHandler{mode: mode})
			client := newStreamClient(clientConn, 30*time.Second)
			go client.readLoop()
			payload := []byte("request")
			b.ReportAllocs()
			for b.Loop() {
				if _, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// zzWP2SyncCtxHandler replies before it returns, after using the
// request's context as zzWP2CtxHandler's mode says.
type zzWP2SyncCtxHandler struct{ mode string }

func (zzWP2SyncCtxHandler) HandleStreamFrame(clusterwire.StreamFrame, func(clusterwire.StreamFrame)) bool {
	return false
}

func (h zzWP2SyncCtxHandler) HandleStreamRequest(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	switch h.mode {
	case "done":
		select {
		case <-ctx.Done():
		default:
		}
	case "derived":
		_, cancel := context.WithTimeout(ctx, time.Second)
		cancel()
	}
	respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID})
	return true
}

// The server's own per-request work, on one goroutine: register the
// request, run a handler that replies at once, retire the request and
// stage the reply (the write itself goes nowhere).
func BenchmarkZZWP2ServerHandleFrame(b *testing.B) {
	for _, mode := range []string{"none", "done", "derived"} {
		b.Run(mode, func(b *testing.B) {
			c := newStreamServerConn(zzWP2DiscardConn{}, nil, nil, nil, zzWP2SyncCtxHandler{mode: mode})
			frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest}
			b.ReportAllocs()
			for b.Loop() {
				frame.RequestID++
				c.handleFrame(frame)
			}
		})
	}
}
