package clusterrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// StreamFrameHandler handles a decoded cluster-RPC request frame and
// writes its reply via respond. It returns true if it handled the frame.
type StreamFrameHandler interface {
	HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool
}

// StreamRequestHandler is the context-aware form of StreamFrameHandler.
// The server derives one context per request frame and cancels it when
// the client sends a StreamFrameCancel for that request or when the
// stream ends, so a handler parked on the client's behalf (a forwarded
// long-poll) stops as soon as the client is gone. The context carries
// the serving stream's identity (StreamIDFromContext). A handler that
// implements it is preferred over HandleStreamFrame.
type StreamRequestHandler interface {
	HandleStreamRequest(ctx context.Context, frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool
}

// StreamCancelHandler is told about every StreamFrameCancel after the
// request's context was cancelled, so a handler can undo side effects of
// a reply the client will never read (a consume that already reserved a
// message, say). requestID is scoped to streamID.
type StreamCancelHandler interface {
	HandleStreamCancel(streamID, requestID uint64)
}

type streamIDKey struct{}

// StreamIDFromContext returns the identity of the stream a request
// arrived on (unique per process), or 0 for a context not derived by
// the stream server.
func StreamIDFromContext(ctx context.Context) uint64 {
	id, _ := ctx.Value(streamIDKey{}).(uint64)
	return id
}

var nextStreamID atomic.Uint64

type streamServerConn struct {
	conn     streamConn
	reader   io.Reader
	expected []byte // expected auth token; nil disables auth
	logger   *slog.Logger
	handler  StreamFrameHandler
	writeMu  sync.Mutex
	writeBuf []byte

	// streamID identifies this stream in request contexts and cancel
	// notifications; inflight maps a request ID to its context's cancel
	// func while its handler is running.
	streamID   uint64
	inflightMu sync.Mutex
	inflight   map[uint64]context.CancelFunc
	baseCtx    context.Context
	cancelAll  context.CancelFunc
}

// ServeStreamConn serves cluster-RPC frames on a single stream. When
// secret is non-empty, the stream's first frame must be a valid auth
// proof or the stream is closed without serving any request.
func ServeStreamConn(conn streamConn, reader io.Reader, secret string, logger *slog.Logger, handlers ...StreamFrameHandler) {
	serveStreamConn(conn, reader, expectedAuthToken(secret), logger, firstStreamFrameHandler(handlers))
}

// serveStreamConn is ServeStreamConn with the auth token already derived
// (once per listener, not once per stream).
func serveStreamConn(conn streamConn, reader io.Reader, expectedToken []byte, logger *slog.Logger, handler StreamFrameHandler) {
	if reader == nil {
		reader = conn
	}
	baseCtx, cancelAll := context.WithCancel(context.WithValue(context.Background(), streamIDKey{}, nextStreamID.Add(1)))
	c := &streamServerConn{
		conn:      conn,
		reader:    reader,
		expected:  expectedToken,
		logger:    logger,
		handler:   handler,
		streamID:  StreamIDFromContext(baseCtx),
		inflight:  make(map[uint64]context.CancelFunc),
		baseCtx:   baseCtx,
		cancelAll: cancelAll,
	}
	// When the stream ends every request still running on it is
	// cancelled: the client can no longer receive their replies.
	defer cancelAll()
	c.serve()
}

func firstStreamFrameHandler(handlers []StreamFrameHandler) StreamFrameHandler {
	for _, handler := range handlers {
		if handler != nil {
			return handler
		}
	}
	return nil
}

func (c *streamServerConn) serve() {
	if !c.authenticate() {
		// Rejected: reset both directions so the peer's reader sees the
		// failure now rather than at connection teardown.
		abortStream(c.conn)
		return
	}
	for {
		frame, err := clusterwire.ReadStreamFrame(c.reader, clusterwire.MaxStreamFramePayloadBytes)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// The peer finished cleanly; close our send side the same way.
				_ = c.conn.Close()
				return
			}
			if c.logger != nil && !errors.Is(err, net.ErrClosed) {
				c.logger.Debug("cluster stream read", "err", err)
			}
			abortStream(c.conn)
			return
		}
		c.handleFrame(frame)
	}
}

// authHandshakeTimeout bounds how long the server waits for a client's
// first (auth) frame, so an authenticated-transport peer that opens a
// stream and then goes silent cannot pin a serving goroutine and its
// receive buffer indefinitely.
const authHandshakeTimeout = 5 * time.Second

// authenticate consumes and verifies the stream's first frame when a
// cluster secret is configured. With no secret it is a no-op. A missing
// or invalid proof closes the stream (returns false).
func (c *streamServerConn) authenticate() bool {
	if c.expected == nil {
		return true
	}
	// Bound the wait for the auth frame, then clear the deadline so the
	// served stream reverts to its normal (deadline-free) read loop.
	_ = c.conn.SetReadDeadline(time.Now().Add(authHandshakeTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()

	frame, err := clusterwire.ReadStreamFrame(c.reader, clusterwire.MaxStreamFramePayloadBytes)
	if err != nil {
		if c.logger != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			c.logger.Debug("cluster stream auth read", "err", err)
		}
		return false
	}
	if !verifyAuthToken(c.expected, frame) {
		if c.logger != nil {
			c.logger.Warn("cluster stream rejected: invalid auth", "component", "audit")
		}
		return false
	}
	return true
}

func (c *streamServerConn) handleFrame(frame clusterwire.StreamFrame) {
	switch frame.Type {
	case clusterwire.StreamFramePing:
		c.writeFrame(clusterwire.StreamFrame{
			Type:      clusterwire.StreamFramePong,
			RequestID: frame.RequestID,
		})
	case clusterwire.StreamFrameCancel:
		c.cancelRequest(frame.RequestID)
		if h, ok := c.handler.(StreamCancelHandler); ok {
			h.HandleStreamCancel(c.streamID, frame.RequestID)
		}
	default:
		if h, ok := c.handler.(StreamRequestHandler); ok {
			ctx, respond := c.beginRequest(frame.RequestID)
			if h.HandleStreamRequest(ctx, frame, respond) {
				return
			}
			c.endRequest(frame.RequestID)
		} else if c.handler != nil && c.handler.HandleStreamFrame(frame, c.writeFrame) {
			return
		}
		c.writeError(frame.RequestID, fmt.Sprintf("unsupported stream frame type %d", frame.Type))
	}
}

// beginRequest derives the request's context and returns it with a
// respond func that retires the context entry before writing the reply.
func (c *streamServerConn) beginRequest(requestID uint64) (context.Context, func(clusterwire.StreamFrame)) {
	ctx, cancel := context.WithCancel(c.baseCtx)
	c.inflightMu.Lock()
	if prev := c.inflight[requestID]; prev != nil {
		prev() // a reused request ID: the earlier one can only be stale
	}
	c.inflight[requestID] = cancel
	c.inflightMu.Unlock()
	return ctx, func(frame clusterwire.StreamFrame) {
		c.endRequest(requestID)
		c.writeFrame(frame)
	}
}

// endRequest releases the request's context entry (and its resources).
func (c *streamServerConn) endRequest(requestID uint64) {
	c.inflightMu.Lock()
	cancel := c.inflight[requestID]
	delete(c.inflight, requestID)
	c.inflightMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// cancelRequest cancels a running request's context on a client cancel.
// The entry stays until the handler responds (respond releases it), so
// a reply the handler still sends is written and then discarded by the
// client, which is harmless.
func (c *streamServerConn) cancelRequest(requestID uint64) {
	c.inflightMu.Lock()
	cancel := c.inflight[requestID]
	c.inflightMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *streamServerConn) writeError(requestID uint64, message string) {
	payload, err := clusterwire.EncodeStreamError(message)
	if err != nil {
		return
	}
	c.writeFrame(clusterwire.StreamFrame{
		Type:      clusterwire.StreamFrameError,
		RequestID: requestID,
		Payload:   payload,
	})
}

// replyWriteTimeout bounds one reply write: the base client timeout plus
// one second per 256 KiB of payload, so a multi-megabyte segment chunk
// or commit-batch reply is not cut off by a deadline sized for small
// control replies, while a stalled peer still cannot pin a reply
// goroutine indefinitely.
func replyWriteTimeout(payloadBytes int) time.Duration {
	return defaultStreamTimeout + time.Duration(payloadBytes/(256<<10))*time.Second
}

func (c *streamServerConn) writeFrame(frame clusterwire.StreamFrame) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// Bound reply writes (mirrors the client's write-deadline convention):
	// each request frame spawns a reply goroutine, so a stalled peer must
	// not pile them up on writeMu/flow control until the idle timeout.
	_ = c.conn.SetWriteDeadline(time.Now().Add(replyWriteTimeout(len(frame.Payload))))
	buf, err := clusterwire.WriteStreamFrameInto(c.conn, c.writeBuf, frame)
	_ = c.conn.SetWriteDeadline(time.Time{})
	if cap(buf) <= maxRetainedWriteBuffer {
		c.writeBuf = buf[:0]
	}
	if err != nil {
		if c.logger != nil {
			c.logger.Debug("cluster stream write", "err", err)
		}
		// A failed (possibly partial) write corrupts the framing; the
		// stream is unrecoverable. Aborting both directions also unblocks
		// the serve loop's Read on a QUIC stream.
		abortStream(c.conn)
	}
}
