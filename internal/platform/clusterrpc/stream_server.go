package clusterrpc

import (
	"bufio"
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
// respond writes the reply before it returns and keeps no reference to
// its payload, so a handler may reuse the payload's buffer afterwards
// (for example, one it encoded the reply into with node.AppendResponse).
type StreamFrameHandler interface {
	HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool
}

// StreamRequestHandler is the context-aware form of StreamFrameHandler.
// The server derives one context per request frame and cancels it when
// the client sends a StreamFrameCancel for that request, when the stream
// ends, or when the handler responds, so a handler parked on the
// client's behalf (a forwarded long-poll) stops as soon as the client is
// gone. The context carries the serving stream's identity
// (StreamIDFromContext). A handler that implements it is preferred over
// HandleStreamFrame. respond behaves as described on StreamFrameHandler;
// it finds the request by the reply's RequestID, which must be the
// request's (the client correlates the reply by it too).
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
	auth     *connAuth // per-connection proofs; nil disables auth
	logger   *slog.Logger
	handler  StreamFrameHandler
	writeMu  sync.Mutex
	writeBuf []byte

	// streamID identifies this stream in request contexts and cancel
	// notifications (streamIDValue is it boxed once, for Value); inflight
	// maps a request ID to its context while its handler is running.
	streamID      uint64
	streamIDValue any
	inflightMu    sync.Mutex
	inflight      map[uint64]*requestContext

	// respondRequest and respondFrame are the respond callbacks handed to
	// StreamRequestHandler and StreamFrameHandler, made once per stream
	// rather than once per request.
	respondRequest func(clusterwire.StreamFrame)
	respondFrame   func(clusterwire.StreamFrame)
}

// ServeStreamConn serves cluster-RPC frames on a single UNAUTHENTICATED
// stream: it is for transports that carry no cluster secret (tests,
// in-memory pipes). Authenticated streams are served by the QUIC
// listener, which derives the per-connection proofs from the TLS
// session (see serveQUICConn and auth.go).
func ServeStreamConn(conn streamConn, reader io.Reader, logger *slog.Logger, handlers ...StreamFrameHandler) {
	serveStreamConn(conn, reader, nil, logger, firstStreamFrameHandler(handlers))
}

// serveStreamConn serves one stream. auth carries the proofs expected
// and sent on this stream's connection (derived once per connection,
// not once per stream); nil disables auth.
func serveStreamConn(conn streamConn, reader io.Reader, auth *connAuth, logger *slog.Logger, handler StreamFrameHandler) {
	c := newStreamServerConn(conn, reader, auth, logger, handler)
	// When the stream ends every request still running on it is
	// cancelled: the client can no longer receive their replies.
	defer c.cancelInflight()
	c.serve()
}

func newStreamServerConn(conn streamConn, reader io.Reader, auth *connAuth, logger *slog.Logger, handler StreamFrameHandler) *streamServerConn {
	if reader == nil {
		reader = conn
	}
	streamID := nextStreamID.Add(1)
	c := &streamServerConn{
		conn:          conn,
		reader:        reader,
		auth:          auth,
		logger:        logger,
		handler:       handler,
		streamID:      streamID,
		streamIDValue: streamID,
		inflight:      make(map[uint64]*requestContext),
	}
	c.respondRequest = c.finishRequest
	c.respondFrame = c.respond
	return c
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
	// Past the handshake, read through a buffer: a run of small frames
	// costs one stream read instead of two per frame, and the header is
	// parsed in place rather than in a per-frame heap array (see
	// clusterwire.ReadStreamFrame). The handshake reads unbuffered, so a
	// peer that never authenticates is not given a buffer.
	if _, ok := c.reader.(*bufio.Reader); !ok {
		c.reader = bufio.NewReader(c.reader)
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

// authenticate runs the server side of the stream auth handshake when
// a cluster secret is configured. With no secret it is a no-op. The
// stream's first frame must carry the client's proof, read under the
// 64-byte pre-auth cap (an unauthenticated peer cannot make this node
// allocate a 16 MiB buffer per stream by lying in the frame header);
// the server then answers with its own session-bound proof so the
// client can tell a real node from an impostor. A missing or invalid
// proof closes the stream (returns false). In legacy mode (peer on the
// old ALPN) the fixed token is checked and no reply is sent, because
// old clients do not read one.
func (c *streamServerConn) authenticate() bool {
	if c.auth == nil {
		return true
	}
	// Bound the wait for the auth frame, then clear the deadline so the
	// served stream reverts to its normal (deadline-free) read loop.
	_ = c.conn.SetReadDeadline(time.Now().Add(authHandshakeTimeout))
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()

	frame, err := clusterwire.ReadStreamFrame(c.reader, maxAuthFramePayloadBytes)
	if err != nil {
		if c.logger != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			c.logger.Debug("cluster stream auth read", "err", err)
		}
		return false
	}
	if !verifyAuthToken(c.auth.expectedClient, frame) {
		if c.logger != nil {
			c.logger.Warn("cluster stream rejected: invalid auth", "component", "audit", "legacy", c.auth.legacy)
		}
		return false
	}
	if c.auth.legacy {
		return true
	}
	// Prove the secret back. writeFrame aborts the stream on failure, so
	// a client that went away mid-handshake cannot leave it half-open.
	return c.writeFrame(authFrame(c.auth.serverReply))
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
			if h.HandleStreamRequest(c.beginRequest(frame.RequestID), frame, c.respondRequest) {
				return
			}
			c.endRequest(frame.RequestID)
		} else if c.handler != nil && c.handler.HandleStreamFrame(frame, c.respondFrame) {
			return
		}
		c.writeError(frame.RequestID, fmt.Sprintf("unsupported stream frame type %d", frame.Type))
	}
}

// beginRequest creates the request's context and registers it so a
// client cancel, the handler's reply or the stream's end can cancel it.
func (c *streamServerConn) beginRequest(requestID uint64) *requestContext {
	r := &requestContext{stream: c}
	c.inflightMu.Lock()
	prev := c.inflight[requestID]
	c.inflight[requestID] = r
	c.inflightMu.Unlock()
	if prev != nil {
		prev.cancel() // a reused request ID: the earlier one can only be stale
	}
	return r
}

// finishRequest is the respond callback of a StreamRequestHandler: it
// retires the request's context before writing the reply.
func (c *streamServerConn) finishRequest(frame clusterwire.StreamFrame) {
	c.endRequest(frame.RequestID)
	c.writeFrame(frame)
}

// endRequest releases the request's context entry and cancels it.
func (c *streamServerConn) endRequest(requestID uint64) {
	c.inflightMu.Lock()
	r := c.inflight[requestID]
	delete(c.inflight, requestID)
	c.inflightMu.Unlock()
	if r != nil {
		r.cancel()
	}
}

// cancelRequest cancels a running request's context on a client cancel.
// The entry stays until the handler responds (respond releases it), so
// a reply the handler still sends is written and then discarded by the
// client, which is harmless.
func (c *streamServerConn) cancelRequest(requestID uint64) {
	c.inflightMu.Lock()
	r := c.inflight[requestID]
	c.inflightMu.Unlock()
	if r != nil {
		r.cancel()
	}
}

// cancelInflight cancels every request still running on the stream,
// once the stream has ended (serve has returned, so none can begin).
func (c *streamServerConn) cancelInflight() {
	c.inflightMu.Lock()
	requests := make([]*requestContext, 0, len(c.inflight))
	for _, r := range c.inflight {
		requests = append(requests, r)
	}
	c.inflightMu.Unlock()
	for _, r := range requests {
		r.cancel()
	}
}

// requestContext is the context of one request frame (see
// StreamRequestHandler). It costs one small allocation per request:
// until a handler asks for Done, directly or by deriving a context from
// it, cancellation is only a recorded error. Acks and commits never ask;
// a handler that parks (a long-poll consume, a wait for a handler slot)
// does, and gets a context.WithCancel context made on the spot, which
// also keeps derived contexts on the context package's own propagation
// (no goroutine per child).
type requestContext struct {
	stream *streamServerConn

	mu    sync.Mutex
	err   error              // set once, by cancel
	inner context.Context    // made by the first Done; cancelled with err set
	stop  context.CancelFunc // cancels inner
}

func (r *requestContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (r *requestContext) Done() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inner == nil {
		r.inner, r.stop = context.WithCancel(context.Background())
		if r.err != nil {
			r.stop()
		}
	}
	return r.inner.Done()
}

func (r *requestContext) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *requestContext) Value(key any) any {
	if _, ok := key.(streamIDKey); ok {
		return r.stream.streamIDValue
	}
	r.mu.Lock()
	inner := r.inner
	r.mu.Unlock()
	if inner != nil {
		// Only the context package's own key resolves here: it is how a
		// derived context finds inner to register with.
		return inner.Value(key)
	}
	return nil
}

// cancel ends the request with context.Canceled. Err is set and Done
// closed under one lock hold, as the Context contract requires.
func (r *requestContext) cancel() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	r.err = context.Canceled
	if r.stop != nil {
		r.stop()
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

// respond is writeFrame as a handler callback (the result is not the
// handler's concern; a failed write already aborted the stream).
func (c *streamServerConn) respond(frame clusterwire.StreamFrame) {
	c.writeFrame(frame)
}

// writeFrame writes one frame and reports whether it succeeded; a
// failed write aborts the stream. Small frames are staged in writeBuf and
// written once, as the client does; maxRetainedWriteBuffer has the
// measurements behind the copy.
func (c *streamServerConn) writeFrame(frame clusterwire.StreamFrame) bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// Bound reply writes (mirrors the client's write-deadline convention):
	// each request frame spawns a reply goroutine, so a stalled peer must
	// not pile them up on writeMu/flow control until the idle timeout.
	_ = c.conn.SetWriteDeadline(time.Now().Add(replyWriteTimeout(len(frame.Payload))))
	buf, _, err := clusterwire.WriteStreamFrameStaged(c.conn, c.writeBuf, frame, maxRetainedWriteBuffer)
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
		return false
	}
	return true
}
