package clusterrpc

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// StreamFrameHandler handles a decoded cluster-RPC request frame and
// writes its reply via respond. It returns true if it handled the frame.
type StreamFrameHandler interface {
	HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool
}

type streamServerConn struct {
	conn     streamConn
	reader   io.Reader
	expected []byte // expected auth token; nil disables auth
	logger   *slog.Logger
	handler  StreamFrameHandler
	writeMu  sync.Mutex
	writeBuf []byte
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
	c := &streamServerConn{
		conn:     conn,
		reader:   reader,
		expected: expectedToken,
		logger:   logger,
		handler:  handler,
	}
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
	default:
		if c.handler != nil && c.handler.HandleStreamFrame(frame, c.writeFrame) {
			return
		}
		c.writeError(frame.RequestID, fmt.Sprintf("unsupported stream frame type %d", frame.Type))
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
