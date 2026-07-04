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
	conn    streamConn
	reader  io.Reader
	logger  *slog.Logger
	handler StreamFrameHandler
	writeMu sync.Mutex
}

// ServeStreamConn serves cluster-RPC frames on a single stream.
func ServeStreamConn(conn streamConn, reader io.Reader, logger *slog.Logger, handlers ...StreamFrameHandler) {
	if reader == nil {
		reader = conn
	}
	c := &streamServerConn{
		conn:    conn,
		reader:  reader,
		logger:  logger,
		handler: firstStreamFrameHandler(handlers),
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
	defer c.conn.Close()
	for {
		frame, err := clusterwire.ReadStreamFrame(c.reader, clusterwire.MaxStreamFramePayloadBytes)
		if err != nil {
			if c.logger != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				c.logger.Debug("cluster stream read", "err", err)
			}
			return
		}
		c.handleFrame(frame)
	}
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

func (c *streamServerConn) writeFrame(frame clusterwire.StreamFrame) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// Bound reply writes (mirrors the client's write-deadline convention):
	// each request frame spawns a reply goroutine, so a stalled peer must
	// not pile them up on writeMu/flow control until the idle timeout.
	_ = c.conn.SetWriteDeadline(time.Now().Add(defaultStreamTimeout))
	err := clusterwire.WriteStreamFrame(c.conn, frame)
	_ = c.conn.SetWriteDeadline(time.Time{})
	if err != nil {
		if c.logger != nil {
			c.logger.Debug("cluster stream write", "err", err)
		}
		// A failed (possibly partial) write corrupts the framing; the
		// stream is unrecoverable. Closing also unblocks the serve loop.
		_ = c.conn.Close()
	}
}
