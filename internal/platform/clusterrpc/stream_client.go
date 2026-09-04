package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

const defaultStreamTimeout = 5 * time.Second

// maxRetainedWriteBuffer caps the frame staging buffer a stream keeps
// between writes. Frames up to this size reuse the buffer; a larger one
// (a segment chunk, say) is staged in a throwaway buffer so an
// occasional bulk transfer does not pin megabytes per pooled stream.
const maxRetainedWriteBuffer = 256 << 10

// errFallbackReplyTimeout marks a reply wait that ended because the
// caller supplied no deadline and the client's own timeout fired. It
// unwraps to context.DeadlineExceeded so callers checking for a timeout
// keep working; the pool checks for it specifically to decide whether
// the connection should be probed.
var errFallbackReplyTimeout = fmt.Errorf("reply wait fell back to client timeout: %w", context.DeadlineExceeded)

// streamErrorCodeAborted is the application-level QUIC stream error code
// this transport uses when it abandons a stream.
const streamErrorCodeAborted quic.StreamErrorCode = 1

// streamConn is the subset of net.Conn a stream client needs; both
// *net.TCPConn and *quic.Stream satisfy it.
type streamConn interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// streamAborter is the QUIC-specific close surface. *quic.Stream's Close
// only closes the send direction: a goroutine blocked in Read stays
// blocked until the peer finishes or the connection dies. Cancelling
// both directions unblocks the reader immediately and tells the peer to
// stop sending.
type streamAborter interface {
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
}

// abortStream closes conn in the way that unblocks both directions:
// CancelRead+CancelWrite on a QUIC stream, Close on anything else (pipes
// and TCP connections unblock readers on Close already).
func abortStream(conn streamConn) {
	if aborter, ok := conn.(streamAborter); ok {
		aborter.CancelRead(streamErrorCodeAborted)
		aborter.CancelWrite(streamErrorCodeAborted)
		return
	}
	_ = conn.Close()
}

// streamClient multiplexes request/reply RPCs over one stream. Requests
// are correlated by RequestID: writers park a channel in pending, the
// single readLoop goroutine delivers the matching reply or error.
type streamClient struct {
	conn    streamConn
	reader  *bufio.Reader
	timeout time.Duration

	writeMu  sync.Mutex
	writeBuf []byte
	nextID   atomic.Uint64
	closed   atomic.Bool
	closeMu  sync.Mutex
	closeErr error

	pendingMu sync.Mutex
	pending   map[uint64]chan streamResult
}

func newStreamClient(conn streamConn, timeout time.Duration) *streamClient {
	return &streamClient{
		conn:    conn,
		reader:  bufio.NewReader(conn),
		timeout: timeout,
		pending: make(map[uint64]chan streamResult),
	}
}

type streamResult struct {
	frame clusterwire.StreamFrame
	err   error
}

// requestFrame sends a request frame and waits for the matching reply
// frame (correlated by RequestID). This is the generic cluster-RPC
// transport primitive used by the peer client.
func (c *streamClient) requestFrame(ctx context.Context, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	requestID := c.nextID.Add(1)
	resultCh := make(chan streamResult, 1)
	c.addPending(requestID, resultCh)
	if err := c.writeFrame(ctx, clusterwire.StreamFrame{
		Type:      frameType,
		RequestID: requestID,
		Payload:   payload,
	}); err != nil {
		c.removePending(requestID)
		return clusterwire.StreamFrame{}, err
	}

	// A caller without a deadline must not block forever on a peer that
	// accepted the frame but never replies: fall back to the configured
	// client timeout for the reply wait. Callers with legitimately longer
	// waits (e.g. long-poll consume forwards) pass an explicit deadline.
	var timeoutCh <-chan time.Time
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && c.timeout > 0 {
		timer := time.NewTimer(c.timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}
	select {
	case result := <-resultCh:
		return result.frame, result.err
	case <-timeoutCh:
		// The stream stays open: a slow handler is not a dead peer, and
		// unrelated in-flight RPCs share it. The pool decides whether to
		// probe the connection (see quicClientPool.probeAfterTimeout).
		c.removePending(requestID)
		c.sendCancel(requestID)
		return clusterwire.StreamFrame{}, fmt.Errorf("cluster rpc reply timed out after %s: %w", c.timeout, errFallbackReplyTimeout)
	case <-ctx.Done():
		// Drop only this request's waiter: the stream is multiplexed and
		// unrelated in-flight RPCs must keep it. The reader discards the
		// late reply for this RequestID (complete finds no pending entry).
		c.removePending(requestID)
		c.sendCancel(requestID)
		return clusterwire.StreamFrame{}, ctx.Err()
	}
}

// cancelFrameWriteTimeout bounds the best-effort cancel notification so
// a caller that already spent its whole budget is not held much longer
// by a stalled stream.
const cancelFrameWriteTimeout = time.Second

// sendCancel tells the server the waiter for requestID is gone (see
// clusterwire.StreamFrameCancel). Best-effort: a write failure closes
// the stream like any other, which cancels every server-side request
// on it anyway.
func (c *streamClient) sendCancel(requestID uint64) {
	if c.isClosed() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), cancelFrameWriteTimeout)
	defer cancel()
	_ = c.writeFrame(ctx, clusterwire.StreamFrame{Type: clusterwire.StreamFrameCancel, RequestID: requestID})
}

func (c *streamClient) addPending(requestID uint64, ch chan streamResult) {
	c.pendingMu.Lock()
	c.pending[requestID] = ch
	c.pendingMu.Unlock()
}

func (c *streamClient) removePending(requestID uint64) {
	c.pendingMu.Lock()
	delete(c.pending, requestID)
	c.pendingMu.Unlock()
}

func (c *streamClient) complete(requestID uint64, result streamResult) {
	c.pendingMu.Lock()
	ch := c.pending[requestID]
	delete(c.pending, requestID)
	c.pendingMu.Unlock()
	if ch == nil {
		return
	}
	ch <- result
}

func (c *streamClient) writeFrame(ctx context.Context, frame clusterwire.StreamFrame) error {
	if c.isClosed() {
		return c.closeError()
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(c.timeout)
	}
	_ = c.conn.SetWriteDeadline(deadline)
	buf, err := clusterwire.WriteStreamFrameInto(c.conn, c.writeBuf, frame)
	_ = c.conn.SetWriteDeadline(time.Time{})
	if cap(buf) <= maxRetainedWriteBuffer {
		c.writeBuf = buf[:0]
	}
	if err != nil {
		c.closeWithError(err)
		return err
	}
	return nil
}

func (c *streamClient) readLoop() {
	for {
		frame, err := clusterwire.ReadStreamFrame(c.reader, clusterwire.MaxStreamFramePayloadBytes)
		if err != nil {
			c.closeWithError(err)
			return
		}
		switch frame.Type {
		case clusterwire.StreamFrameNodeReply:
			c.complete(frame.RequestID, streamResult{frame: frame})
		case clusterwire.StreamFramePong:
			c.complete(frame.RequestID, streamResult{frame: frame})
		case clusterwire.StreamFrameError:
			streamErr, err := clusterwire.DecodeStreamError(frame.Payload)
			if err != nil {
				c.complete(frame.RequestID, streamResult{err: err})
				continue
			}
			c.complete(frame.RequestID, streamResult{err: errors.New(streamErr.Message)})
		default:
			c.closeWithError(fmt.Errorf("unsupported cluster stream frame type %d", frame.Type))
			return
		}
	}
}

func (c *streamClient) isClosed() bool {
	return c.closed.Load()
}

// closeError reports why the stream closed, or a generic error if it
// closed without a recorded cause.
func (c *streamClient) closeError() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closeErr == nil {
		return errors.New("cluster stream closed")
	}
	return c.closeErr
}

func (c *streamClient) closeWithError(err error) {
	if err == nil {
		err = errors.New("cluster stream closed")
	}
	c.closeMu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.closeMu.Unlock()
	if c.closed.Swap(true) {
		return
	}
	abortStream(c.conn)

	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[uint64]chan streamResult)
	c.pendingMu.Unlock()
	for _, ch := range pending {
		ch <- streamResult{err: err}
	}
}
