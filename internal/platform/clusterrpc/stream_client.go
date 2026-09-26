package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

	// writeSem serializes frame writes: a one-slot semaphore rather than a
	// mutex, so a caller queued behind a stalled write can give up when
	// its context ends, having written nothing. writeBuf is the frame
	// staging buffer; the slot holder owns it.
	writeSem chan struct{}
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
		conn:     conn,
		reader:   bufio.NewReader(conn),
		timeout:  timeout,
		writeSem: make(chan struct{}, 1),
		pending:  make(map[uint64]chan streamResult),
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
	// A caller whose context has already ended fails without touching
	// the shared stream: nothing registered, nothing written.
	if err := ctx.Err(); err != nil {
		return clusterwire.StreamFrame{}, err
	}
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

// cancelFrameWriteTimeout bounds the best-effort cancel notification:
// the wait for the write slot and the write each.
const cancelFrameWriteTimeout = time.Second

// sendCancel tells the server the waiter for requestID is gone (see
// clusterwire.StreamFrameCancel). Best-effort and off the caller's path:
// when another frame is being written the cancel is queued on its own
// goroutine rather than holding a caller whose budget is already spent,
// and it is dropped if the slot does not free up in time (the server's
// reservation then just runs out its lease).
func (c *streamClient) sendCancel(requestID uint64) {
	if c.isClosed() {
		return
	}
	frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameCancel, RequestID: requestID}
	select {
	case c.writeSem <- struct{}{}:
		c.writeCancel(frame)
		return
	default:
	}
	go func() {
		timer := time.NewTimer(cancelFrameWriteTimeout)
		defer timer.Stop()
		select {
		case c.writeSem <- struct{}{}:
			c.writeCancel(frame)
		case <-timer.C:
		}
	}()
}

// writeCancel writes a cancel frame. The caller holds the write slot.
func (c *streamClient) writeCancel(frame clusterwire.StreamFrame) {
	defer c.unlockWrite()
	_ = c.writeLocked(time.Now().Add(cancelFrameWriteTimeout), frame)
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

// writeFrame writes one frame, bounded by ctx's deadline (the client
// timeout when it has none). Frames go out one at a time; a caller whose
// context ends while it waits its turn gives up with the context's
// error, having written nothing.
func (c *streamClient) writeFrame(ctx context.Context, frame clusterwire.StreamFrame) error {
	if c.isClosed() {
		return c.closeError()
	}
	if len(frame.Payload) > clusterwire.MaxStreamFramePayloadBytes {
		// Refused before a byte is written: the stream is untouched.
		return fmt.Errorf("stream frame payload too large: %d bytes", len(frame.Payload))
	}
	if err := c.lockWrite(ctx); err != nil {
		return err
	}
	defer c.unlockWrite()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(c.timeout)
	}
	err := c.writeLocked(deadline, frame)
	if ok && err != nil && !c.isClosed() {
		// The caller's own deadline cut the write off before any of the
		// frame went out: report it as the context's.
		return context.DeadlineExceeded
	}
	return err
}

// lockWrite takes the write slot, or returns ctx's error if ctx ends
// first. The uncontended case is a single non-blocking send.
func (c *streamClient) lockWrite(ctx context.Context) error {
	select {
	case c.writeSem <- struct{}{}:
		return nil
	default:
	}
	select {
	case c.writeSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *streamClient) unlockWrite() {
	<-c.writeSem
}

// writeLocked writes frame in one Write call under a write deadline. The
// caller holds the write slot. A write that times out before any of the
// frame went out leaves the stream's framing intact, so only this frame
// fails and the stream keeps serving the RPCs multiplexed on it. Any
// other failure, or a frame cut off part-way (which corrupts the
// framing), closes the stream and fails every request on it.
func (c *streamClient) writeLocked(deadline time.Time, frame clusterwire.StreamFrame) error {
	if c.isClosed() {
		return c.closeError()
	}
	buf := clusterwire.AppendStreamFrame(c.writeBuf[:0], frame)
	_ = c.conn.SetWriteDeadline(deadline)
	n, err := c.conn.Write(buf)
	_ = c.conn.SetWriteDeadline(time.Time{})
	if cap(buf) <= maxRetainedWriteBuffer {
		c.writeBuf = buf[:0]
	}
	if err == nil {
		return nil
	}
	if n == 0 && errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}
	c.closeWithError(err)
	return err
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
