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
// between writes. Frames up to this size are staged in it and written in
// one Write; for a larger one (a segment chunk, say) only the header and
// the start of the payload are staged, and the rest is written in place
// (see clusterwire.WriteStreamFrameStaged), so an occasional bulk
// transfer neither pins megabytes per pooled stream nor allocates a
// buffer of its size.
const maxRetainedWriteBuffer = 256 << 10

// errFallbackReplyTimeout marks a reply wait that ended because the
// caller supplied no deadline and the client's own timeout fired. It
// unwraps to context.DeadlineExceeded so callers checking for a timeout
// keep working.
var errFallbackReplyTimeout = fmt.Errorf("reply wait fell back to client timeout: %w", context.DeadlineExceeded)

// errRequestTimeout ends a request whose own budget ran out (see
// QUICFrameClient.RequestOnLaneTimeout). Like an expired context deadline
// it unwraps to context.DeadlineExceeded; it is deliberately not
// errFallbackReplyTimeout, which is reserved for callers without a
// deadline.
var errRequestTimeout = fmt.Errorf("cluster rpc request timed out: %w", context.DeadlineExceeded)

// timerPool recycles the timers that bound reply and queue waits, so a
// request carrying a timeout does not allocate one. Reuse is safe: since
// Go 1.23 (this module's language version), Stop and Reset guarantee
// that no value from before the call is received from the channel
// afterwards.
var timerPool sync.Pool

func getTimer(d time.Duration) *time.Timer {
	if t, ok := timerPool.Get().(*time.Timer); ok {
		t.Reset(d)
		return t
	}
	return time.NewTimer(d)
}

func putTimer(t *time.Timer) {
	t.Stop()
	timerPool.Put(t)
}

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
	// reads counts reads from the stream that returned data; the pool
	// compares snapshots to tell a connection still delivering from a
	// dead one (see quicClientPool.runProbe).
	reads    atomic.Uint64
	nextID   atomic.Uint64
	closed   atomic.Bool
	closeMu  sync.Mutex
	closeErr error

	pendingMu sync.Mutex
	pending   map[uint64]chan streamResult
}

func newStreamClient(conn streamConn, timeout time.Duration) *streamClient {
	c := &streamClient{
		conn:     conn,
		timeout:  timeout,
		writeSem: make(chan struct{}, 1),
		pending:  make(map[uint64]chan streamResult),
	}
	c.reader = bufio.NewReader(countingReader{r: conn, reads: &c.reads})
	return c
}

// countingReader counts the reads of r that returned data.
type countingReader struct {
	r     io.Reader
	reads *atomic.Uint64
}

func (r countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.reads.Add(1)
	}
	return n, err
}

type streamResult struct {
	frame clusterwire.StreamFrame
	err   error
}

// requestFrame sends a request frame and waits for the matching reply
// frame (correlated by RequestID). This is the generic cluster-RPC
// transport primitive used by the peer client.
func (c *streamClient) requestFrame(ctx context.Context, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	frame, _, err := c.roundTrip(ctx, time.Time{}, frameType, payload)
	return frame, err
}

// roundTrip is requestFrame with an optional deadline of the caller's
// own (zero for none), enforced alongside ctx: whichever is earlier ends
// the request, the caller's own with errRequestTimeout. timedOut reports
// that the request ended because a deadline passed (ctx's, the caller's
// own or the client's fallback timeout), not on a cancellation or a
// stream failure.
func (c *streamClient) roundTrip(ctx context.Context, deadline time.Time, frameType clusterwire.StreamFrameType, payload []byte) (frame clusterwire.StreamFrame, timedOut bool, err error) {
	// A caller whose context has already ended fails without touching
	// the shared stream: nothing registered, nothing written.
	if err := ctx.Err(); err != nil {
		return clusterwire.StreamFrame{}, errors.Is(err, context.DeadlineExceeded), err
	}
	// limit is when the request must end; own marks it as the caller's
	// deadline rather than ctx's. ctx.Done() already fires at ctx's, the
	// caller's own needs a timer.
	limit, hasLimit := ctx.Deadline()
	own := !deadline.IsZero() && (!hasLimit || deadline.Before(limit))
	if own {
		limit, hasLimit = deadline, true
	}

	requestID := c.nextID.Add(1)
	resultCh := make(chan streamResult, 1)
	c.addPending(requestID, resultCh)
	if err := c.writeFrame(ctx, limit, own, clusterwire.StreamFrame{
		Type:      frameType,
		RequestID: requestID,
		Payload:   payload,
	}); err != nil {
		c.removePending(requestID)
		return clusterwire.StreamFrame{}, errors.Is(err, context.DeadlineExceeded), err
	}

	var (
		timer      *time.Timer
		timeoutErr error
	)
	switch {
	case own:
		timer = getTimer(time.Until(limit))
		timeoutErr = errRequestTimeout
	case !hasLimit && c.timeout > 0:
		// A caller without a deadline must not block forever on a peer
		// that accepted the frame but never replies: fall back to the
		// configured client timeout for the reply wait. Callers with
		// legitimately longer waits (e.g. long-poll consume forwards)
		// pass an explicit deadline.
		timer = getTimer(c.timeout)
	}
	var timeoutCh <-chan time.Time
	if timer != nil {
		defer putTimer(timer)
		timeoutCh = timer.C
	}
	select {
	case result := <-resultCh:
		return result.frame, false, result.err
	case <-timeoutCh:
		// The stream stays open: a slow handler is not a dead peer, and
		// unrelated in-flight RPCs share it. The pool decides whether to
		// probe the connection (see quicClientPool.probe).
		c.removePending(requestID)
		c.sendCancel(requestID)
		if timeoutErr == nil {
			timeoutErr = fmt.Errorf("cluster rpc reply timed out after %s: %w", c.timeout, errFallbackReplyTimeout)
		}
		return clusterwire.StreamFrame{}, true, timeoutErr
	case <-ctx.Done():
		// Drop only this request's waiter: the stream is multiplexed and
		// unrelated in-flight RPCs must keep it. The reader discards the
		// late reply for this RequestID (complete finds no pending entry).
		c.removePending(requestID)
		c.sendCancel(requestID)
		err := ctx.Err()
		return clusterwire.StreamFrame{}, errors.Is(err, context.DeadlineExceeded), err
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
		timer := getTimer(cancelFrameWriteTimeout)
		defer putTimer(timer)
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

// writeFrame writes one frame, bounded by limit (see roundTrip; the
// client timeout when limit is zero). Frames go out one at a time; a
// caller whose context ends while it waits its turn, or whose own
// deadline (own) passes, gives up having written nothing.
func (c *streamClient) writeFrame(ctx context.Context, limit time.Time, own bool, frame clusterwire.StreamFrame) error {
	if c.isClosed() {
		return c.closeError()
	}
	if len(frame.Payload) > clusterwire.MaxStreamFramePayloadBytes {
		// Refused before a byte is written: the stream is untouched.
		return fmt.Errorf("stream frame payload too large: %d bytes", len(frame.Payload))
	}
	var until time.Time
	if own {
		until = limit
	}
	if err := c.lockWrite(ctx, until); err != nil {
		return err
	}
	defer c.unlockWrite()

	writeDeadline := limit
	if writeDeadline.IsZero() {
		writeDeadline = time.Now().Add(c.timeout)
	}
	err := c.writeLocked(writeDeadline, frame)
	if err == nil || c.isClosed() {
		return err
	}
	// writeLocked leaves the stream open only when the write timed out
	// before any of the frame went out; report whose deadline that was.
	switch {
	case own:
		return errRequestTimeout
	case !limit.IsZero():
		return context.DeadlineExceeded
	default:
		return fmt.Errorf("cluster rpc write stalled for %s: %w", c.timeout, context.DeadlineExceeded)
	}
}

// lockWrite takes the write slot, or gives up with ctx's error if ctx
// ends first, or with errRequestTimeout at until when it is set. The
// uncontended case is a single non-blocking send.
func (c *streamClient) lockWrite(ctx context.Context, until time.Time) error {
	select {
	case c.writeSem <- struct{}{}:
		return nil
	default:
	}
	var timeoutCh <-chan time.Time
	if !until.IsZero() {
		timer := getTimer(time.Until(until))
		defer putTimer(timer)
		timeoutCh = timer.C
	}
	select {
	case c.writeSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timeoutCh:
		return errRequestTimeout
	}
}

func (c *streamClient) unlockWrite() {
	<-c.writeSem
}

// writeLocked writes frame under a write deadline. The caller holds the
// write slot. A write that times out before any of the frame went out
// leaves the stream's framing intact, so only this frame fails and the
// stream keeps serving the RPCs multiplexed on it. Any other failure, or
// a frame cut off part-way (which corrupts the framing), closes the
// stream and fails every request on it.
func (c *streamClient) writeLocked(deadline time.Time, frame clusterwire.StreamFrame) error {
	if c.isClosed() {
		return c.closeError()
	}
	_ = c.conn.SetWriteDeadline(deadline)
	buf, n, err := clusterwire.WriteStreamFrameStaged(c.conn, c.writeBuf, frame, maxRetainedWriteBuffer)
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
