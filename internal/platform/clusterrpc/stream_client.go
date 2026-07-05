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

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

const defaultStreamTimeout = 5 * time.Second

// streamConn is the subset of net.Conn a stream client needs; both
// *net.TCPConn and *quic.Stream satisfy it.
type streamConn interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

// streamClient multiplexes request/reply RPCs over one stream. Requests
// are correlated by RequestID: writers park a channel in pending, the
// single readLoop goroutine delivers the matching reply or error.
type streamClient struct {
	conn    streamConn
	reader  *bufio.Reader
	timeout time.Duration

	writeMu  sync.Mutex
	nextID   atomic.Uint64
	closed   atomic.Bool
	closeMu  sync.Mutex
	closeErr error

	pendingMu sync.Mutex
	pending   map[uint64]chan streamResult
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
		c.removePending(requestID)
		return clusterwire.StreamFrame{}, fmt.Errorf("cluster rpc reply timed out after %s: %w", c.timeout, context.DeadlineExceeded)
	case <-ctx.Done():
		// Drop only this request's waiter: the stream is multiplexed and
		// unrelated in-flight RPCs must keep it. The reader discards the
		// late reply for this RequestID (complete finds no pending entry).
		c.removePending(requestID)
		return clusterwire.StreamFrame{}, ctx.Err()
	}
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
	err := clusterwire.WriteStreamFrame(c.conn, frame)
	_ = c.conn.SetWriteDeadline(time.Time{})
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
	_ = c.conn.Close()

	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[uint64]chan streamResult)
	c.pendingMu.Unlock()
	for _, ch := range pending {
		ch <- streamResult{err: err}
	}
}
