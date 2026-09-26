package clusterrpc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// The tests in this file pin the one case where a failed frame write
// must NOT close the shared stream: Write timed out before any byte of
// the frame went out, so the framing is intact (streamClient.writeLocked).
// Each checks that the timeout really came out of Write itself, since a
// request that gives up earlier (before or while queueing for the write
// slot) never reaches that branch.

// zzWP2FrameHeaderBytes is the size of a stream frame header.
const zzWP2FrameHeaderBytes = 20

// zzWP2WriteLog wraps a stream and records every Write's outcome.
type zzWP2WriteLog struct {
	streamConn
	mu      sync.Mutex
	written int
	writes  []zzWP2Write
}

type zzWP2Write struct {
	size, n int
	err     error
}

func (w *zzWP2WriteLog) Write(p []byte) (int, error) {
	n, err := w.streamConn.Write(p)
	w.mu.Lock()
	w.written += n
	w.writes = append(w.writes, zzWP2Write{size: len(p), n: n, err: err})
	w.mu.Unlock()
	return n, err
}

// CancelRead and CancelWrite keep a wrapped QUIC stream aborted the way
// production aborts it (see abortStream).
func (w *zzWP2WriteLog) CancelRead(code quic.StreamErrorCode) {
	if a, ok := w.streamConn.(streamAborter); ok {
		a.CancelRead(code)
	}
}

func (w *zzWP2WriteLog) CancelWrite(code quic.StreamErrorCode) {
	if a, ok := w.streamConn.(streamAborter); ok {
		a.CancelWrite(code)
	}
}

func (w *zzWP2WriteLog) total() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

// requireZeroByteTimeout fails unless a Write of size bytes timed out
// having written nothing.
func (w *zzWP2WriteLog) requireZeroByteTimeout(t *testing.T, size int) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, wr := range w.writes {
		if wr.size == size && wr.n == 0 && errors.Is(wr.err, os.ErrDeadlineExceeded) {
			return
		}
	}
	t.Fatalf("no %d-byte Write timed out with nothing written; writes = %+v", size, w.writes)
}

// Over a pipe: the peer takes one request, then stops reading, so the
// next request's Write blocks inside Write until its caller's deadline.
func TestZZWP2ZeroByteWriteTimeoutKeepsStream(t *testing.T) {
	clientEnd, server := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close(); _ = server.Close() })
	log := &zzWP2WriteLog{streamConn: clientEnd}
	client := newStreamClient(log, 5*time.Second)
	go client.readLoop()
	reader := bufio.NewReader(server)

	parked := zzWP2Go(func() error {
		_, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("park"))
		return err
	})
	first, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, []byte("late"))
	log.requireZeroByteTimeout(t, zzWP2FrameHeaderBytes+len("late"))
	if client.isClosed() {
		t.Fatalf("a zero-byte write timeout closed the shared stream: %v", client.closeError())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late writer error = %v, want context.DeadlineExceeded", err)
	}
	writeReply(t, server, first.RequestID, []byte("ok"))
	if err := zzWP2Result(t, parked, "request in flight"); err != nil {
		t.Fatalf("request in flight error = %v", err)
	}
}

// zzWP2GateHandler: "park" parks on a goroutine until release is closed,
// "stall" holds the stream's serve loop (so the server stops reading the
// stream) until unstall is closed, anything else is echoed. seen records
// every payload size that reached the handler.
type zzWP2GateHandler struct {
	release, parked   chan struct{}
	unstall, stalled  chan struct{}
	parkOnce, stallOn sync.Once
	mu                sync.Mutex
	seen              []int
}

func newZZWP2GateHandler() *zzWP2GateHandler {
	return &zzWP2GateHandler{
		release: make(chan struct{}), parked: make(chan struct{}),
		unstall: make(chan struct{}), stalled: make(chan struct{}),
	}
}

func (h *zzWP2GateHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	h.mu.Lock()
	h.seen = append(h.seen, len(frame.Payload))
	h.mu.Unlock()
	reply := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID}
	switch string(frame.Payload) {
	case "park":
		go func() {
			h.parkOnce.Do(func() { close(h.parked) })
			<-h.release
			respond(reply)
		}()
	case "stall":
		h.stallOn.Do(func() { close(h.stalled) })
		<-h.unstall
		respond(reply)
	default:
		reply.Payload = frame.Payload
		respond(reply)
	}
	return true
}

func (h *zzWP2GateHandler) sawSize(n int) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, size := range h.seen {
		if size == n {
			return true
		}
	}
	return false
}

// Over real QUIC, the shape of a stalled peer: the server stops reading
// the stream, a large request fills the stream's flow-control window
// and leaves a little of its frame queued in quic-go's send buffer, so
// the next small request cannot be buffered and blocks inside Write
// until its deadline. The stream and the requests already on it (a
// parked long-poll and the large request) must survive that timeout.
func TestZZWP2QUICZeroByteWriteTimeoutKeepsStream(t *testing.T) {
	h := newZZWP2GateHandler()
	addr := zzWP2Server(t, h)
	pool := zzWP2QUICPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pool.getConn(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := conn.openStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	log := &zzWP2WriteLog{streamConn: raw}
	if err := pool.authenticateStream(ctx, conn, log); err != nil {
		t.Fatal(err)
	}
	client := newStreamClient(log, 5*time.Second)
	go client.readLoop()
	t.Cleanup(func() { client.closeWithError(errors.New("test done")) })

	send := func(payload []byte) <-chan error {
		return zzWP2Go(func() error {
			frame, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, payload)
			if err == nil && len(payload) > 5 && len(frame.Payload) != len(payload) {
				err = errors.New("echo reply has the wrong size")
			}
			return err
		})
	}
	parked := send([]byte("park"))
	zzWP2Await(t, h.parked, parked, "parking the long-poll")
	stall := send([]byte("stall"))
	zzWP2Await(t, h.stalled, stall, "stalling the serve loop")

	// The server has stopped reading, and it has read too little to have
	// moved the stream's receive window: the client may send up to
	// quicInitialStreamReceiveWindow bytes in all. Size the large frame
	// so that queued bytes of it are left over past that window.
	const leftover = 1200
	sent := log.total()
	big := make([]byte, quicInitialStreamReceiveWindow+leftover-sent-zzWP2FrameHeaderBytes)
	large := send(big)
	for want := sent + zzWP2FrameHeaderBytes + len(big); log.total() != want; {
		select {
		case err := <-large:
			t.Fatalf("large request ended early: %v", err)
		case <-time.After(time.Millisecond):
		}
		if ctx.Err() != nil {
			t.Fatal("the large frame's Write never returned")
		}
	}

	// leftover plus this frame is more than quic-go buffers for a stream
	// (protocol.MaxPacketBufferSize, 1452 bytes): Write blocks.
	late := make([]byte, 400)
	lateCtx, lateCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer lateCancel()
	_, err = client.requestFrame(lateCtx, clusterwire.StreamFrameNodeRequest, late)
	log.requireZeroByteTimeout(t, zzWP2FrameHeaderBytes+len(late))
	if client.isClosed() {
		t.Fatalf("a zero-byte write timeout closed the shared stream: %v", client.closeError())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("late request error = %v, want context.DeadlineExceeded", err)
	}

	close(h.unstall)
	for what, ch := range map[string]<-chan error{"stalling request": stall, "large request": large} {
		if err := zzWP2Result(t, ch, what); err != nil {
			t.Fatalf("%s error = %v", what, err)
		}
	}
	close(h.release)
	if err := zzWP2Result(t, parked, "parked long-poll"); err != nil {
		t.Fatalf("parked long-poll error = %v", err)
	}
	if h.sawSize(len(late)) {
		t.Fatal("the timed-out frame reached the server")
	}
}

// Over real QUIC through the pool: a request whose own budget (see
// requestWithin) has run out by the time it holds the write slot gets
// quic-go's deadline error at Write entry, having written nothing. The
// stream and a long-poll parked on it must survive.
func TestZZWP2QUICSpentBudgetAtWriteKeepsStream(t *testing.T) {
	h := newZZWP2ParkHandler()
	addr := zzWP2Server(t, h)
	pool := zzWP2QUICPool(t)

	pool.nextShard.Store(0)
	parked := zzWP2Go(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, err := pool.request(ctx, addr, LaneConsume, clusterwire.StreamFrameNodeRequest, []byte("park"))
		return err
	})
	zzWP2Await(t, h.parked, parked, "parking the long-poll")
	key := streamKey{addr: quicAddr(addr), lane: LaneConsume, shard: 0}
	pool.mu.Lock()
	ps := pool.streams[key]
	pool.mu.Unlock()
	// Swap in the write log while holding the write slot, so the swap is
	// ordered after the long-poll's write and before the next one.
	if err := ps.client.lockWrite(context.Background(), time.Time{}); err != nil {
		t.Fatal(err)
	}
	log := &zzWP2WriteLog{streamConn: ps.client.conn}
	ps.client.conn = log
	ps.client.unlockWrite()

	pool.nextShard.Store(0) // same (lane, shard): same stream
	_, err := pool.requestWithin(context.Background(), addr, LaneConsume, time.Nanosecond, clusterwire.StreamFrameNodeRequest, []byte("probe"))
	log.requireZeroByteTimeout(t, zzWP2FrameHeaderBytes+len("probe"))
	if ps.client.isClosed() {
		t.Fatalf("a zero-byte write timeout closed the shared stream: %v", ps.client.closeError())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("spent-budget request error = %v, want context.DeadlineExceeded", err)
	}
	zzWP2Pending(t, parked, "long-poll on the same stream")
	close(h.release)
	if err := zzWP2Result(t, parked, "long-poll"); err != nil {
		t.Fatalf("long-poll error = %v", err)
	}
}

// A frame too big for the staging buffer goes out as header then
// payload. When the header's Write times out with nothing written the
// framing is intact, so the stream must survive as for a staged frame.
func TestZZWP2BigFrameZeroByteTimeoutKeepsStream(t *testing.T) {
	clientEnd, server := net.Pipe()
	t.Cleanup(func() { _ = clientEnd.Close(); _ = server.Close() })
	log := &zzWP2WriteLog{streamConn: clientEnd}
	client := newStreamClient(log, 5*time.Second)
	go client.readLoop()
	reader := bufio.NewReader(server)

	parked := zzWP2Go(func() error {
		_, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("park"))
		return err
	})
	first, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, make([]byte, 1<<20))
	log.requireZeroByteTimeout(t, zzWP2FrameHeaderBytes)
	if client.isClosed() {
		t.Fatalf("a zero-byte write timeout closed the shared stream: %v", client.closeError())
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("big request error = %v, want context.DeadlineExceeded", err)
	}
	writeReply(t, server, first.RequestID, []byte("ok"))
	if err := zzWP2Result(t, parked, "request in flight"); err != nil {
		t.Fatalf("request in flight error = %v", err)
	}
}

// A big frame whose header went out but whose payload was cut off leaves
// a torn frame on the stream, which must close.
func TestZZWP2BigFramePartialWriteClosesStream(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	go func() {
		// Take the header and part of the payload, then stop reading.
		_, _ = io.ReadFull(server, make([]byte, 100))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, make([]byte, 1<<20)); err == nil {
		t.Fatal("torn write reported success")
	}
	if !client.isClosed() {
		t.Fatal("stream left open after a partial frame write")
	}
}

// Neither side stages a frame too big for its retained buffer: writing a
// 1 MiB frame allocates nothing (it used to cost a 1 MiB buffer and a
// copy per frame).
func TestZZWP2BigFrameWritesAllocateNothing(t *testing.T) {
	if zzWP2Race {
		t.Skip("allocation counts differ under the race detector")
	}
	frame := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: make([]byte, 1<<20)}
	client := newStreamClient(zzWP2DiscardConn{}, 5*time.Second)
	deadline := time.Now().Add(time.Hour)
	if got := testing.AllocsPerRun(20, func() {
		if err := client.writeLocked(deadline, frame); err != nil {
			t.Fatal(err)
		}
	}); got != 0 {
		t.Fatalf("client allocs per 1 MiB frame = %v, want 0", got)
	}
	server := newStreamServerConn(zzWP2DiscardConn{}, nil, nil, nil, nil)
	if got := testing.AllocsPerRun(20, func() {
		if !server.writeFrame(frame) {
			t.Fatal("write failed")
		}
	}); got != 0 {
		t.Fatalf("server allocs per 1 MiB frame = %v, want 0", got)
	}
}
