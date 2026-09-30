package clusterrpc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// cluster-rpc-transport#1: a request whose deadline has already passed
// must fail alone, without writing, and leave the shared stream and the
// long-poll multiplexed on it running.
func TestZZWP2ExpiredDeadlineKeepsSharedStream(t *testing.T) {
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

	pool.nextShard.Store(0) // same (lane, shard): same stream
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	if _, err := pool.request(expired, addr, LaneConsume, clusterwire.StreamFrameNodeRequest, []byte("probe")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired caller error = %v, want context.DeadlineExceeded", err)
	}
	zzWP2Pending(t, parked, "long-poll on the same stream")
	close(h.release)
	if err := zzWP2Result(t, parked, "long-poll"); err != nil {
		t.Fatalf("long-poll error = %v", err)
	}
}

// cluster-rpc-transport#1, realistic shape: a 500ms probe queues for the
// stream behind a 4 MiB write stalled on flow control (the server's serve
// loop is busy). The probe must give up at its own deadline without
// touching the stream; the stalled write and a parked long-poll on the
// same stream must still complete.
func TestZZWP2QueuedCallerGivesUpWithoutKillingStream(t *testing.T) {
	h := newZZWP2ParkHandler()
	h.blockFor = 900 * time.Millisecond
	addr := zzWP2Server(t, h)
	pool := zzWP2QUICPool(t)

	onShard0 := func(ctx context.Context, payload []byte) error {
		pool.nextShard.Store(0)
		_, err := pool.request(ctx, addr, LaneConsume, clusterwire.StreamFrameNodeRequest, payload)
		return err
	}
	long := func(payload []byte) <-chan error {
		ch := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			ch <- onShard0(ctx, payload)
		}()
		return ch
	}

	parked := long([]byte("park"))
	zzWP2Await(t, h.parked, parked, "parking the long-poll")
	blocked := long([]byte("block"))
	time.Sleep(50 * time.Millisecond)
	big := long(bytes.Repeat([]byte("z"), 4<<20))
	time.Sleep(50 * time.Millisecond)

	probeCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := onShard0(probeCtx, []byte("probe"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued probe error = %v, want context.DeadlineExceeded", err)
	}
	if waited := time.Since(start); waited > 800*time.Millisecond {
		t.Fatalf("queued probe returned after %s, want about its 500ms budget", waited)
	}
	if err := zzWP2Result(t, big, "4 MiB request"); err != nil {
		t.Fatalf("4 MiB request error = %v", err)
	}
	if err := zzWP2Result(t, blocked, "blocking request"); err != nil {
		t.Fatalf("blocking request error = %v", err)
	}
	zzWP2Pending(t, parked, "parked long-poll")
	close(h.release)
	if err := zzWP2Result(t, parked, "parked long-poll"); err != nil {
		t.Fatalf("parked long-poll error = %v", err)
	}
}

// cluster-rpc-transport#1: an oversized frame is refused before anything
// is written, so the stream (and its other RPCs) must survive it.
func TestZZWP2OversizedFrameKeepsStream(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	frames := serveFrames(server)
	if _, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, make([]byte, clusterwire.MaxStreamFramePayloadBytes+1)); err == nil {
		t.Fatal("oversized frame was accepted")
	}
	if client.isClosed() {
		t.Fatal("an oversized frame closed the shared stream")
	}
	res := zzWP2Go(func() error {
		frame, err := client.requestFrame(context.Background(), clusterwire.StreamFrameNodeRequest, []byte("x"))
		if err == nil && string(frame.Payload) != "ok" {
			err = errors.New("unexpected reply")
		}
		return err
	})
	req := <-frames
	writeReply(t, server, req.RequestID, []byte("ok"))
	if err := zzWP2Result(t, res, "request after oversized frame"); err != nil {
		t.Fatalf("request after oversized frame error = %v", err)
	}
	if n := pendingCount(client); n != 0 {
		t.Fatalf("pending entries = %d, want 0", n)
	}
}

// cluster-rpc-transport#1: a write cut off part-way leaves a torn frame
// on the stream; that stream can carry nothing more and must close.
func TestZZWP2PartialWriteClosesStream(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	go func() {
		// Take part of the frame, then stop reading.
		_, _ = io.ReadFull(server, make([]byte, 100))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, make([]byte, 64<<10)); err == nil {
		t.Fatal("torn write reported success")
	}
	if !client.isClosed() {
		t.Fatal("stream left open after a partial frame write")
	}
}

// cluster-rpc-transport#1: a caller that gives up while another frame is
// being written must not wait for that write to send its cancel: the
// cancel is queued off its path and still reaches the server.
func TestZZWP2CancelDoesNotWaitBehindAStalledWrite(t *testing.T) {
	client, server := newTestStreamClient(t, 5*time.Second)
	reader := bufio.NewReader(server)

	ctxB, cancelB := context.WithCancel(context.Background())
	b := zzWP2Go(func() error {
		_, err := client.requestFrame(ctxB, clusterwire.StreamFrameNodeRequest, []byte("b"))
		return err
	})
	frameB, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	// A: a large frame whose write blocks, holding the write slot, while
	// the server is not reading.
	a := zzWP2Go(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, make([]byte, 64<<10))
		return err
	})
	// A holds the write slot once its frame starts arriving: frameB was
	// the only frame before it, and bufio buffers at most 4 KiB of A's
	// 64 KiB, so A stays mid-write, holding the slot, until frameA is
	// read below. Peek consumes nothing.
	if _, err := reader.Peek(1); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	cancelB()
	if err := zzWP2Result(t, b, "cancelled request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request error = %v, want context.Canceled", err)
	}
	if waited := time.Since(start); waited > 200*time.Millisecond {
		t.Fatalf("cancelled request returned after %s, held behind the stalled write", waited)
	}

	frameA, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
	if err != nil || len(frameA.Payload) != 64<<10 {
		t.Fatalf("large frame = %d bytes, %v", len(frameA.Payload), err)
	}
	cancel, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
	if err != nil {
		t.Fatal(err)
	}
	if cancel.Type != clusterwire.StreamFrameCancel || cancel.RequestID != frameB.RequestID {
		t.Fatalf("next frame = %+v, want a cancel for request %d", cancel, frameB.RequestID)
	}
	writeReply(t, server, frameA.RequestID, []byte("a"))
	if err := zzWP2Result(t, a, "large request"); err != nil {
		t.Fatalf("large request error = %v", err)
	}
	if client.isClosed() {
		t.Fatal("stream closed")
	}
}
