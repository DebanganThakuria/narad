package clusterrpc

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2ReusingHandler replies from one buffer it overwrites as soon as
// respond returns, relying on respond not retaining the payload.
type zzWP2ReusingHandler struct{ buf []byte }

func (h *zzWP2ReusingHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	h.buf = append(h.buf[:0], frame.Payload...)
	respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: h.buf})
	for i := range h.buf {
		h.buf[i] = 'X'
	}
	return true
}

// A reply payload is copied before respond returns, so a handler may
// reuse its buffer at once.
func TestZZWP2RespondDoesNotRetainThePayload(t *testing.T) {
	serverEnd, clientEnd := net.Pipe()
	t.Cleanup(func() { _ = serverEnd.Close(); _ = clientEnd.Close() })
	go ServeStreamConn(serverEnd, serverEnd, nil, &zzWP2ReusingHandler{})
	_ = clientEnd.SetDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(clientEnd)
	for i, want := range []string{"first reply", "second"} {
		if err := clusterwire.WriteStreamFrame(clientEnd, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: uint64(i + 1), Payload: []byte(want)}); err != nil {
			t.Fatal(err)
		}
		reply, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes)
		if err != nil {
			t.Fatal(err)
		}
		if string(reply.Payload) != want {
			t.Fatalf("reply %d = %q, want %q", i, reply.Payload, want)
		}
	}
}

// zzWP2BigReusingHandler replies to a one-byte request with a big buffer
// full of that byte, which it scribbles over as soon as respond returns.
type zzWP2BigReusingHandler struct {
	mu  sync.Mutex
	buf []byte
}

func (h *zzWP2BigReusingHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest || len(frame.Payload) != 1 {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.buf {
		h.buf[i] = frame.Payload[0]
	}
	respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: h.buf})
	for i := range h.buf {
		h.buf[i] = 'X'
	}
	return true
}

// A reply too big to stage is written from the handler's own buffer
// rather than copied (see clusterwire.WriteStreamFrameStaged), so the
// contract rests on the stream's Write: over real QUIC it must not keep
// the payload once it returns.
func TestZZWP2RespondDoesNotRetainABigPayload(t *testing.T) {
	addr := zzWP2Server(t, &zzWP2BigReusingHandler{buf: make([]byte, 1<<20)})
	pool := zzWP2QUICPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, b := range []byte("abcdefgh") {
		reply, err := pool.request(ctx, addr, LaneProduce, clusterwire.StreamFrameNodeRequest, []byte{b})
		if err != nil {
			t.Fatal(err)
		}
		if len(reply.Payload) != 1<<20 {
			t.Fatalf("reply %q is %d bytes, want %d", b, len(reply.Payload), 1<<20)
		}
		if i := bytes.IndexFunc(reply.Payload, func(r rune) bool { return r != rune(b) }); i >= 0 {
			t.Fatalf("reply %q has %q at byte %d: the payload changed after respond returned", b, reply.Payload[i], i)
		}
	}
}
