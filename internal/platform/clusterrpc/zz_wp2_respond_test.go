package clusterrpc

import (
	"bufio"
	"net"
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
