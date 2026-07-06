package clusterrpc

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// echoHandler replies to a node-request frame so a test can tell the
// stream was actually served past the auth gate.
type echoHandler struct{}

func (echoHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: frame.Payload})
	return true
}

// serveAuthPipe serves one pipe end with the given secret and returns
// the client end for the test to drive.
func serveAuthPipe(t *testing.T, secret string) net.Conn {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	go ServeStreamConn(serverConn, serverConn, secret, slog.New(slog.NewTextHandler(io.Discard, nil)), echoHandler{})
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	return clientConn
}

func TestServerAcceptsValidAuthThenServes(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if err := clusterwire.WriteStreamFrame(conn, authFrame("sekret")); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	req := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 7, Payload: []byte("ping")}
	if err := clusterwire.WriteStreamFrame(conn, req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	reply, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if reply.Type != clusterwire.StreamFrameNodeReply || reply.RequestID != 7 || string(reply.Payload) != "ping" {
		t.Fatalf("unexpected reply %+v", reply)
	}
}

func TestServerRejectsInvalidAuth(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Wrong secret: the server must close the stream without serving.
	if err := clusterwire.WriteStreamFrame(conn, authFrame("wrong")); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	req := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: []byte("x")}
	_ = clusterwire.WriteStreamFrame(conn, req)

	// The read must fail (closed), not return a reply.
	if _, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes); err == nil {
		t.Fatal("server served a request after invalid auth")
	}
}

func TestServerRejectsRequestFrameInPlaceOfAuth(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Skipping auth and sending a request first must be rejected.
	req := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: []byte("x")}
	if err := clusterwire.WriteStreamFrame(conn, req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if _, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes); err == nil {
		t.Fatal("server served a request without auth")
	}
}
