package clusterrpc

import (
	"bufio"
	"bytes"
	"encoding/binary"
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

// serveAuthPipe serves one pipe end with the given secret (bound to
// testSessionKey) and returns the client end for the test to drive.
func serveAuthPipe(t *testing.T, secret string) net.Conn {
	t.Helper()
	return serveAuthPipeWith(t, testConnAuth(secret))
}

func serveAuthPipeWith(t *testing.T, auth *connAuth) net.Conn {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	go serveStreamConn(serverConn, serverConn, auth, slog.New(slog.NewTextHandler(io.Discard, nil)), echoHandler{})
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	return clientConn
}

// handshakeAsClient runs the client side of the session-bound handshake
// on a pipe served with testSessionKey: send the client proof, read and
// check the server's proof.
func handshakeAsClient(t *testing.T, conn net.Conn, secret string) {
	t.Helper()
	if err := clusterwire.WriteStreamFrame(conn, authFrame(clientProof(secret, testSessionKey))); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	reply, err := clusterwire.ReadStreamFrame(conn, maxAuthFramePayloadBytes)
	if err != nil {
		t.Fatalf("read server proof: %v", err)
	}
	if !verifyAuthToken(serverProof(secret, testSessionKey), reply) {
		t.Fatalf("server proof did not verify: %+v", reply)
	}
}

func TestServerAcceptsValidAuthProvesItselfThenServes(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	handshakeAsClient(t, conn, "sekret")
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

// The server's reply proof carries the server role: a server that
// merely echoed the client's proof back would be an impostor with a
// captured frame, and the client-side check must not accept it.
func TestServerProofIsNotAReflectionOfTheClientProof(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	client := clientProof("sekret", testSessionKey)
	if err := clusterwire.WriteStreamFrame(conn, authFrame(client)); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	reply, err := clusterwire.ReadStreamFrame(conn, maxAuthFramePayloadBytes)
	if err != nil {
		t.Fatalf("read server proof: %v", err)
	}
	if bytes.Equal(reply.Payload, client) {
		t.Fatal("server answered with the client's own proof")
	}
	if !verifyAuthToken(serverProof("sekret", testSessionKey), reply) {
		t.Fatal("server proof did not verify")
	}
}

func TestServerRejectsInvalidAuth(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Wrong secret: the server must close the stream without serving.
	if err := clusterwire.WriteStreamFrame(conn, authFrame(clientProof("wrong", testSessionKey))); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	req := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: []byte("x")}
	_ = clusterwire.WriteStreamFrame(conn, req)

	// The read must fail (closed), not return a reply.
	if _, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes); err == nil {
		t.Fatal("server served a request after invalid auth")
	}
}

// A proof captured on another TLS session (a different exported key)
// is worthless here even though it was made with the right secret.
func TestServerRejectsProofFromAnotherSession(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	otherSession := bytes.Repeat([]byte{0x11}, sessionAuthKeyBytes)
	if err := clusterwire.WriteStreamFrame(conn, authFrame(clientProof("sekret", otherSession))); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if _, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes); err == nil {
		t.Fatal("server accepted a proof bound to a different session")
	}
}

// The legacy fixed token is not a valid proof on a session-bound
// connection, even with the right secret.
func TestServerRejectsLegacyTokenOnSessionBoundConnection(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if err := clusterwire.WriteStreamFrame(conn, authFrame(legacyAuthToken("sekret"))); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if _, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes); err == nil {
		t.Fatal("server accepted the legacy fixed token on a v2 connection")
	}
}

// In legacy mode (peer on the old ALPN, compatibility on) the fixed
// token is accepted and NO server proof is sent: old clients start
// their read loop right away and would treat an auth frame as a
// protocol error.
func TestServerLegacyModeAcceptsFixedTokenWithoutReply(t *testing.T) {
	conn := serveAuthPipeWith(t, newConnAuth("sekret", sessionBinding{legacy: true}))
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	if err := clusterwire.WriteStreamFrame(conn, authFrame(legacyAuthToken("sekret"))); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	req := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 3, Payload: []byte("old")}
	if err := clusterwire.WriteStreamFrame(conn, req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	reply, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if reply.Type != clusterwire.StreamFrameNodeReply || reply.RequestID != 3 {
		t.Fatalf("legacy peer got %+v instead of its reply (server sent an unexpected frame first?)", reply)
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

// An unauthenticated peer used to be able to pin 16 MiB per stream by
// sending a 20-byte header claiming a 16 MiB auth frame: the server
// allocated the payload buffer before reading it. The pre-auth read is
// capped at 64 bytes, so an oversized claim is rejected from the header
// alone, before any payload arrives.
func TestServerRejectsOversizedAuthFrameFromHeaderAlone(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	var header [20]byte
	binary.BigEndian.PutUint32(header[0:4], 0x4e525331) // NRS1
	header[4] = 1
	header[5] = byte(clusterwire.StreamFrameAuth)
	binary.BigEndian.PutUint32(header[16:20], 16<<20) // claim 16 MiB, send none
	if _, err := conn.Write(header[:]); err != nil {
		t.Fatalf("write header: %v", err)
	}
	// The server must close without waiting for the payload: a read on
	// the client end fails promptly instead of hanging on a server that
	// is blocked reading 16 MiB that will never come.
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("server kept the stream open after an oversized auth frame header")
	}
}

// A frame just over the cap is rejected too, so the cap is exact.
func TestAuthFrameCapIsExact(t *testing.T) {
	conn := serveAuthPipe(t, "sekret")
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))

	oversized := bytes.Repeat([]byte{1}, maxAuthFramePayloadBytes+1)
	if err := clusterwire.WriteStreamFrame(conn, authFrame(oversized)); err != nil {
		// The pipe may be reset before the whole payload is written; either
		// way the stream must not be served.
		return
	}
	if _, err := clusterwire.ReadStreamFrame(bufio.NewReader(conn), clusterwire.MaxStreamFramePayloadBytes); err == nil {
		t.Fatalf("server served after a %d-byte auth frame", len(oversized))
	}
}
