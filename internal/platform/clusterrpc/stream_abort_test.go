package clusterrpc

import (
	"bufio"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// quicLikeConn is a pipe end that also exposes the QUIC stream cancel
// surface, recording what the transport calls.
type quicLikeConn struct {
	net.Conn
	cancelRead  atomic.Int32
	cancelWrite atomic.Int32
	closes      atomic.Int32
}

func (c *quicLikeConn) CancelRead(quic.StreamErrorCode)  { c.cancelRead.Add(1) }
func (c *quicLikeConn) CancelWrite(quic.StreamErrorCode) { c.cancelWrite.Add(1) }
func (c *quicLikeConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

// On a QUIC stream Close only closes the send side; a blocked Read stays
// blocked. Closing a stream client must cancel both directions.
func TestClientCloseCancelsBothDirectionsOnQUICStream(t *testing.T) {
	clientEnd, serverEnd := net.Pipe()
	defer serverEnd.Close()
	conn := &quicLikeConn{Conn: clientEnd}
	client := newStreamClient(conn, time.Second)

	client.closeWithError(errors.New("boom"))

	if conn.cancelRead.Load() != 1 || conn.cancelWrite.Load() != 1 {
		t.Fatalf("CancelRead=%d CancelWrite=%d, want 1/1", conn.cancelRead.Load(), conn.cancelWrite.Load())
	}
	if conn.closes.Load() != 0 {
		t.Fatalf("Close called %d times on a QUIC stream, want CancelRead/CancelWrite instead", conn.closes.Load())
	}
	if !client.isClosed() {
		t.Fatal("client not marked closed")
	}
}

// Pipes and TCP have no cancel surface: Close is what unblocks them.
func TestClientCloseFallsBackToCloseOnPlainConn(t *testing.T) {
	client, server := newTestStreamClient(t, time.Second)
	client.closeWithError(errors.New("boom"))
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := server.Read(make([]byte, 1)); err == nil {
		t.Fatal("server end still readable after client close")
	}
}

// The server's failure path (auth rejected, corrupt frame, failed reply
// write) must abort both directions of a QUIC stream.
func TestServerFailurePathAbortsQUICStream(t *testing.T) {
	for name, drive := range map[string]func(t *testing.T, peer net.Conn){
		"invalid auth": func(t *testing.T, peer net.Conn) {
			if err := clusterwire.WriteStreamFrame(peer, authFrame("wrong")); err != nil {
				t.Fatalf("write auth: %v", err)
			}
		},
		"corrupt frame": func(t *testing.T, peer net.Conn) {
			if err := clusterwire.WriteStreamFrame(peer, authFrame("sekret")); err != nil {
				t.Fatalf("write auth: %v", err)
			}
			// Exactly one header's worth of garbage: the server reads a
			// 20-byte header, rejects the magic, and stops reading.
			if _, err := peer.Write([]byte("not a frame header! ")); err != nil {
				t.Fatalf("write garbage: %v", err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			serverEnd, peer := net.Pipe()
			defer peer.Close()
			conn := &quicLikeConn{Conn: serverEnd}
			done := make(chan struct{})
			go func() {
				defer close(done)
				ServeStreamConn(conn, conn, "sekret", nil, echoHandler{})
			}()
			_ = peer.SetDeadline(time.Now().Add(2 * time.Second))
			drive(t, peer)
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("server did not finish")
			}
			if conn.cancelRead.Load() != 1 || conn.cancelWrite.Load() != 1 {
				t.Fatalf("CancelRead=%d CancelWrite=%d, want 1/1", conn.cancelRead.Load(), conn.cancelWrite.Load())
			}
		})
	}
}

// A clean EOF from the peer is not a failure: the server closes its send
// side gracefully rather than resetting the stream.
func TestServerCleanEOFClosesSendSide(t *testing.T) {
	serverEnd, peer := net.Pipe()
	conn := &quicLikeConn{Conn: serverEnd}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ServeStreamConn(conn, conn, "", nil, echoHandler{})
	}()
	_ = peer.SetDeadline(time.Now().Add(2 * time.Second))
	req := clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, RequestID: 1, Payload: []byte("x")}
	if err := clusterwire.WriteStreamFrame(peer, req); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if _, err := clusterwire.ReadStreamFrame(bufio.NewReader(peer), clusterwire.MaxStreamFramePayloadBytes); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	_ = peer.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not finish after peer EOF")
	}
	if conn.closes.Load() != 1 || conn.cancelRead.Load() != 0 || conn.cancelWrite.Load() != 0 {
		t.Fatalf("Close=%d CancelRead=%d CancelWrite=%d, want graceful Close only", conn.closes.Load(), conn.cancelRead.Load(), conn.cancelWrite.Load())
	}
}

func TestReplyWriteTimeoutScalesWithPayload(t *testing.T) {
	for payload, want := range map[int]time.Duration{
		0:               5 * time.Second,
		(256 << 10) - 1: 5 * time.Second,
		256 << 10:       6 * time.Second,
		1 << 20:         9 * time.Second,
		16 << 20:        69 * time.Second,
	} {
		if got := replyWriteTimeout(payload); got != want {
			t.Fatalf("replyWriteTimeout(%d) = %s, want %s", payload, got, want)
		}
	}
}
