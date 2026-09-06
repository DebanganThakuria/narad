package clusterrpc

// Fuzz targets for the stream serve loop and the client read loop: the
// two places arbitrary peer bytes are turned into frames and acted on.
// The invariants: neither loop panics, both terminate once the peer
// goes away, an unauthenticated peer never gets a frame served, and a
// client waiting on a reply is always released.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// fuzzEchoHandler answers every node request with a reply carrying the same
// payload and counts what it served.
type fuzzEchoHandler struct {
	served atomic.Int64
}

func (h *fuzzEchoHandler) HandleStreamFrame(frame clusterwire.StreamFrame, respond func(clusterwire.StreamFrame)) bool {
	if frame.Type != clusterwire.StreamFrameNodeRequest {
		return false
	}
	h.served.Add(1)
	respond(clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: frame.RequestID, Payload: frame.Payload})
	return true
}

func fuzzFrame(typ clusterwire.StreamFrameType, id uint64, payload []byte) []byte {
	return clusterwire.AppendStreamFrame(nil, clusterwire.StreamFrame{Type: typ, RequestID: id, Payload: payload})
}

// fuzzSecret is the cluster secret the authenticated mode expects. The
// session binding is a fixed key: the proof only depends on the secret
// and the exported keying material, and the fuzzer wants to be able to
// produce the correct proof as well as every wrong one.
const fuzzSecret = "fuzz-secret"

var fuzzSessionKey = bytes.Repeat([]byte{0x5a}, sessionAuthKeyBytes)

// FuzzServeStreamConn writes arbitrary bytes into a served stream and
// drains whatever comes back. The serve loop must return within a
// bounded time once the peer closes, must not panic, and with auth on
// must serve nothing unless the first frame is the exact proof.
func FuzzServeStreamConn(f *testing.F) {
	proof := clientProof(fuzzSecret, fuzzSessionKey)
	f.Add(fuzzFrame(clusterwire.StreamFrameNodeRequest, 1, []byte("req")), false)
	f.Add(append(fuzzFrame(clusterwire.StreamFramePing, 1, nil), fuzzFrame(clusterwire.StreamFrameCancel, 1, nil)...), false)
	f.Add(append(fuzzFrame(clusterwire.StreamFrameAuth, 0, proof), fuzzFrame(clusterwire.StreamFrameNodeRequest, 2, []byte("req"))...), true)
	f.Add(append(fuzzFrame(clusterwire.StreamFrameAuth, 0, bytes.Repeat([]byte{0}, 32)), fuzzFrame(clusterwire.StreamFrameNodeRequest, 2, []byte("req"))...), true)
	f.Add(fuzzFrame(clusterwire.StreamFrameNodeRequest, 1, []byte("req")), true)
	f.Add([]byte("NRS1"), false)
	f.Add([]byte{}, true)
	// Header claiming a 4 GiB payload.
	f.Add([]byte{'N', 'R', 'S', '1', 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff}, false)

	f.Fuzz(func(t *testing.T, data []byte, withAuth bool) {
		clientEnd, serverEnd := net.Pipe()
		defer clientEnd.Close()
		defer serverEnd.Close()

		var auth *connAuth
		if withAuth {
			auth = newConnAuth(fuzzSecret, sessionBinding{key: fuzzSessionKey})
		}
		handler := &fuzzEchoHandler{}
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))

		served := make(chan struct{})
		go func() {
			defer close(served)
			serveStreamConn(serverEnd, bufio.NewReader(serverEnd), auth, logger, handler)
		}()

		// Drain the server's replies so its (synchronous pipe) writes
		// never block, and collect them for the auth check.
		var replies bytes.Buffer
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			_, _ = io.Copy(&replies, clientEnd)
		}()

		// Feed the input, then close our end so the server sees EOF.
		_ = clientEnd.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, _ = clientEnd.Write(data)
		_ = clientEnd.Close()

		select {
		case <-served:
		case <-time.After(5 * time.Second):
			t.Fatal("serve loop did not return after the peer closed")
		}
		<-drained

		if withAuth {
			// The peer is authenticated iff its FIRST frame is an auth frame
			// carrying exactly the proof; its request ID and reserved bytes
			// do not matter.
			first, err := clusterwire.ReadStreamFrame(bytes.NewReader(data), maxAuthFramePayloadBytes)
			authenticated := err == nil && first.Type == clusterwire.StreamFrameAuth && bytes.Equal(first.Payload, proof)
			if !authenticated && handler.served.Load() != 0 {
				t.Fatalf("served %d requests on a stream that never proved the secret", handler.served.Load())
			}
			if !authenticated && replies.Len() != 0 {
				t.Fatalf("wrote %d bytes to a peer that never proved the secret", replies.Len())
			}
		}
	})
}

// FuzzStreamClientReadLoop hands a client arbitrary server bytes while a
// request is in flight. The request must complete (with a reply or an
// error) within the client timeout, the read loop must exit once the
// server closes, and nothing may panic.
func FuzzStreamClientReadLoop(f *testing.F) {
	f.Add(fuzzFrame(clusterwire.StreamFrameNodeReply, 1, []byte("reply")))
	f.Add(fuzzFrame(clusterwire.StreamFrameError, 1, []byte{0, 4, 'b', 'o', 'o', 'm'}))
	f.Add(fuzzFrame(clusterwire.StreamFrameError, 1, []byte{0xff, 0xff}))
	f.Add(fuzzFrame(clusterwire.StreamFramePong, 1, nil))
	f.Add(fuzzFrame(clusterwire.StreamFrameNodeReply, 2, []byte("wrong id")))
	f.Add(fuzzFrame(clusterwire.StreamFrameAuth, 1, nil))
	f.Add([]byte("garbage"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		clientEnd, serverEnd := net.Pipe()
		defer clientEnd.Close()
		defer serverEnd.Close()

		client := newStreamClient(clientEnd, 500*time.Millisecond)
		go client.readLoop()

		// Absorb the request frame the client writes.
		go func() { _, _ = io.Copy(io.Discard, serverEnd) }()

		done := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := client.requestFrame(ctx, clusterwire.StreamFrameNodeRequest, []byte("req"))
			done <- err
		}()

		_ = serverEnd.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, _ = serverEnd.Write(data)
		_ = serverEnd.Close()

		select {
		case err := <-done:
			if err != nil && errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("request hung past the client timeout: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("request never completed")
		}
		deadline := time.Now().Add(2 * time.Second)
		for !client.isClosed() {
			if time.Now().After(deadline) {
				t.Fatal("read loop still running after the server closed")
			}
			time.Sleep(time.Millisecond)
		}
		if n := pendingCount(client); n != 0 {
			t.Fatalf("%d requests still pending after close", n)
		}
	})
}
