package clusterrpc

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

func TestStatelessResetKeyIsDeterministicAndSecretSpecific(t *testing.T) {
	a1, a2, b := statelessResetKey("a"), statelessResetKey("a"), statelessResetKey("b")
	if *a1 != *a2 {
		t.Fatal("stateless reset key is not deterministic")
	}
	if *a1 == *b {
		t.Fatal("different secrets produced the same stateless reset key")
	}
	if string(a1[:]) == string(authToken("a")) {
		t.Fatal("stateless reset key collides with the auth token (missing domain separation)")
	}
	if expectedAuthToken("") != nil {
		t.Fatal("empty secret must disable auth")
	}
}

func TestServerTLSUsesECDSAAndClientCachesSessions(t *testing.T) {
	conf, err := quicServerTLSConfig()
	if err != nil {
		t.Fatalf("quicServerTLSConfig() error = %v", err)
	}
	cert, err := x509.ParseCertificate(conf.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("ParseCertificate() error = %v", err)
	}
	if cert.PublicKeyAlgorithm != x509.ECDSA {
		t.Fatalf("certificate key algorithm = %s, want ECDSA", cert.PublicKeyAlgorithm)
	}
	client := quicClientTLSConfig()
	if client.ClientSessionCache == nil {
		t.Fatal("client tls config has no session cache")
	}
	if len(client.NextProtos) != 1 || client.NextProtos[0] != quicALPN || conf.NextProtos[0] != quicALPN {
		t.Fatal("ALPN pin missing on client or server config")
	}
	if !client.InsecureSkipVerify {
		t.Fatal("client must not verify the ephemeral self-signed certificate")
	}
}

// startEchoServer binds a real QUIC listener on loopback and serves the
// echo handler until the returned stop function is called.
func startEchoServer(t *testing.T, addr, secret string) (*quicServer, func()) {
	t.Helper()
	server, err := listenQUIC(addr, secret)
	if err != nil {
		t.Fatalf("listenQUIC() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveQUICListener(ctx, server.listener, expectedAuthToken(secret), slog.New(slog.NewTextHandler(io.Discard, nil)), echoHandler{})
	}()
	stop := func() {
		cancel()
		server.close()
		<-done
	}
	t.Cleanup(stop)
	return server, stop
}

func TestQUICRoundTripAndAuth(t *testing.T) {
	server, _ := startEchoServer(t, "127.0.0.1:0", "sekret")
	addr := server.addr().String()

	client := NewQUICFrameClient(2*time.Second, "sekret")
	defer client.Close()
	for _, lane := range []Lane{LaneControl, LaneProduce, LaneConsume, LaneAck} {
		reply, err := client.RequestOnLane(context.Background(), addr, lane, clusterwire.StreamFrameNodeRequest, []byte("ping"))
		if err != nil {
			t.Fatalf("lane %s request error = %v", lane, err)
		}
		if reply.Type != clusterwire.StreamFrameNodeReply || string(reply.Payload) != "ping" {
			t.Fatalf("lane %s reply = %+v", lane, reply)
		}
	}

	wrong := NewQUICFrameClient(2*time.Second, "wrong")
	defer wrong.Close()
	if _, err := wrong.Request(context.Background(), addr, clusterwire.StreamFrameNodeRequest, []byte("ping")); err == nil {
		t.Fatal("request with the wrong secret succeeded")
	}
}

// A peer that restarts (its transport is torn down without notifying
// anyone, then comes back on the same port) must be reachable again
// promptly. The stateless reset key derived from the shared secret is
// what makes the client drop the dead connection: without it the client
// would wait out the 30s MaxIdleTimeout.
func TestQUICClientRecoversFromPeerRestart(t *testing.T) {
	server, stop := startEchoServer(t, "127.0.0.1:0", "sekret")
	addr := server.addr().String()

	client := NewQUICFrameClient(2*time.Second, "sekret")
	defer client.Close()
	if _, err := client.Request(context.Background(), addr, clusterwire.StreamFrameNodeRequest, []byte("before")); err != nil {
		t.Fatalf("request before restart error = %v", err)
	}

	stop()
	var restarted *quicServer
	for attempt := 0; restarted == nil; attempt++ {
		s, err := listenQUIC(addr, "sekret")
		if err != nil {
			if attempt > 20 {
				t.Fatalf("rebind %s: %v", addr, err)
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		restarted = s
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go serveQUICListener(ctx, restarted.listener, expectedAuthToken("sekret"), nil, echoHandler{})
	t.Cleanup(restarted.close)

	start := time.Now()
	var lastErr error
	for attempt := range 5 {
		reqCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := client.Request(reqCtx, addr, clusterwire.StreamFrameNodeRequest, []byte("after"))
		cancel()
		if err == nil {
			if string(reply.Payload) != "after" {
				t.Fatalf("reply after restart = %+v", reply)
			}
			if elapsed := time.Since(start); elapsed > 10*time.Second {
				t.Fatalf("recovery took %s (attempt %d), want well under the 30s idle timeout", elapsed, attempt)
			}
			return
		}
		lastErr = err
		// The first attempt after the restart may be the one that learns
		// the old connection is dead; back off briefly and retry.
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("client never recovered after peer restart: %v", lastErr)
}

func TestQUICFrameClientNilSafety(t *testing.T) {
	var client *QUICFrameClient
	if _, err := client.Request(context.Background(), "x", clusterwire.StreamFrameNodeRequest, nil); err == nil {
		t.Fatal("nil client request succeeded")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("nil client Close() error = %v", err)
	}
	p := newQUICClientPool(time.Second, "")
	if err := p.close(); err != nil {
		t.Fatalf("close() of an undialed pool error = %v", err)
	}
	if _, err := p.request(context.Background(), "127.0.0.1:1", LaneControl, clusterwire.StreamFrameNodeRequest, nil); !errors.Is(err, errPoolClosed) {
		t.Fatalf("request on closed pool error = %v, want errPoolClosed", err)
	}
}
