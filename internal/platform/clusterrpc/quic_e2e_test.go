package clusterrpc

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

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
	if string(a1[:]) == string(legacyAuthToken("a")) {
		t.Fatal("stateless reset key collides with the auth token (missing domain separation)")
	}
	if newConnAuth("", sessionBinding{key: testSessionKey}) != nil {
		t.Fatal("empty secret must disable auth")
	}
}

func TestServerTLSUsesECDSAAndClientCachesSessions(t *testing.T) {
	conf, err := quicServerTLSConfig(false)
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
	client := quicClientTLSConfig(false)
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
	return startEchoServerCompat(t, addr, secret, false)
}

// startEchoServerCompat is startEchoServer with an explicit legacy
// compatibility setting.
func startEchoServerCompat(t *testing.T, addr, secret string, allowLegacy bool) (*quicServer, func()) {
	t.Helper()
	server, err := listenQUIC(addr, secret, allowLegacy)
	if err != nil {
		t.Fatalf("listenQUIC() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveQUICListener(ctx, server.listener, listenerAuth{secret: secret, allowLegacy: allowLegacy}, slog.New(slog.NewTextHandler(io.Discard, nil)), echoHandler{})
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
		s, err := listenQUIC(addr, "sekret", false)
		if err != nil {
			if attempt > 20 {
				t.Fatalf("rebind %s: %v", addr, err)
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		restarted = s
	}
	ctx := t.Context()
	go serveQUICListener(ctx, restarted.listener, listenerAuth{secret: "sekret"}, nil, echoHandler{})
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
	p := newQUICClientPool(time.Second, "", false)
	if err := p.close(); err != nil {
		t.Fatalf("close() of an undialed pool error = %v", err)
	}
	if _, err := p.request(context.Background(), "127.0.0.1:1", LaneControl, clusterwire.StreamFrameNodeRequest, nil); !errors.Is(err, errPoolClosed) {
		t.Fatalf("request on closed pool error = %v, want errPoolClosed", err)
	}
}

// The server must prove the secret back: a listener that does not know
// it (a rogue endpoint at a spoofed peer address, say) must not be
// usable, and in particular the client must not send it any request.
func TestQUICClientRefusesServerThatCannotProveSecret(t *testing.T) {
	server, _ := startEchoServer(t, "127.0.0.1:0", "impostor")
	addr := server.addr().String()

	client := NewQUICFrameClient(2*time.Second, "sekret")
	defer client.Close()
	_, err := client.Request(context.Background(), addr, clusterwire.StreamFrameNodeRequest, []byte("ping"))
	if err == nil {
		t.Fatal("request to a server with the wrong secret succeeded")
	}
	// Either side may notice first: the impostor rejects our proof (the
	// stream is reset before its reply is read) or its reply fails our
	// check. Both are refusals; what matters is that no request went out.
	if !strings.Contains(err.Error(), "prove") && !strings.Contains(err.Error(), "auth") && !strings.Contains(err.Error(), "reset") && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("unexpected refusal error: %v", err)
	}
}

// Two upgraded nodes must talk session-bound auth even with the
// compatibility switch on: the current ALPN is listed first on both
// ends, so legacy is never selected between them.
func TestQUICCompatModePrefersSessionBoundAuth(t *testing.T) {
	server, _ := startEchoServerCompat(t, "127.0.0.1:0", "sekret", true)
	addr := server.addr().String()

	pool := newQUICClientPool(2*time.Second, "sekret", true)
	t.Cleanup(func() { _ = pool.close() })
	if _, err := pool.request(context.Background(), addr, LaneControl, clusterwire.StreamFrameNodeRequest, []byte("ping")); err != nil {
		t.Fatalf("request error = %v", err)
	}
	pool.mu.Lock()
	conn := pool.conns[addr]
	pool.mu.Unlock()
	cs, ok := conn.tlsState()
	if !ok || cs.NegotiatedProtocol != quicALPN {
		t.Fatalf("negotiated ALPN = %q, want %q", cs.NegotiatedProtocol, quicALPN)
	}
}

// legacyOnlyServer imitates a node from before session-bound auth: it
// offers only the legacy ALPN and verifies the fixed token without
// replying. (A current listener in compatibility mode behaves exactly
// this way on a legacy-ALPN connection, so trimming its ALPN list is a
// faithful stand-in.)
func legacyOnlyServer(t *testing.T, secret string) string {
	t.Helper()
	tlsConf, err := quicServerTLSConfig(true)
	if err != nil {
		t.Fatal(err)
	}
	tlsConf.NextProtos = []string{quicALPNLegacy}
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	tr := &quic.Transport{Conn: udpConn, StatelessResetKey: statelessResetKey(secret)}
	listener, err := tr.Listen(tlsConf, quicConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = serveQUICListener(ctx, listener, listenerAuth{secret: secret, allowLegacy: true}, nil, echoHandler{})
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		_ = tr.Close()
		_ = udpConn.Close()
		<-done
	})
	return listener.Addr().String()
}

// legacyOnlyDialer imitates a client from before session-bound auth:
// it offers only the legacy ALPN. The pool then sends the fixed token
// and expects no reply, which is what an old client did.
func legacyOnlyDialer(p *quicClientPool) func(ctx context.Context, addr string) (poolConn, error) {
	return func(ctx context.Context, addr string) (poolConn, error) {
		tr, err := p.clientTransport()
		if err != nil {
			return nil, err
		}
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		tlsConf := newSelfSignedClusterClientTLS(true)
		tlsConf.NextProtos = []string{quicALPNLegacy}
		conn, err := tr.Dial(ctx, udpAddr, tlsConf, quicConfig())
		if err != nil {
			return nil, err
		}
		return quicPoolConn{conn: conn}, nil
	}
}

// With compatibility ON, an upgraded client reaches a legacy-only
// server and an upgraded server serves a legacy-only client, both on
// the fixed token. With it OFF (the default), neither pairing gets past
// the TLS handshake: no shared ALPN, so no mis-authentication.
func TestQUICLegacyCompatibilityIsOptIn(t *testing.T) {
	t.Run("new client to legacy server", func(t *testing.T) {
		addr := legacyOnlyServer(t, "sekret")
		compat := newQUICClientPool(2*time.Second, "sekret", true)
		t.Cleanup(func() { _ = compat.close() })
		if _, err := compat.request(context.Background(), addr, LaneControl, clusterwire.StreamFrameNodeRequest, []byte("ping")); err != nil {
			t.Fatalf("compat client to legacy server error = %v", err)
		}
		strict := newQUICClientPool(2*time.Second, "sekret", false)
		t.Cleanup(func() { _ = strict.close() })
		if _, err := strict.request(context.Background(), addr, LaneControl, clusterwire.StreamFrameNodeRequest, []byte("ping")); err == nil {
			t.Fatal("strict client talked to a legacy-only server")
		}
	})
	t.Run("legacy client to new server", func(t *testing.T) {
		compatServer, _ := startEchoServerCompat(t, "127.0.0.1:0", "sekret", true)
		old := newQUICClientPool(2*time.Second, "sekret", true)
		old.dial = legacyOnlyDialer(old)
		t.Cleanup(func() { _ = old.close() })
		if _, err := old.request(context.Background(), compatServer.addr().String(), LaneControl, clusterwire.StreamFrameNodeRequest, []byte("ping")); err != nil {
			t.Fatalf("legacy client to compat server error = %v", err)
		}
		// A wrong fixed token is still a wrong token.
		bad := newQUICClientPool(2*time.Second, "wrong", true)
		bad.dial = legacyOnlyDialer(bad)
		t.Cleanup(func() { _ = bad.close() })
		if _, err := bad.request(context.Background(), compatServer.addr().String(), LaneControl, clusterwire.StreamFrameNodeRequest, []byte("ping")); err == nil {
			t.Fatal("compat server accepted a wrong legacy token")
		}

		strictServer, _ := startEchoServer(t, "127.0.0.1:0", "sekret")
		old2 := newQUICClientPool(2*time.Second, "sekret", true)
		old2.dial = legacyOnlyDialer(old2)
		t.Cleanup(func() { _ = old2.close() })
		if _, err := old2.request(context.Background(), strictServer.addr().String(), LaneControl, clusterwire.StreamFrameNodeRequest, []byte("ping")); err == nil {
			t.Fatal("strict server served a legacy-only client")
		}
	})
}

// The exported keying material is per TLS session: two connections to
// the same server, even with session resumption, must not share it, or
// the proof would be replayable between them.
func TestQUICSessionKeyDiffersPerConnection(t *testing.T) {
	server, _ := startEchoServer(t, "127.0.0.1:0", "sekret")
	addr := server.addr().String()

	keys := make([][]byte, 0, 2)
	for range 2 {
		pool := newQUICClientPool(2*time.Second, "sekret", false)
		if _, err := pool.request(context.Background(), addr, LaneControl, clusterwire.StreamFrameNodeRequest, []byte("ping")); err != nil {
			t.Fatalf("request error = %v", err)
		}
		pool.mu.Lock()
		cs, _ := pool.conns[addr].tlsState()
		pool.mu.Unlock()
		b, err := sessionBindingFrom(cs, false)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, b.key)
		_ = pool.close()
	}
	if string(keys[0]) == string(keys[1]) {
		t.Fatal("two connections exported the same keying material")
	}
}
