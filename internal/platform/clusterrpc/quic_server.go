package clusterrpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/quic-go/quic-go"
)

// ServeQUIC listens for cluster-RPC connections over QUIC and dispatches
// each request frame to the supplied handlers (e.g. the cluster RPC
// server). When secret is non-empty, every stream must complete the
// session-bound auth handshake first or it is closed unserved. It
// honours the process-wide legacy compatibility setting
// (SetLegacyAuthCompat) as read at call time. It blocks until ctx is
// cancelled.
func ServeQUIC(ctx context.Context, addr, secret string, logger *slog.Logger, handlers ...StreamFrameHandler) error {
	allowLegacy := LegacyAuthCompat()
	server, err := listenQUIC(addr, secret, allowLegacy)
	if err != nil {
		return err
	}
	defer server.close()
	return serveQUICListener(ctx, server.listener, listenerAuth{secret: secret, allowLegacy: allowLegacy}, logger, handlers...)
}

// listenerAuth is the auth policy of one listener: the secret every
// stream must prove, and whether peers on the legacy ALPN are served.
type listenerAuth struct {
	secret      string
	allowLegacy bool
}

// quicServer is a bound cluster-RPC listener and the transport under it.
type quicServer struct {
	transport *quic.Transport
	udpConn   *net.UDPConn
	listener  *quic.Listener
}

// listenQUIC binds addr and starts a QUIC listener on an explicit
// Transport. The transport carries a StatelessResetKey derived from the
// cluster secret: it is what lets a restarted node answer packets for
// connection IDs it no longer knows with a reset the peer can verify
// (the pre-restart process handed out tokens from the same key), so
// peers drop dead connections immediately instead of at MaxIdleTimeout.
func listenQUIC(addr, secret string, allowLegacy bool) (*quicServer, error) {
	tlsConf, err := quicServerTLSConfig(allowLegacy)
	if err != nil {
		return nil, fmt.Errorf("quic cluster tls: %w", err)
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, fmt.Errorf("quic cluster listen: %w", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("quic cluster listen: %w", err)
	}
	tr := &quic.Transport{
		Conn:              udpConn,
		StatelessResetKey: statelessResetKey(secret),
	}
	listener, err := tr.Listen(tlsConf, quicConfig())
	if err != nil {
		_ = tr.Close()
		_ = udpConn.Close()
		return nil, fmt.Errorf("quic cluster listen: %w", err)
	}
	return &quicServer{transport: tr, udpConn: udpConn, listener: listener}, nil
}

// addr is the bound address (useful when addr was ":0").
func (s *quicServer) addr() net.Addr {
	return s.listener.Addr()
}

// close tears down the listener, its connections, and the socket.
// Connections are destroyed without a CONNECTION_CLOSE, which is what a
// crash looks like to peers; the stateless reset key covers recovery.
func (s *quicServer) close() {
	_ = s.listener.Close()
	_ = s.transport.Close()
	_ = s.udpConn.Close()
}

func serveQUICListener(ctx context.Context, listener *quic.Listener, auth listenerAuth, logger *slog.Logger, handlers ...StreamFrameHandler) error {
	defer listener.Close()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	handler := firstStreamFrameHandler(handlers)
	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) || errors.Is(err, quic.ErrServerClosed) {
				return nil
			}
			return err
		}
		go serveQUICConn(ctx, conn, auth, logger, handler)
	}
}

// connErrorCodeAuth is the application error code a connection is closed
// with when its TLS session cannot carry the auth handshake.
const connErrorCodeAuth quic.ApplicationErrorCode = 2

// serveQUICConn serves every stream of one connection. The auth proofs
// are derived ONCE here from the connection's TLS session (the exported
// keying material is per connection, so every stream on it shares the
// same proofs) and handed to each stream server.
func serveQUICConn(ctx context.Context, conn *quic.Conn, auth listenerAuth, logger *slog.Logger, handler StreamFrameHandler) {
	defer conn.CloseWithError(0, "cluster quic connection closed")

	var streamAuth *connAuth
	if auth.secret != "" {
		binding, err := sessionBindingFrom(conn.ConnectionState().TLS, auth.allowLegacy)
		if err != nil {
			if logger != nil {
				logger.Warn("cluster quic connection rejected", "component", "audit", "remote", conn.RemoteAddr().String(), "err", err)
			}
			_ = conn.CloseWithError(connErrorCodeAuth, "cluster auth binding unavailable")
			return
		}
		if binding.legacy && logger != nil {
			// One line per connection, not per stream: connections are
			// long-lived and carry dozens of pooled streams.
			logger.Warn("peer negotiated legacy fixed-token cluster auth; upgrade it and turn security.allow_legacy_cluster_auth off",
				"component", "audit", "remote", conn.RemoteAddr().String())
		}
		streamAuth = newConnAuth(auth.secret, binding)
	}

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if logger != nil && !errors.Is(err, context.Canceled) {
				logger.Debug("cluster quic accept stream", "err", err)
			}
			return
		}
		go serveStreamConn(stream, stream, streamAuth, logger, handler)
	}
}
