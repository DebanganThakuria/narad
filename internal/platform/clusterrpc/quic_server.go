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
// server). When secret is non-empty, every stream must present a valid
// auth frame first or it is closed unserved. It blocks until ctx is
// cancelled.
func ServeQUIC(ctx context.Context, addr, secret string, logger *slog.Logger, handlers ...StreamFrameHandler) error {
	server, err := listenQUIC(addr, secret)
	if err != nil {
		return err
	}
	defer server.close()
	return serveQUICListener(ctx, server.listener, expectedAuthToken(secret), logger, handlers...)
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
func listenQUIC(addr, secret string) (*quicServer, error) {
	tlsConf, err := quicServerTLSConfig()
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

func serveQUICListener(ctx context.Context, listener *quic.Listener, expectedToken []byte, logger *slog.Logger, handlers ...StreamFrameHandler) error {
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
		go serveQUICConn(ctx, conn, expectedToken, logger, handler)
	}
}

func serveQUICConn(ctx context.Context, conn *quic.Conn, expectedToken []byte, logger *slog.Logger, handler StreamFrameHandler) {
	defer conn.CloseWithError(0, "cluster quic connection closed")
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if logger != nil && !errors.Is(err, context.Canceled) {
				logger.Debug("cluster quic accept stream", "err", err)
			}
			return
		}
		go serveStreamConn(stream, stream, expectedToken, logger, handler)
	}
}
