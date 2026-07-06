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
	tlsConf, err := quicServerTLSConfig()
	if err != nil {
		return fmt.Errorf("quic cluster tls: %w", err)
	}
	listener, err := quic.ListenAddr(addr, tlsConf, quicConfig())
	if err != nil {
		return fmt.Errorf("quic cluster listen: %w", err)
	}
	return serveQUICListener(ctx, listener, secret, logger, handlers...)
}

func serveQUICListener(ctx context.Context, listener *quic.Listener, secret string, logger *slog.Logger, handlers ...StreamFrameHandler) error {
	defer listener.Close()
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go serveQUICConn(ctx, conn, secret, logger, handlers...)
	}
}

func serveQUICConn(ctx context.Context, conn *quic.Conn, secret string, logger *slog.Logger, handlers ...StreamFrameHandler) {
	defer conn.CloseWithError(0, "cluster quic connection closed")
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if logger != nil && !errors.Is(err, context.Canceled) {
				logger.Debug("cluster quic accept stream", "err", err)
			}
			return
		}
		go ServeStreamConn(stream, stream, secret, logger, handlers...)
	}
}
