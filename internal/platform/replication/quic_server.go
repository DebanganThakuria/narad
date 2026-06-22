package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	"github.com/quic-go/quic-go"
)

func ServeQUIC(ctx context.Context, addr string, logs StreamLogStore, logger *slog.Logger, handlers ...StreamFrameHandler) error {
	tlsConf, err := quicServerTLSConfig()
	if err != nil {
		return fmt.Errorf("quic replication tls: %w", err)
	}
	listener, err := quic.ListenAddr(addr, tlsConf, quicConfig())
	if err != nil {
		return fmt.Errorf("quic replication listen: %w", err)
	}
	return serveQUICListener(ctx, listener, logs, logger, handlers...)
}

func serveQUICListener(ctx context.Context, listener *quic.Listener, logs StreamLogStore, logger *slog.Logger, handlers ...StreamFrameHandler) error {
	defer listener.Close()
	appendCoordinator := newReplicaAppendCoordinator(logs, logger)
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
		go serveQUICConn(ctx, conn, logs, logger, appendCoordinator, handlers...)
	}
}

func serveQUICConn(ctx context.Context, conn *quic.Conn, logs StreamLogStore, logger *slog.Logger, appendCoordinator *replicaAppendCoordinator, handlers ...StreamFrameHandler) {
	defer conn.CloseWithError(0, "replication quic connection closed")
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			if logger != nil && !errors.Is(err, context.Canceled) {
				logger.Debug("replication quic accept stream", "err", err)
			}
			return
		}
		go serveStreamConnWithCoordinator(stream, stream, logs, logger, appendCoordinator, handlers...)
	}
}
