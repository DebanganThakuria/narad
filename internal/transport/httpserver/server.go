// Package httpserver hosts the HTTP transport: server lifecycle, routing,
// and middleware. The actual request handling lives in the handlers
// subpackage.
package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/netutil"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// maxReadHeaderTimeout bounds how long a client may take to send its
// request headers. ReadTimeout (10 s by default) used to cover it; a
// slowloris-style client can hold a connection that long per attempt,
// so headers get a tighter, separate budget.
const maxReadHeaderTimeout = 5 * time.Second

// Server owns an *http.Server, runs it, and handles graceful shutdown
// when the context is cancelled.
type Server struct {
	cfg    config.HTTPConfig
	srv    *http.Server
	logger *slog.Logger
}

// New constructs a Server. The handler should already include the
// middleware stack (NewRouter takes care of that).
func New(cfg config.HTTPConfig, h http.Handler, log *slog.Logger) *Server {
	return &Server{
		cfg:    cfg,
		logger: log,
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           h,
			ReadTimeout:       cfg.ReadTimeout.D(),
			ReadHeaderTimeout: readHeaderTimeout(cfg.ReadTimeout.D()),
			WriteTimeout:      cfg.WriteTimeout.D(),
			IdleTimeout:       cfg.IdleTimeout.D(),
			MaxHeaderBytes:    cfg.MaxHeaderBytes,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
		},
	}
}

// readHeaderTimeout is maxReadHeaderTimeout, or the read timeout when
// that is shorter.
func readHeaderTimeout(readTimeout time.Duration) time.Duration {
	if readTimeout > 0 && readTimeout < maxReadHeaderTimeout {
		return readTimeout
	}
	return maxReadHeaderTimeout
}

// limitListener caps the connections accepted from ln at maxConns;
// further connections wait in the kernel's accept backlog until one
// closes. maxConns <= 0 returns ln unchanged.
func limitListener(ln net.Listener, maxConns int) net.Listener {
	if maxConns <= 0 {
		return ln
	}
	return netutil.LimitListener(ln, maxConns)
}

// Run blocks until ctx is cancelled or the server fails to start. On
// shutdown it gives in-flight requests up to ShutdownGrace to finish
// before forcing the listener closed.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.logger.Info("http listening", "addr", ln.Addr().String(), "max_connections", s.cfg.MaxConnections)
	ln = limitListener(ln, s.cfg.MaxConnections)

	serveErr := make(chan error, 1)
	go func() {
		err := s.srv.Serve(ln)
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownGrace.D())
		defer cancel()

		s.logger.Info("http shutting down", "grace", s.cfg.ShutdownGrace)
		if err := s.srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-serveErr

	case err := <-serveErr:
		return err
	}
}
