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

	"github.com/debanganthakuria/narad/internal/config"
)

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
			Addr:         cfg.Addr,
			Handler:      h,
			ReadTimeout:  cfg.ReadTimeout.D(),
			WriteTimeout: cfg.WriteTimeout.D(),
			IdleTimeout:  cfg.IdleTimeout.D(),
			ErrorLog:     slog.NewLogLogger(log.Handler(), slog.LevelError),
		},
	}
}

// Run blocks until ctx is cancelled or the server fails to start. On
// shutdown it gives in-flight requests up to ShutdownGrace to finish
// before forcing the listener closed.
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.logger.Info("http listening", "addr", ln.Addr().String())

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
