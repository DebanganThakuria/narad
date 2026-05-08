package httpserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"
)

// pprofShutdownGrace bounds how long we wait for in-flight profile
// requests to finish before forcing the listener closed. Profiles can
// legitimately run for tens of seconds, but at process-exit time we
// don't want to block on them — five seconds is a generous floor.
const pprofShutdownGrace = 5 * time.Second

// RunPProf serves Go's net/http/pprof endpoints on a dedicated
// listener until ctx is cancelled or the server fails to start.
//
// pprof handlers are registered on a private mux rather than via the
// `_ "net/http/pprof"` side-effect import — that would leak the
// endpoints onto http.DefaultServeMux and risk exposure on whatever
// other server happens to use it.
//
// The server intentionally sets only ReadHeaderTimeout: a global
// WriteTimeout would cut off /debug/pprof/profile?seconds=N for any
// non-trivial N, which is exactly the request operators care about.
// The expectation is that addr binds to loopback (e.g. 127.0.0.1:6060)
// so unauthenticated profile access is acceptable.
func RunPProf(ctx context.Context, addr string, log *slog.Logger) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Info("pprof listening", "addr", ln.Addr().String())
	if !isLoopback(addr) {
		log.Warn("pprof bound to non-loopback address; pprof exposes goroutine and heap details",
			"addr", addr)
	}

	serveErr := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), pprofShutdownGrace)
		defer cancel()

		log.Info("pprof shutting down", "grace", pprofShutdownGrace)
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-serveErr

	case err := <-serveErr:
		return err
	}
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.TrimSpace(host)
	// Empty host (":6060") means bind to all interfaces, not loopback.
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
