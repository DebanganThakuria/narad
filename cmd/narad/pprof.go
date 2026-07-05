package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"
)

// startPprofServer serves the pprof endpoints on their own listener when
// addr is non-empty, keeping profiling off the public API port. The server
// runs on wg and shuts down when ctx is cancelled.
func startPprofServer(ctx context.Context, wg *sync.WaitGroup, addr string, log *slog.Logger) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
	}

	wg.Go(func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("pprof listen", "addr", addr, "err", err)
			return
		}
		log.Info("pprof listening", "addr", ln.Addr().String())

		serveErr := make(chan error, 1)
		go func() {
			err := srv.Serve(ln)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			serveErr <- err
		}()

		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				log.Error("pprof shutdown", "err", err)
				_ = srv.Close()
			}
			if err := <-serveErr; err != nil {
				log.Error("pprof serve", "err", err)
			}
		case err := <-serveErr:
			if err != nil {
				log.Error("pprof serve", "err", err)
			}
		}
	})
}
