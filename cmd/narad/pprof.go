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

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// startDiagnosticsServers serves the pprof endpoints (http.pprof_addr)
// and, when http.metrics_addr is set, the Prometheus exposition, each
// on its own listener off the public API port. The two may name the
// same address, in which case one listener serves both. Neither
// listener authenticates: they are meant to stay loopback or
// cluster-internal (a NetworkPolicy, a port the ingress never routes).
// The servers run on wg and shut down when ctx is cancelled.
func startDiagnosticsServers(ctx context.Context, wg *sync.WaitGroup, cfg config.HTTPConfig, reg *prometheus.Registry, log *slog.Logger) {
	muxes := map[string]*http.ServeMux{}
	muxFor := func(addr string) *http.ServeMux {
		if mux := muxes[addr]; mux != nil {
			return mux
		}
		mux := http.NewServeMux()
		muxes[addr] = mux
		return mux
	}
	if addr := strings.TrimSpace(cfg.PprofAddr); addr != "" {
		mux := muxFor(addr)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	if addr := strings.TrimSpace(cfg.MetricsAddr); addr != "" && reg != nil {
		muxFor(addr).Handle("GET /metrics", metrics.Endpoint(reg))
	}
	for addr, mux := range muxes {
		serveDiagnostics(ctx, wg, addr, mux, log)
	}
}

// serveDiagnostics runs one diagnostics listener until ctx is cancelled.
func serveDiagnostics(ctx context.Context, wg *sync.WaitGroup, addr string, mux *http.ServeMux, log *slog.Logger) {
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
	}

	wg.Go(func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("diagnostics listen", "addr", addr, "err", err)
			return
		}
		log.Info("diagnostics listening (pprof and/or metrics, unauthenticated; keep it cluster-internal)", "addr", ln.Addr().String())

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
				log.Error("diagnostics shutdown", "addr", addr, "err", err)
				_ = srv.Close()
			}
			if err := <-serveErr; err != nil {
				log.Error("diagnostics serve", "addr", addr, "err", err)
			}
		case err := <-serveErr:
			if err != nil {
				log.Error("diagnostics serve", "addr", addr, "err", err)
			}
		}
	})
}
