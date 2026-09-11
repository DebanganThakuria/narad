package main

import (
	"context"
	"errors"
	"fmt"
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
//
// health, when non-nil, serves GET /healthz and GET /readyz and is
// mounted on the metrics listener too. The API listener keeps serving
// both paths, but a probe there queues behind client traffic: a
// saturated, healthy broker answered its 1s liveness probe late and
// was killed by kubelet, then could not pass its startup probe while
// clients kept hammering the API port. The chart points the probes at
// the metrics port.
//
// fail is called when the listener that carries the probes cannot bind:
// with kubelet probing that port, a silent bind failure would leave the
// API serving while every probe fails, and the pod would restart-loop
// for a reason no log line explained.
func startDiagnosticsServers(ctx context.Context, wg *sync.WaitGroup, cfg config.HTTPConfig, reg *prometheus.Registry, health http.Handler, fail func(error), log *slog.Logger) {
	healthAddr := ""
	if health != nil {
		healthAddr = strings.TrimSpace(cfg.MetricsAddr)
	}
	for addr, mux := range diagnosticsMuxes(cfg, reg, health) {
		var onListenErr func(error)
		if addr == healthAddr && healthAddr != "" {
			onListenErr = fail
		}
		serveDiagnostics(ctx, wg, addr, mux, onListenErr, log)
	}
}

// diagnosticsMuxes builds one mux per diagnostics address.
func diagnosticsMuxes(cfg config.HTTPConfig, reg *prometheus.Registry, health http.Handler) map[string]*http.ServeMux {
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
	if addr := strings.TrimSpace(cfg.MetricsAddr); addr != "" {
		mux := muxFor(addr)
		if reg != nil {
			mux.Handle("GET /metrics", metrics.Endpoint(reg))
		}
		if health != nil {
			mux.Handle("GET /healthz", health)
			mux.Handle("GET /readyz", health)
		}
	}
	return muxes
}

// serveDiagnostics runs one diagnostics listener until ctx is cancelled.
// onListenErr, when set, is told about a bind failure; otherwise the
// failure is only logged.
func serveDiagnostics(ctx context.Context, wg *sync.WaitGroup, addr string, mux *http.ServeMux, onListenErr func(error), log *slog.Logger) {
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
	}

	wg.Go(func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error("diagnostics listen", "addr", addr, "err", err)
			if onListenErr != nil {
				onListenErr(fmt.Errorf("diagnostics listener %s (health probes): %w", addr, err))
			}
			return
		}
		log.Info("diagnostics listening (pprof, metrics and/or health probes, unauthenticated; keep it cluster-internal)", "addr", ln.Addr().String())

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
