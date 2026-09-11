package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// TestDiagnosticsListenerServesHealth pins that the metrics listener
// answers the probes the chart points at it, with the handler it was
// given, and that a pprof-only listener does not.
func TestDiagnosticsListenerServesHealth(t *testing.T) {
	health := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	cfg := config.HTTPConfig{MetricsAddr: "127.0.0.1:9100", PprofAddr: "127.0.0.1:6060"}
	muxes := diagnosticsMuxes(cfg, prometheus.NewRegistry(), health)

	for _, path := range []string{"/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		muxes["127.0.0.1:9100"].ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusTeapot {
			t.Fatalf("metrics listener GET %s = %d, want the health handler's %d", path, rec.Code, http.StatusTeapot)
		}
		rec = httptest.NewRecorder()
		muxes["127.0.0.1:6060"].ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("pprof listener GET %s = %d, want 404", path, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	muxes["127.0.0.1:9100"].ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics listener GET /metrics = %d, want 200", rec.Code)
	}

	// No metrics listener: nothing to mount health on, and no panic.
	if m := diagnosticsMuxes(config.HTTPConfig{PprofAddr: "127.0.0.1:6060"}, nil, health); len(m) != 1 {
		t.Fatalf("muxes without a metrics address = %d, want 1 (pprof only)", len(m))
	}
}

// TestDiagnosticsListenerBindFailureIsReported pins that a listener
// which cannot bind tells its caller: with the probes on that port, a
// silent failure would leave the API up and the pod restart-looping.
func TestDiagnosticsListenerBindFailureIsReported(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()

	var wg sync.WaitGroup
	got := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDiagnostics(ctx, &wg, taken.Addr().String(), http.NewServeMux(), func(err error) { got <- err }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	wg.Wait()
	select {
	case err := <-got:
		if err == nil || !strings.Contains(err.Error(), "health probes") {
			t.Fatalf("bind failure reported as %v, want an error naming the health probes", err)
		}
	default:
		t.Fatal("bind failure was not reported")
	}
}
