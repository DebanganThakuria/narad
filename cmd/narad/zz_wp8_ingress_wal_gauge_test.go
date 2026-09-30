package main

import (
	"context"
	"io"
	"log/slog"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

type wp8NoSnapshots struct{}

func (wp8NoSnapshots) Snapshot(context.Context) ([]metrics.TopicSnapshot, error) { return nil, nil }

// TestWP8IngressWALFailureReachesTheGauge wires a real ingress manager
// to the metrics poller the way runServe does and latches its WAL with
// an injected sync failure: the gauge an operator alerts on turns 1 and
// stays 1 after the disk recovers.
func TestWP8IngressWALFailureReachesTheGauge(t *testing.T) {
	mgr, err := ingress.OpenManager(t.TempDir(), ingressWALOptions(config.Default().Storage))
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	poller := metrics.NewPoller(m, wp8NoSnapshots{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	poller.SetIngressWALHealth(mgr.Healthy)
	pollOnce := func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { poller.Run(ctx); close(done) }()
		time.Sleep(50 * time.Millisecond)
		cancel()
		<-done
	}
	gauge := func() float64 {
		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, mf := range mfs {
			if mf.GetName() == "narad_ingress_wal_failed" {
				return mf.GetMetric()[0].GetGauge().GetValue()
			}
		}
		t.Fatal("narad_ingress_wal_failed not exported")
		return 0
	}

	ctx := context.Background()
	if _, err := mgr.AcceptProduce(ctx, "orders", "k", 0, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	pollOnce()
	if got := gauge(); got != 0 {
		t.Fatalf("gauge while the WAL is healthy = %v, want 0", got)
	}

	inj := faulttest.New(t)
	inj.FailNth(syncfile.OpSyncData, ".wal", 1, syscall.EIO)
	if _, err := mgr.AcceptProduce(ctx, "orders", "k", 0, []byte("fails")); err == nil {
		t.Fatal("accept during the injected sync failure succeeded")
	}
	pollOnce()
	if got := gauge(); got != 1 {
		t.Fatalf("gauge after the WAL latched = %v, want 1", got)
	}
}
