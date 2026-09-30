package metrics

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestWP8PollerExportsIngressWALFailure pins narad_ingress_wal_failed:
// 0 while the WAL accepts produce, 1 once it latched a failure, and no
// update at all when no health source is wired.
func TestWP8PollerExportsIngressWALFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger())
	healthy := true
	p.SetIngressWALHealth(func() bool { return healthy })

	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_wal_failed", nil); got != 0 {
		t.Fatalf("narad_ingress_wal_failed while healthy = %v, want 0", got)
	}
	healthy = false
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_wal_failed", nil); got != 1 {
		t.Fatalf("narad_ingress_wal_failed after the latch = %v, want 1", got)
	}

	m.IngressWALFailed.Set(7)
	unwired := NewPoller(m, fakeSnapshotProvider{}, discardLogger())
	unwired.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_wal_failed", nil); got != 7 {
		t.Fatalf("a poller without a health source changed the gauge to %v", got)
	}
}
