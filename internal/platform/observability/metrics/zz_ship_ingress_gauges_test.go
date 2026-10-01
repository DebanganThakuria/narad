package metrics

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// narad_ingress_wal_failed follows its source on every tick, also when
// the broker snapshot fails: it is the gauge operators alert on, and a
// failing disk can latch the ingress WAL and break the metastore read
// behind the snapshot at once. It used to be set only after a snapshot
// that succeeded, so a latch during a run of failed snapshots read 0.
func TestShipIngressWALFailedFollowsSourceWhenSnapshotFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, zzWP14FailingSnapshots{}, discardLogger())
	healthy := true
	p.SetIngressWALHealth(func() bool { return healthy })

	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_wal_failed", nil); got != 0 {
		t.Fatalf("narad_ingress_wal_failed while healthy, snapshot failing = %v, want 0", got)
	}
	healthy = false
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_wal_failed", nil); got != 1 {
		t.Fatalf("narad_ingress_wal_failed after the latch, snapshot failing = %v, want 1", got)
	}
}
