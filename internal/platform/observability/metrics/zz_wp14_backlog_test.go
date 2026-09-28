package metrics

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

type zzWP14FailingSnapshots struct{}

func (zzWP14FailingSnapshots) Snapshot(context.Context) ([]TopicSnapshot, error) {
	return nil, errors.New("snapshot unavailable")
}

// narad_ingress_dispatch_backlog_records follows its source on every
// tick, also when the broker snapshot fails, since operators wait for it
// to read 0 before a rollback.
func TestZZWP14PollerExportsIngressDispatchBacklog(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger())
	backlog := uint64(42)
	p.SetIngressDispatchBacklog(func() uint64 { return backlog })

	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_dispatch_backlog_records", nil); got != 42 {
		t.Fatalf("backlog gauge = %v, want 42", got)
	}

	failing := NewPoller(m, zzWP14FailingSnapshots{}, discardLogger())
	failing.SetIngressDispatchBacklog(func() uint64 { return backlog })
	backlog = 0
	failing.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_dispatch_backlog_records", nil); got != 0 {
		t.Fatalf("backlog gauge after a failed snapshot = %v, want 0", got)
	}

	m.IngressDispatchBacklog.Set(7)
	unwired := NewPoller(m, fakeSnapshotProvider{}, discardLogger())
	unwired.tick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_dispatch_backlog_records", nil); got != 7 {
		t.Fatalf("a poller without a backlog source changed the gauge to %v", got)
	}
}
