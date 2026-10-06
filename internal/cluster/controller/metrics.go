package controller

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics is the controller's leader-only Prometheus surface. A nil
// *metrics (no Registerer) records nothing; every method is nil-safe.
// Every series is reset when a leader term begins and ends, so only the
// current leader reports a verdict.
type metrics struct {
	deadMarkingRefused prometheus.Gauge
	decomBlocked       *prometheus.GaugeVec
	colocated          prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	if reg == nil {
		return nil
	}
	m := &metrics{
		deadMarkingRefused: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "narad_dead_marking_refused",
			Help: "1 while the leader refuses a dead verdict that would leave most Raft voters marked dead (its node RPC plane is the likelier fault), else 0. Leader only.",
		}),
		decomBlocked: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "narad_decommission_blocked",
			Help: "1 for each reason a draining node's decommission cannot progress. Leader only.",
		}, []string{"node", "reason"}),
		colocated: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "narad_colocated_child_partitions",
			Help: "Fan-out child partitions owned by the same node as their parent's same-index partition. Leader only.",
		}),
	}
	m.deadMarkingRefused = registerOrReuse(reg, m.deadMarkingRefused)
	m.decomBlocked = registerOrReuse(reg, m.decomBlocked)
	m.colocated = registerOrReuse(reg, m.colocated)
	return m
}

// registerOrReuse registers c on reg, or returns the collector already
// registered under the same name (a second controller on one registry,
// as tests build).
func registerOrReuse[C prometheus.Collector](reg prometheus.Registerer, c C) C {
	if err := reg.Register(c); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(C); ok {
				return existing
			}
		}
		panic(err)
	}
	return c
}

// reset zeroes every leader-only series.
func (m *metrics) reset() {
	if m == nil {
		return
	}
	m.deadMarkingRefused.Set(0)
	m.decomBlocked.Reset()
	m.colocated.Set(0)
}

func (m *metrics) setDeadMarkingRefused(on bool) {
	if m == nil {
		return
	}
	v := 0.0
	if on {
		v = 1
	}
	m.deadMarkingRefused.Set(v)
}

func (m *metrics) setDecomBlocked(node, reason string) {
	if m == nil {
		return
	}
	m.decomBlocked.WithLabelValues(node, reason).Set(1)
}

func (m *metrics) clearDecomBlocked(node, reason string) {
	if m == nil {
		return
	}
	m.decomBlocked.DeleteLabelValues(node, reason)
}

func (m *metrics) setColocated(n int) {
	if m == nil {
		return
	}
	m.colocated.Set(float64(n))
}
