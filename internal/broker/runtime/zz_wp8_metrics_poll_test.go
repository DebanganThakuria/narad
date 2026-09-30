package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// wp8PollOnce runs the poller for its immediate first tick.
func wp8PollOnce(p *metrics.Poller) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done
}

// wp8Gauge returns the value of the gauge name for {topic, partition}
// and whether the series exists.
func wp8Gauge(t *testing.T, reg *prometheus.Registry, name, topicName, partition string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			var tp, part string
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "topic":
					tp = l.GetValue()
				case "partition":
					part = l.GetValue()
				}
			}
			if tp == topicName && part == partition {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// wp8TopicSeriesCount counts every series labelled topic=name.
func wp8TopicSeriesCount(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "topic" && l.GetValue() == name {
					n++
				}
			}
		}
	}
	return n
}
