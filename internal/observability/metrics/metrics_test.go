package metrics

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
)

// TestNewRegistersAllCollectors guards against accidentally adding a
// collector to the Metrics struct without registering it. If a future
// field skips MustRegister, the collector silently disappears from
// /metrics — this test catches that by scraping the registry and
// asserting every namespaced family is present.
func TestNewRegistersAllCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	if m == nil {
		t.Fatal("New returned nil")
	}

	// Force a single observation on every collector so it shows up in
	// the gather output (Prometheus omits collectors that have never
	// been touched).
	m.HTTPRequestsTotal.WithLabelValues("/x", "GET", "200").Inc()
	m.HTTPRequestDuration.WithLabelValues("/x", "GET").Observe(0.01)
	m.HTTPBytesIn.WithLabelValues("/x").Add(1)
	m.HTTPBytesOut.WithLabelValues("/x").Add(1)
	m.HTTPRequestsInFlight.Set(0)
	m.MessagesProducedTotal.WithLabelValues("t", "0").Inc()
	m.MessagesConsumedTotal.WithLabelValues("t", "0").Inc()
	m.BytesProducedTotal.WithLabelValues("t", "0").Add(1)
	m.BytesConsumedTotal.WithLabelValues("t", "0").Add(1)
	m.ProduceRejectionsTotal.WithLabelValues("t", "schema").Inc()
	m.ConsumeWaitSeconds.WithLabelValues("t", "hit").Observe(0.01)
	m.ConsumeEmptyTotal.WithLabelValues("t").Inc()
	m.TopicsTotal.Set(1)
	m.PartitionsTotal.Set(1)
	m.TopicBytes.WithLabelValues("t").Set(1)
	m.PartitionSizeBytes.WithLabelValues("t", "0").Set(1)
	m.Segments.WithLabelValues("t", "0").Set(1)
	m.ConsumerLagMessages.WithLabelValues("t", "0").Set(0)
	m.ConsumerDroppedMessages.WithLabelValues("t", "0").Set(0)
	m.OldestUnconsumedAgeSeconds.WithLabelValues("t", "0").Set(0)
	m.FlushDurationSeconds.WithLabelValues("t", "0").Observe(0.001)
	m.FlushBytesTotal.WithLabelValues("t", "0").Add(1)
	m.FsyncDurationSeconds.WithLabelValues("t", "0").Observe(0.001)
	m.SegmentsRolledTotal.WithLabelValues("t", "0").Inc()
	m.RetentionDeletionsTotal.WithLabelValues("t", "0", "age").Inc()
	m.RetentionBytesDeleted.WithLabelValues("t", "0", "age").Add(1)
	m.RetentionMessagesDeleted.WithLabelValues("t", "0", "age").Add(1)
	m.RetentionRunSeconds.WithLabelValues("t", "0").Observe(0.001)
	m.SegmentsScannedAtBoot.WithLabelValues("t", "0").Inc()
	m.IncError("test", "kind")
	m.BootDurationSeconds.Set(0.1)

	want := []string{
		"narad_http_requests_total",
		"narad_http_request_duration_seconds",
		"narad_http_request_bytes_in_total",
		"narad_http_response_bytes_out_total",
		"narad_http_requests_in_flight",
		"narad_messages_produced_total",
		"narad_messages_consumed_total",
		"narad_bytes_produced_total",
		"narad_bytes_consumed_total",
		"narad_produce_rejections_total",
		"narad_consume_wait_seconds",
		"narad_consume_empty_total",
		"narad_topics_total",
		"narad_partitions_total",
		"narad_topic_bytes",
		"narad_partition_size_bytes",
		"narad_segments",
		"narad_consumer_lag_messages",
		"narad_consumer_dropped_messages",
		"narad_oldest_unconsumed_message_age_seconds",
		"narad_storage_flush_duration_seconds",
		"narad_storage_flush_bytes_total",
		"narad_storage_fsync_duration_seconds",
		"narad_storage_segments_rolled_total",
		"narad_storage_retention_deletions_total",
		"narad_storage_retention_bytes_deleted_total",
		"narad_storage_retention_messages_deleted_total",
		"narad_storage_retention_run_duration_seconds",
		"narad_storage_segments_scanned_at_boot_total",
		"narad_errors_total",
		"narad_boot_duration_seconds",
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	got := make(map[string]struct{}, len(mfs))
	for _, mf := range mfs {
		got[mf.GetName()] = struct{}{}
	}

	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("metric %q not registered", name)
		}
	}
}

// TestHTTPMiddlewareLabelsRoutePattern verifies the middleware uses
// the matched ServeMux pattern as the route label, not the literal
// URL path. Without this, every distinct topic name would produce its
// own series — a cardinality leak.
func TestHTTPMiddlewareLabelsRoutePattern(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/topics/{topic}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	stack := HTTPMiddleware(m)(mux)

	for _, topic := range []string{"alpha", "beta", "gamma"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/topics/"+topic, nil)
		rec := httptest.NewRecorder()
		stack.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("topic %q: got status %d", topic, rec.Code)
		}
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	var family *dto.MetricFamily
	for _, mf := range mfs {
		if mf.GetName() == "narad_http_requests_total" {
			family = mf
			break
		}
	}
	if family == nil {
		t.Fatal("narad_http_requests_total missing")
	}

	// Three requests should collapse into one series labeled with the
	// route template, not three series labeled with the literal paths.
	if got := len(family.GetMetric()); got != 1 {
		t.Errorf("series count: got %d, want 1 (cardinality leak: middleware used literal path?)", got)
	}
	for _, met := range family.GetMetric() {
		labels := labelsToMap(met.GetLabel())
		if labels["route"] != "GET /v1/topics/{topic}" {
			t.Errorf("route label: got %q, want template", labels["route"])
		}
	}
}

// TestPollerUpdatesGauges feeds a fake snapshot provider into the
// poller and verifies one tick populates the lag gauges as expected.
func TestPollerUpdatesGauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	produced := time.Now().Add(-30 * time.Second)
	provider := fakeSnapshotProvider{
		[]TopicSnapshot{{
			Topic: "orders",
			Partitions: []PartitionSnapshot{{
				Partition:          0,
				LogStartOffset:     0,
				LogEndOffset:       100,
				CommittedOffset:    40,
				SegmentCount:       2,
				SizeBytes:          1024,
				OldestUnconsumedAt: produced,
			}},
		}},
	}

	p := NewPoller(m, provider, discardLogger())
	p.tick(context.Background())

	if got := readGauge(t, reg, "narad_topics_total", nil); got != 1 {
		t.Errorf("topics_total: got %v, want 1", got)
	}
	if got := readGauge(t, reg, "narad_partitions_total", nil); got != 1 {
		t.Errorf("partitions_total: got %v, want 1", got)
	}
	if got := readGauge(t, reg, "narad_consumer_lag_messages", map[string]string{"topic": "orders", "partition": "0"}); got != 60 {
		t.Errorf("consumer_lag_messages: got %v, want 60", got)
	}
	if got := readGauge(t, reg, "narad_oldest_unconsumed_message_age_seconds", map[string]string{"topic": "orders", "partition": "0"}); got < 25 || got > 35 {
		t.Errorf("oldest_unconsumed_age_seconds: got %v, want ~30", got)
	}
}

// TestPollerPrunesDeletedTopics verifies that when a topic disappears
// between ticks, its gauge series are removed from the registry.
// Without pruning, /metrics would leak series indefinitely after
// DeleteTopic.
func TestPollerPrunesDeletedTopics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	provider := &mutableSnapshotProvider{
		snaps: []TopicSnapshot{{
			Topic: "doomed",
			Partitions: []PartitionSnapshot{{
				Partition:    0,
				LogEndOffset: 10,
			}},
		}},
	}

	p := NewPoller(m, provider, discardLogger())
	p.tick(context.Background())

	if !hasSeries(t, reg, "narad_consumer_lag_messages", "topic", "doomed") {
		t.Fatal("first tick: lag series for doomed missing")
	}

	provider.snaps = nil // simulate DeleteTopic
	p.tick(context.Background())

	if hasSeries(t, reg, "narad_consumer_lag_messages", "topic", "doomed") {
		t.Error("second tick: lag series for doomed still present after deletion")
	}
}

// --- helpers ---

type fakeSnapshotProvider struct {
	snaps []TopicSnapshot
}

func (f fakeSnapshotProvider) Snapshot(_ context.Context) ([]TopicSnapshot, error) {
	return f.snaps, nil
}

type mutableSnapshotProvider struct {
	snaps []TopicSnapshot
}

func (m *mutableSnapshotProvider) Snapshot(_ context.Context) ([]TopicSnapshot, error) {
	return m.snaps, nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func labelsToMap(lps []*dto.LabelPair) map[string]string {
	out := make(map[string]string, len(lps))
	for _, lp := range lps {
		out[lp.GetName()] = lp.GetValue()
	}
	return out
}

// readGauge looks up the named metric in reg and returns the gauge
// value for the (sub)set of labels supplied. matchLabels can be nil
// for unlabeled metrics. Fails the test if not exactly one metric
// matches.
func readGauge(t *testing.T, reg *prometheus.Registry, name string, matchLabels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, met := range mf.GetMetric() {
			if !labelsMatch(met.GetLabel(), matchLabels) {
				continue
			}
			if g := met.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	t.Fatalf("metric %q with labels %v not found", name, matchLabels)
	return 0
}

func hasSeries(t *testing.T, reg *prometheus.Registry, name string, lvKey, lvVal string) bool {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, met := range mf.GetMetric() {
			for _, lp := range met.GetLabel() {
				if lp.GetName() == lvKey && lp.GetValue() == lvVal {
					return true
				}
			}
		}
	}
	return false
}

func labelsMatch(have []*dto.LabelPair, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	got := labelsToMap(have)
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}
