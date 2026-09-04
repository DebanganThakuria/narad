package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// readCounter returns the counter value for name matching the labels,
// or ok=false when no such series is exposed.
func readCounter(t *testing.T, reg *prometheus.Registry, name string, matchLabels map[string]string) (float64, bool) {
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
			if c := met.GetCounter(); c != nil {
				return c.GetValue(), true
			}
		}
	}
	return 0, false
}

// TestPartitionCountersCachedAndLiveAfterPrune verifies the cache
// hands back the same set until the topic is pruned, and that the
// first call after a prune binds a live child that appears in the
// exposition (a stale cached child would increment invisibly).
func TestPartitionCountersCachedAndLiveAfterPrune(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	labels := map[string]string{"topic": "orders", "partition": "3"}

	pc := m.PartitionCounters("orders", 3)
	if pc == nil {
		t.Fatal("PartitionCounters returned nil")
	}
	if again := m.PartitionCounters("orders", 3); again != pc {
		t.Fatal("second PartitionCounters call did not return the cached set")
	}
	if other := m.PartitionCounters("orders", 4); other == pc {
		t.Fatal("different partition shares a counter set")
	}

	pc.MessagesProduced.Inc()
	pc.BytesProduced.Add(10)
	pc.MessagesConsumed.Inc()
	pc.BytesConsumed.Add(10)
	pc.CorruptSkipped.Inc()
	if got, ok := readCounter(t, reg, "narad_messages_produced_total", labels); !ok || got != 1 {
		t.Fatalf("messages_produced_total = %v, %v; want 1, true", got, ok)
	}

	m.pruneTopicSeries("orders")
	if _, ok := readCounter(t, reg, "narad_messages_produced_total", labels); ok {
		t.Fatal("series survived pruneTopicSeries")
	}

	// The old set is detached: incrementing it must not resurrect the
	// series. The next lookup must bind a fresh, live child.
	pc.MessagesProduced.Inc()
	if _, ok := readCounter(t, reg, "narad_messages_produced_total", labels); ok {
		t.Fatal("detached child reappeared in the exposition")
	}
	fresh := m.PartitionCounters("orders", 3)
	if fresh == pc {
		t.Fatal("PartitionCounters returned the pruned set after pruneTopicSeries")
	}
	fresh.MessagesProduced.Inc()
	fresh.BytesConsumed.Add(7)
	if got, ok := readCounter(t, reg, "narad_messages_produced_total", labels); !ok || got != 1 {
		t.Fatalf("messages_produced_total after re-bind = %v, %v; want 1, true (live child)", got, ok)
	}
	if got, ok := readCounter(t, reg, "narad_bytes_consumed_total", labels); !ok || got != 7 {
		t.Fatalf("bytes_consumed_total after re-bind = %v, %v; want 7, true", got, ok)
	}

	// Pruning one topic must not evict another's cache entry.
	keep := m.PartitionCounters("payments", 0)
	m.pruneTopicSeries("orders")
	if m.PartitionCounters("payments", 0) != keep {
		t.Fatal("pruning orders evicted the payments cache entry")
	}

	var nilMetrics *Metrics
	if nilMetrics.PartitionCounters("x", 0) != nil {
		t.Fatal("nil Metrics must yield nil PartitionCounters")
	}
}

// TestStorageRecorderPreResolvedChildrenSurvivePrune verifies the
// recorder's pre-bound children observe into live series, and that
// after the topic's series are pruned the next observation re-binds
// rather than landing on the detached children.
func TestStorageRecorderPreResolvedChildrenSurvivePrune(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	labels := map[string]string{"topic": "orders", "partition": "0"}

	rec := m.StorageRecorder("orders", 0)
	if rec == nil {
		t.Fatal("StorageRecorder returned nil")
	}
	rec.ObserveFlush(time.Millisecond, 100)
	rec.ObserveFsync(time.Millisecond)
	rec.ObserveHighWatermarkPersist(time.Millisecond, "ok")
	rec.ObserveHighWatermarkPersist(time.Millisecond, "error")
	rec.ObserveHighWatermarkPersist(time.Millisecond, "other")
	rec.IncRetentionDeletion("age", 5, 1)
	rec.IncRetentionDeletion("bytes", 6, 2)
	rec.IncRetentionDeletion("manual", 7, 3)
	rec.ObserveRetentionRun(time.Millisecond)

	if got, ok := readCounter(t, reg, "narad_storage_flush_bytes_total", labels); !ok || got != 100 {
		t.Fatalf("flush_bytes_total = %v, %v; want 100, true", got, ok)
	}
	for _, reason := range []string{"age", "bytes", "manual"} {
		if _, ok := readCounter(t, reg, "narad_storage_retention_bytes_deleted_total", map[string]string{"topic": "orders", "partition": "0", "reason": reason}); !ok {
			t.Fatalf("retention_bytes_deleted_total reason=%s missing", reason)
		}
	}
	for _, outcome := range []string{"ok", "error", "other"} {
		if !hasSeries(t, reg, "narad_storage_high_watermark_persist_duration_seconds", "outcome", outcome) {
			t.Fatalf("high_watermark_persist outcome=%s missing", outcome)
		}
	}

	m.pruneTopicSeries("orders")
	if _, ok := readCounter(t, reg, "narad_storage_flush_bytes_total", labels); ok {
		t.Fatal("flush_bytes_total survived pruneTopicSeries")
	}

	rec.ObserveFlush(time.Millisecond, 25)
	if got, ok := readCounter(t, reg, "narad_storage_flush_bytes_total", labels); !ok || got != 25 {
		t.Fatalf("flush_bytes_total after prune = %v, %v; want 25, true (re-bound child)", got, ok)
	}

	var nilMetrics *Metrics
	if nilMetrics.StorageRecorder("x", 0) != nil {
		t.Fatal("nil Metrics must yield a nil recorder")
	}
}

// TestHTTPSeriesCacheMatchesDirectResolution checks the middleware's
// cached children are the same objects the vectors hand out for the
// labels, and that the status label is the decimal code.
func TestHTTPSeriesCacheMatchesDirectResolution(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/topics/{topic}/produce", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	stack := HTTPMiddleware(m)(mux)
	for range 3 {
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", nil)
		req.ContentLength = 12
		stack.ServeHTTP(httptest.NewRecorder(), req)
	}
	req := httptest.NewRequest(http.MethodGet, "/nowhere", nil)
	stack.ServeHTTP(httptest.NewRecorder(), req)

	const route = "POST /v1/topics/{topic}/produce"
	if got, ok := readCounter(t, reg, "narad_http_requests_total", map[string]string{"route": route, "method": "POST", "status": "202"}); !ok || got != 3 {
		t.Fatalf("requests_total = %v, %v; want 3, true", got, ok)
	}
	if got, ok := readCounter(t, reg, "narad_http_request_bytes_in_total", map[string]string{"route": route}); !ok || got != 36 {
		t.Fatalf("request_bytes_in_total = %v, %v; want 36, true", got, ok)
	}
	if got, ok := readCounter(t, reg, "narad_http_requests_total", map[string]string{"route": "unmatched", "method": "GET", "status": "404"}); !ok || got != 1 {
		t.Fatalf("unmatched requests_total = %v, %v; want 1, true", got, ok)
	}

	cached := m.httpSeriesFor(route, http.MethodPost, http.StatusAccepted)
	if cached.requests != m.HTTPRequestsTotal.WithLabelValues(route, "POST", "202") {
		t.Fatal("cached requests child differs from the vector's child")
	}
	if cached != m.httpSeriesFor(route, http.MethodPost, http.StatusAccepted) {
		t.Fatal("httpSeriesFor did not return the cached set")
	}
	if statusLabel(599) != "599" || statusLabel(1000) != "1000" || statusLabel(-1) != "-1" {
		t.Fatal("statusLabel mismatch")
	}
}
