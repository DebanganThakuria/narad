package metrics

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The method label must stay bounded: a client that invents methods
// (or sends one that is not UTF-8, which the label validator rejects
// with a panic) must land in one "other" series, not a series each.
func TestHTTPMiddlewareBoundsMethodLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	h := HTTPMiddleware(m)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))

	for i := range 50 {
		r := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
		r.Method = "INVENTED" + strconv.Itoa(i)
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
	r.Method = "\xa9ET"
	h.ServeHTTP(httptest.NewRecorder(), r)

	other := testutil.ToFloat64(m.HTTPRequestsTotal.WithLabelValues("unmatched", "other", "405"))
	if other != 51 {
		t.Fatalf("other-method series = %v, want 51", other)
	}
	if n := testutil.CollectAndCount(m.HTTPRequestsTotal); n != 1 {
		t.Fatalf("http_requests_total has %d series, want 1", n)
	}
}
