package metrics

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// HTTPMiddleware returns an http middleware that records request
// counts, durations, in-flight count, and bytes per route.
//
// Routes are labeled with the matched ServeMux pattern (e.g.
// "GET /v1/topics/{topic}") rather than the literal URL path. This
// keeps cardinality bounded — there's one series per registered
// route, not one per (route, topic) combination. Requests that don't
// match any pattern (404s) are bucketed under "unmatched" so a
// 404-flood can't blow up label cardinality either.
//
// The middleware uses Go 1.22+ (*Request).Pattern, which is set by
// http.ServeMux after pattern matching but before the matched
// handler runs.
//
// Resolved children are cached per {route, method, status} (see
// httpSeriesFor), so the per-request cost after the handler returns is
// one map lookup rather than four label-hash resolutions and an Itoa.
func HTTPMiddleware(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if m == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			m.HTTPRequestsInFlight.Inc()
			defer m.HTTPRequestsInFlight.Dec()

			rec := &metricsRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			elapsed := time.Since(start).Seconds()

			series := m.httpSeriesFor(route, r.Method, rec.status)
			series.requests.Inc()
			series.duration.Observe(elapsed)
			if rec.bytes > 0 {
				series.bytesOut.Add(float64(rec.bytes))
			}
			if r.ContentLength > 0 {
				series.bytesIn.Add(float64(r.ContentLength))
			}
			if rec.status >= 500 {
				m.IncError("http", "5xx")
			}
		})
	}
}

// httpSeriesKey identifies one cached set of HTTP children.
type httpSeriesKey struct {
	route  string
	method string
	status int
}

// httpSeries is the set of children one request touches, resolved once
// per distinct label combination. The HTTP collectors are never pruned,
// so a cached child stays live for the process lifetime.
type httpSeries struct {
	requests prometheus.Counter
	duration prometheus.Observer
	bytesIn  prometheus.Counter
	bytesOut prometheus.Counter
}

// httpSeriesFor returns the cached children for the label combination,
// resolving and caching them on first sight. The cache has the same
// cardinality as the collectors themselves.
func (m *Metrics) httpSeriesFor(route, method string, status int) *httpSeries {
	key := httpSeriesKey{route: route, method: method, status: status}
	if v, ok := m.httpSeries.Load(key); ok {
		return v.(*httpSeries)
	}
	label := statusLabel(status)
	s := &httpSeries{
		requests: m.HTTPRequestsTotal.WithLabelValues(route, method, label),
		duration: m.HTTPRequestDuration.WithLabelValues(route, method, label),
		bytesIn:  m.HTTPBytesIn.WithLabelValues(route),
		bytesOut: m.HTTPBytesOut.WithLabelValues(route),
	}
	v, _ := m.httpSeries.LoadOrStore(key, s)
	return v.(*httpSeries)
}

// statusLabels pre-interns the label string for every status code a
// handler can plausibly emit, so a cache miss does not allocate for the
// common codes either.
var statusLabels = func() (labels [600]string) {
	for code := range labels {
		labels[code] = strconv.Itoa(code)
	}
	return labels
}()

// statusLabel returns the status label string for code.
func statusLabel(code int) string {
	if code >= 0 && code < len(statusLabels) {
		return statusLabels[code]
	}
	return strconv.Itoa(code)
}

// metricsRecorder mirrors httpserver.recorder. Duplicated locally
// rather than exported from httpserver to keep packages decoupled —
// this struct is trivial and rarely needs to change.
type metricsRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (r *metricsRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *metricsRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

func (r *metricsRecorder) Flush() {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *metricsRecorder) ReadFrom(src io.Reader) (int64, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	var n int64
	var err error
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(src)
	} else {
		n, err = io.Copy(r.ResponseWriter, src)
	}
	r.bytes += n
	return n, err
}

func (r *metricsRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	if !r.wroteHeader {
		r.status = http.StatusSwitchingProtocols
		r.wroteHeader = true
	}
	return hijacker.Hijack()
}
