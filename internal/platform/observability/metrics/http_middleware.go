package metrics

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

// HTTPMiddleware returns an http middleware that records request
// counts and durations per route.
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
func HTTPMiddleware(m *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if m == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			rec := &metricsRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			method := r.Method
			status := strconv.Itoa(rec.status)
			elapsed := time.Since(start).Seconds()

			m.HTTPRequestsTotal.WithLabelValues(route, method, status).Inc()
			m.HTTPRequestDuration.WithLabelValues(route, method, status).Observe(elapsed)
			if rec.status >= 500 {
				m.IncError("http", "5xx")
			}
		})
	}
}

// metricsRecorder mirrors httpserver.recorder. Duplicated locally
// rather than exported from httpserver to keep packages decoupled —
// this struct is trivial and rarely needs to change.
type metricsRecorder struct {
	http.ResponseWriter
	status      int
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
	return r.ResponseWriter.Write(b)
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
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	return io.Copy(r.ResponseWriter, src)
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
