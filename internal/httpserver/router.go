package httpserver

import (
	"log/slog"
	"net/http"

	"github.com/debanganthakuria/narad/internal/httpserver/handlers"
	"github.com/debanganthakuria/narad/internal/observability/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// NewRouter wires HTTP routes to handler methods. All API routes live
// under /v1; /healthz and /readyz are unprefixed (Kubernetes
// convention). /metrics serves the Prometheus exposition.
//
// reg is the Prometheus registry used to back /metrics. m is the
// metrics struct that the HTTP middleware reads/writes. Either can
// be nil — passing nil for both disables the /metrics endpoint and
// the middleware (useful for tests that don't care about
// observability).
func NewRouter(h *handlers.Set, log *slog.Logger, m *metrics.Metrics, reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()

	// Create and list topics
	mux.HandleFunc("POST /v1/topics", h.CreateTopic)
	mux.HandleFunc("GET /v1/topics", h.ListTopics) // TODO Add pagination

	// Topic specific actions
	mux.HandleFunc("GET /v1/topics/{topic}", h.GetTopic)
	mux.HandleFunc("PATCH /v1/topics/{topic}", h.AlterTopic) // TODO Add schema update with backwards compatability, Also update retention MS
	mux.HandleFunc("DELETE /v1/topics/{topic}", h.DeleteTopic)

	// Produce, Consume and Ack
	mux.HandleFunc("POST /v1/topics/{topic}/produce", h.Produce)
	mux.HandleFunc("GET /v1/topics/{topic}/consume", h.Consume)
	mux.HandleFunc("POST /v1/topics/{topic}/ack", h.Ack)

	mux.HandleFunc("GET /healthz", h.Healthz)
	mux.HandleFunc("GET /readyz", h.Readyz)

	if reg != nil {
		mux.Handle("GET /metrics", metrics.Endpoint(reg))
	}

	stack := Chain(
		Recover(log),
		RequestID(),
		metrics.HTTPMiddleware(m),
		AccessLog(log),
	)
	return stack(mux)
}
