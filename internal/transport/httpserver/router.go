package httpserver

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/health"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
	httpreplication "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/replication"
	httptopics "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/topics"
)

// NewRouter wires HTTP routes to per-domain handler subpackages. All
// API routes live under /v1; /healthz and /readyz are unprefixed
// (Kubernetes convention). /metrics serves the Prometheus exposition.
//
// reg is the Prometheus registry used to back /metrics. m is the
// metrics struct that the HTTP middleware reads/writes. Either can
// be nil — passing nil for both disables the /metrics endpoint and
// the middleware (useful for tests that don't care about
// observability).
func NewRouter(h *handlers.Set, log *slog.Logger, m *metrics.Metrics, reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()

	// Topic CRUD
	mux.HandleFunc("POST /v1/topics", httptopics.Create(h))
	mux.HandleFunc("GET /v1/topics", httptopics.List(h))
	mux.HandleFunc("GET /v1/topics/{topic}", httptopics.Get(h))
	mux.HandleFunc("PATCH /v1/topics/{topic}", httptopics.Alter(h))
	mux.HandleFunc("DELETE /v1/topics/{topic}", httptopics.Delete(h))

	// Data plane
	mux.HandleFunc("POST /v1/topics/{topic}/produce", httpmessaging.Produce(h))
	mux.HandleFunc("GET /v1/topics/{topic}/consume", httpmessaging.Consume(h))
	mux.HandleFunc("POST /v1/topics/{topic}/ack", httpmessaging.Ack(h))

	// Internal replication and maintenance
	mux.HandleFunc("POST /internal/v1/replicate", httpreplication.Replicate(h))
	mux.HandleFunc("GET /internal/v1/replicate", httpreplication.ReadReplica(h))
	mux.HandleFunc("DELETE /internal/v1/topics/{topic}", httptopics.PurgeLocal(h))

	// Health Checks
	mux.HandleFunc("GET /healthz", health.Healthz(h))
	mux.HandleFunc("GET /readyz", health.Readyz(h))

	// Expose metrics endpoint
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
