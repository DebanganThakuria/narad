package httpserver

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/health"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
	httptopics "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/topics"
	httpusers "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/users"
)

// NewRouter wires HTTP routes to per-domain handler subpackages. All
// API routes live under /v1; /healthz and /readyz are unprefixed
// (Kubernetes convention). /metrics serves the Prometheus exposition.
//
// reg is the Prometheus registry used to back /metrics. m is the
// metrics struct that the HTTP middleware reads/writes. Either can
// be nil — passing nil for both disables the /metrics endpoint and
// the middleware (useful for tests that don't care about
// observability). auth enables Basic authentication when non-nil;
// /healthz, /readyz, and /metrics are always exempt.
func NewRouter(h *handlers.Set, log *slog.Logger, m *metrics.Metrics, reg *prometheus.Registry, auth *security.Authenticator) http.Handler {
	mux := http.NewServeMux()

	// Topic CRUD
	mux.HandleFunc("POST /v1/topics", httptopics.Create(h))
	mux.HandleFunc("GET /v1/topics", httptopics.List(h))
	mux.HandleFunc("GET /v1/topics/{topic}", httptopics.Get(h))
	mux.HandleFunc("PATCH /v1/topics/{topic}", httptopics.Alter(h))
	mux.HandleFunc("DELETE /v1/topics/{topic}", httptopics.Delete(h))

	// Fan-out child management
	mux.HandleFunc("POST /v1/topics/{parent}/children", httptopics.AttachChild(h))
	mux.HandleFunc("GET /v1/topics/{parent}/children", httptopics.ListChildren(h))
	mux.HandleFunc("DELETE /v1/topics/{parent}/children/{child}", httptopics.DetachChild(h))

	// Data plane
	mux.HandleFunc("POST /v1/topics/{topic}/produce", httpmessaging.Produce(h))
	mux.HandleFunc("GET /v1/topics/{topic}/consume", httpmessaging.Consume(h))
	mux.HandleFunc("POST /v1/topics/{topic}/ack", httpmessaging.Ack(h))

	// User administration. Registered only when a metastore is wired in
	// (multi-node builds); the handlers write users through Raft.
	if h.Deps.Metastore != nil {
		mux.HandleFunc("POST /v1/users", httpusers.Create(h))
		mux.HandleFunc("GET /v1/users", httpusers.List(h))
		mux.HandleFunc("GET /v1/users/{username}", httpusers.Get(h))
		mux.HandleFunc("DELETE /v1/users/{username}", httpusers.Delete(h))
		mux.HandleFunc("PUT /v1/users/{username}/grants", httpusers.UpdateGrants(h))
		mux.HandleFunc("PUT /v1/users/{username}/password", httpusers.UpdatePassword(h))
	}

	// Health Checks
	mux.HandleFunc("GET /healthz", health.Healthz(h))
	mux.HandleFunc("GET /readyz", health.Readyz(h))

	// Expose metrics endpoint
	if reg != nil {
		mux.Handle("GET /metrics", metrics.Endpoint(reg))
	}

	// Metrics outermost, Recover inside it: a panicking handler is
	// converted to a 500 by Recover within the metrics measurement
	// window, so panic storms still show up in requests_total,
	// request durations, and the 5xx error counter. Auth sits inside
	// Recover so 401s are metered and an authenticator panic is a
	// clean 500.
	stack := Chain(
		metrics.HTTPMiddleware(m),
		Recover(log),
		Auth(auth, log),
	)
	return stack(mux)
}
