package httpserver

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpcluster "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/cluster"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/health"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
	httptopics "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/topics"
	httpusers "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/users"
)

// RouterOptions tunes the parts of the router that are deployment
// policy rather than routing.
type RouterOptions struct {
	// MetricsOnAPI serves /metrics on this router (the API port). False
	// when the exposition is served on its own listener instead.
	MetricsOnAPI bool
	// MetricsRequireAuth makes /metrics on the API port require the same
	// Basic credentials as the API. Ignored when auth is nil.
	MetricsRequireAuth bool
	// ConsumeInFlightPerIdentity caps concurrent consume requests per
	// authenticated user (or per client IP without auth); 0 disables.
	ConsumeInFlightPerIdentity int
}

// DefaultRouterOptions is the secure default: metrics on the API port
// behind credentials, no per-identity consume cap.
func DefaultRouterOptions() RouterOptions {
	return RouterOptions{MetricsOnAPI: true, MetricsRequireAuth: true}
}

// NewRouter is NewRouterWithOptions with DefaultRouterOptions.
func NewRouter(h *handlers.Set, log *slog.Logger, m *metrics.Metrics, reg *prometheus.Registry, auth *security.Authenticator) http.Handler {
	return NewRouterWithOptions(h, log, m, reg, auth, DefaultRouterOptions())
}

// NewRouterWithOptions wires HTTP routes to per-domain handler
// subpackages. All API routes live under /v1; /healthz and /readyz are
// unprefixed (Kubernetes convention). /metrics serves the Prometheus
// exposition when opts.MetricsOnAPI is set.
//
// reg is the Prometheus registry used to back /metrics. m is the
// metrics struct that the HTTP middleware reads/writes. Either can
// be nil; passing nil for both disables the /metrics endpoint and
// the middleware (useful for tests that don't care about
// observability). auth enables Basic authentication when non-nil;
// /healthz and /readyz are always exempt, /metrics only when
// opts.MetricsRequireAuth is false.
func NewRouterWithOptions(h *handlers.Set, log *slog.Logger, m *metrics.Metrics, reg *prometheus.Registry, auth *security.Authenticator, opts RouterOptions) http.Handler {
	mux := http.NewServeMux()
	consumeLimit := newInFlightLimiter(opts.ConsumeInFlightPerIdentity)

	// Topic CRUD
	mux.HandleFunc("POST /v1/topics", httptopics.Create(h))
	mux.HandleFunc("GET /v1/topics", httptopics.List(h))
	mux.HandleFunc("GET /v1/topics/{topic}", httptopics.Get(h))
	mux.HandleFunc("PATCH /v1/topics/{topic}", httptopics.Alter(h))
	mux.HandleFunc("DELETE /v1/topics/{topic}", httptopics.Delete(h))
	mux.HandleFunc("GET /v1/topics/{topic}/schema", httptopics.SchemaHistory(h))

	// Fan-out child management
	mux.HandleFunc("POST /v1/topics/{parent}/children", httptopics.AttachChild(h))
	mux.HandleFunc("GET /v1/topics/{parent}/children", httptopics.ListChildren(h))
	mux.HandleFunc("DELETE /v1/topics/{parent}/children/{child}", httptopics.DetachChild(h))

	// Data plane
	mux.HandleFunc("POST /v1/topics/{topic}/produce", httpmessaging.Produce(h))
	mux.Handle("GET /v1/topics/{topic}/consume", consumeLimit.wrap(httpmessaging.Consume(h)))
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

		// Cluster operations: partition rebalance/decommission control and
		// placement visibility.
		mux.HandleFunc("POST /v1/cluster/members/{id}/decommission", httpcluster.Decommission(h))
		mux.HandleFunc("DELETE /v1/cluster/members/{id}/decommission", httpcluster.Decommission(h))
		mux.HandleFunc("GET /v1/cluster/moves", httpcluster.Moves(h))
		mux.HandleFunc("GET /v1/cluster/members", httpcluster.Members(h))
	}

	// Health Checks
	mux.HandleFunc("GET /healthz", health.Healthz(h))
	mux.HandleFunc("GET /readyz", health.Readyz(h))

	// Expose metrics endpoint
	if reg != nil && opts.MetricsOnAPI {
		mux.Handle("GET /metrics", metrics.Endpoint(reg))
	}

	// Metrics outermost, Recover inside it: a panicking handler is
	// converted to a 500 by Recover within the metrics measurement
	// window, so panic storms still show up in requests_total,
	// request durations, and the 5xx error counter. Auth sits inside
	// Recover so 401s are metered and an authenticator panic is a
	// clean 500. The cross-site guard sits inside Auth: it only matters
	// for requests that carry credentials, and an anonymous request is
	// answered 401 first.
	stack := Chain(
		metrics.HTTPMiddleware(m),
		Recover(log),
		AuthExempting(auth, log, authExemptPaths(!opts.MetricsRequireAuth)),
		RequireAPIContentType(),
	)
	return stack(mux)
}
