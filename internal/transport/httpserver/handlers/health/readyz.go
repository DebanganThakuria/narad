package health

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// ClusterReadiness is the live cluster-side readiness check /readyz
// consults on every probe; *metastore.Store implements it. nil means
// routable now. It is deliberately not a latch: a node that loses its
// Raft leader, stops hearing from it, or is removed from the voter set
// must stop receiving traffic, because it would otherwise keep serving a
// frozen replica (stale users and grants, stale topics) for as long as
// the process lives.
type ClusterReadiness interface {
	ClusterReady() error
}

// DegradedReadiness lists conditions /readyz reports while it still
// answers 200; *metastore.Store implements it (an expired Raft TLS
// certificate). They are listed rather than failing readiness because
// they hit every node at once (one certificate usually serves them
// all), and a readiness failure on every pod empties the Services.
type DegradedReadiness interface {
	ReadinessDegraded() []string
}

// readyBody is the 200 answer. Degraded is omitted when empty, so a
// healthy node answers {"status":"ready"} as before.
type readyBody struct {
	Status   string   `json:"status"`
	Degraded []string `json:"degraded,omitempty"`
}

// Readyz handles GET /readyz. 200 only while BOTH hold: the broker has
// finished startup (reconcile done, MarkReady called) AND the metastore
// reports the node in live contact with a leader with a caught-up view
// of partition ownership. Either failing answers 503 with the reason.
// A 200 lists any degraded conditions (DegradedReadiness). Readiness
// probe.
func Readyz(s *handlers.Set) http.HandlerFunc {
	var cluster ClusterReadiness
	var degraded DegradedReadiness
	if s.Deps.Metastore != nil {
		cluster = s.Deps.Metastore
		degraded = s.Deps.Metastore
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.Deps.Broker.Ready(r.Context()); err != nil {
			s.WriteError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if cluster != nil {
			if err := cluster.ClusterReady(); err != nil {
				s.WriteError(w, http.StatusServiceUnavailable, err.Error())
				return
			}
		}
		body := readyBody{Status: "ready"}
		if degraded != nil {
			body.Degraded = degraded.ReadinessDegraded()
		}
		s.WriteJSON(w, http.StatusOK, body)
	}
}
