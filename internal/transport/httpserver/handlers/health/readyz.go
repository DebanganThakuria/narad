package health

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Readyz handles GET /readyz. 200 if the broker is ready to serve
// traffic, 503 otherwise. Readiness probe.
func Readyz(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := s.Deps.Broker.Ready(r.Context()); err != nil {
			s.WriteError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		s.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}
