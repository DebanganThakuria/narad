package handlers

import "net/http"

// Readyz handles GET /readyz. 200 if the broker is ready to serve
// traffic, 503 otherwise. Readiness probe.
func (s *Set) Readyz(w http.ResponseWriter, r *http.Request) {
	if err := s.deps.Broker.Ready(r.Context()); err != nil {
		s.writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
