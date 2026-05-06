package handlers

import "net/http"

// Healthz handles GET /healthz. Always 200 if the process is up.
// Liveness probe.
func (s *Set) Healthz(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
