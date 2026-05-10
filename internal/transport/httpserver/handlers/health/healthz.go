// Package health holds the liveness and readiness HTTP handlers.
package health

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Healthz handles GET /healthz. Always 200 if the process is up.
// Liveness probe.
func Healthz(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		s.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
