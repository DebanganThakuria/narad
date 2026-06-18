// Package health holds the liveness and readiness HTTP handlers.
package health

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Healthz handles GET /healthz. It stays healthy during normal operation
// and flips unhealthy once graceful shutdown has started.
func Healthz(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if s.Deps.ShutdownCtx.Err() != nil {
			s.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "shutting down"})
			return
		}
		s.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
