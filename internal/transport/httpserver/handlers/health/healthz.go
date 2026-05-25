// Package health holds the liveness and readiness HTTP handlers.
package health

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Healthz handles GET /healthz. Always 200 if the process is up.
// Liveness probe.
// TODO Do we need a mechanism to mark it unhealthy if something goes wrong or during graceful shutdown. So, that in the meantime k8s can bring up another pod?
func Healthz(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		s.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
