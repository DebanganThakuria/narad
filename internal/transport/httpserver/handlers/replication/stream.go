package replication

import (
	"net/http"

	platformreplication "github.com/debanganthakuria/narad/internal/platform/replication"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func Stream(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		platformreplication.ServeStream(w, r, s.Deps.Logs, s.Deps.Logger)
	}
}
