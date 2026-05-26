package topics

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// PurgeLocal handles DELETE /internal/v1/topics/{topic}. It removes only
// local runtime and disk state for the topic; metastore deletion is handled
// by the leader's public delete flow.
func PurgeLocal(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if err := s.Deps.Broker.PurgeTopic(r.Context(), topicName); err != nil {
			s.WriteBrokerError(w, "purge topic", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
