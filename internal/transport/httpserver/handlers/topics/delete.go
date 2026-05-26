package topics

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Delete handles DELETE /v1/topics/{topic}. The request is forwarded to
// the leader; the leader's broker remains responsible for any partition
// cleanup it orchestrates.
func Delete(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteDeleteTopic(r.Context(), w, r, topicName) {
				return
			}
		}
		if err := s.Deps.Broker.DeleteTopic(r.Context(), topicName); err != nil {
			s.WriteBrokerError(w, "delete topic", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
