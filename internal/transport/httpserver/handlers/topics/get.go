package topics

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Get handles GET /v1/topics/{topic}.
// TODO Need to route to each partition owner to get the required details
func Get(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		d, err := s.Deps.Broker.GetTopicDetails(r.Context(), topicName)
		if err != nil {
			s.WriteBrokerError(w, "get topic", err)
			return
		}
		s.WriteJSON(w, http.StatusOK, d)
	}
}
