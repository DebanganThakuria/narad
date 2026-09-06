package topics

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// SchemaHistory handles GET /v1/topics/{topic}/schema: every schema
// version of the topic in ascending order plus the current version
// number. It follows the topic read rule (any grant on the topic,
// ownership, or admin): producers need the schema to build valid
// messages and consumers to interpret them. The history is read from
// this node's metastore replica, the same source its produce path
// validates against.
func SchemaHistory(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		if !s.AuthorizeTopicRead(w, r, topicName) {
			return
		}
		history, err := s.Deps.Broker.TopicSchemaHistory(r.Context(), topicName)
		if err != nil {
			s.WriteBrokerError(w, "get topic schema", err)
			return
		}
		s.WriteJSON(w, http.StatusOK, history)
	}
}
