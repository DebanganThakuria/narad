package topics

import (
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Get handles GET /v1/topics/{topic}.
func Get(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		partitionQuery := r.URL.Query().Get("partition")
		d, err := s.Deps.Broker.GetTopicDetails(r.Context(), topicName)
		if err != nil {
			s.WriteBrokerError(w, "get topic", err)
			return
		}
		if partitionQuery != "" {
			partition, err := strconv.Atoi(partitionQuery)
			if err != nil || partition < 0 || partition >= len(d.Partitions) {
				s.WriteError(w, http.StatusBadRequest, "invalid partition")
				return
			}
			d.Partitions = []topic.PartitionStats{d.Partitions[partition]}
			s.WriteJSON(w, http.StatusOK, d)
			return
		}
		if s.Deps.Router != nil {
			d, err = s.Deps.Router.RouteGetTopic(r.Context(), r, topicName, d)
			if err != nil {
				s.WriteBrokerError(w, "get topic", err)
				return
			}
		}
		s.WriteJSON(w, http.StatusOK, d)
	}
}
