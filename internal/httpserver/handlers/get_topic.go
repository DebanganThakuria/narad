package handlers

import "net/http"

func (s *Set) GetTopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}
	d, err := s.deps.Broker.GetTopicDetails(r.Context(), topicName)
	if err != nil {
		s.writeBrokerError(w, "get topic", err)
		return
	}
	s.writeJSON(w, http.StatusOK, d)
}
