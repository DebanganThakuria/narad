package handlers

import "net/http"

func (s *Set) DeleteTopic(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}
	if err := s.deps.Broker.DeleteTopic(r.Context(), topicName); err != nil {
		s.writeBrokerError(w, "delete topic", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
