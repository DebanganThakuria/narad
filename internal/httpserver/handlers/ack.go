package handlers

import "net/http"

type ackRequest struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (s *Set) Ack(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}

	var req ackRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}

	if err := s.deps.Broker.Ack(r.Context(), topicName, req.Partition, req.Offset); err != nil {
		s.writeBrokerError(w, "ack", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
