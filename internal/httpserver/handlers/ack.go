package handlers

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker"
)

// Ack handles POST /topics/{topic}/ack.
func (s *Set) Ack(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}

	var req struct {
		Partition int   `json:"partition"`
		Offset    int64 `json:"offset"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}

	err := s.deps.Broker.Ack(r.Context(), topicName, req.Partition, req.Offset)
	switch {
	case errors.Is(err, broker.ErrTopicNotFound):
		s.writeError(w, http.StatusNotFound, "topic not found")
	case errors.Is(err, broker.ErrInvalidArgument):
		s.writeError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		s.deps.Logger.Error("ack", "err", err, "topic", topicName)
		s.writeError(w, http.StatusInternalServerError, "ack failed")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}
