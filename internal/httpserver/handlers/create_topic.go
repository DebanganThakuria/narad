package handlers

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker"
)

// CreateTopic handles POST /topics.
func (s *Set) CreateTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name              string `json:"name"`
		Partitions        int    `json:"partitions"`
		ReplicationFactor int    `json:"replication_factor"`
	}
	if !s.decodeJSON(w, r, &req) {
		return
	}

	t, err := s.deps.Broker.CreateTopic(r.Context(), req.Name, req.Partitions, req.ReplicationFactor)
	switch {
	case errors.Is(err, broker.ErrTopicAlreadyExists):
		s.writeError(w, http.StatusConflict, "topic already exists")
	case errors.Is(err, broker.ErrInvalidArgument):
		s.writeError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		s.deps.Logger.Error("create topic", "err", err)
		s.writeError(w, http.StatusInternalServerError, "create topic failed")
	default:
		s.writeJSON(w, http.StatusCreated, t)
	}
}
