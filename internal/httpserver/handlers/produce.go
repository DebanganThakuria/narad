package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
)

type produceRequest struct {
	Key     string          `json:"key,omitempty"`
	Message json.RawMessage `json:"message"`
}

func (req produceRequest) Validate() error {
	if len(req.Message) == 0 {
		return errors.New("message required")
	}
	if !json.Valid(req.Message) {
		return errors.New("message is not valid JSON")
	}
	return nil
}

func (s *Set) Produce(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}

	var req produceRequest
	if !s.decodeAndValidate(w, r, &req) {
		return
	}

	offset, partition, err := s.deps.Broker.Produce(r.Context(), topicName, req.Key, []byte(req.Message))
	if err != nil {
		s.writeBrokerError(w, "produce", err)
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"offset":    offset,
		"partition": partition,
	})
}
