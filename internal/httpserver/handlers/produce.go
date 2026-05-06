package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker"
)

// produceRequest is the body of POST /topics/{topic}/produce. Per the
// PRD, payloads are JSON values.
type produceRequest struct {
	Key     string          `json:"key,omitempty"`
	Message json.RawMessage `json:"message"`
}

// payload returns the message bytes after asserting it is valid JSON.
func (req produceRequest) payload() ([]byte, error) {
	if len(req.Message) == 0 {
		return nil, errors.New("message required")
	}
	if !json.Valid(req.Message) {
		return nil, errors.New("message is not valid JSON")
	}
	return []byte(req.Message), nil
}

// Produce handles POST /topics/{topic}/produce.
func (s *Set) Produce(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")
	if topicName == "" {
		s.writeError(w, http.StatusBadRequest, "topic required")
		return
	}

	var req produceRequest
	if !s.decodeJSON(w, r, &req) {
		return
	}

	payload, err := req.payload()
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	offset, partition, err := s.deps.Broker.Produce(r.Context(), topicName, req.Key, payload)
	switch {
	case errors.Is(err, broker.ErrTopicNotFound):
		s.writeError(w, http.StatusNotFound, "topic not found")
	case errors.Is(err, broker.ErrInvalidArgument):
		s.writeError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		s.deps.Logger.Error("produce", "err", err, "topic", topicName)
		s.writeError(w, http.StatusInternalServerError, "produce failed")
	default:
		s.writeJSON(w, http.StatusOK, map[string]any{
			"offset":    offset,
			"partition": partition,
		})
	}
}
