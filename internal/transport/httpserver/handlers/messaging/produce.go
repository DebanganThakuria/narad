// Package messaging holds the HTTP handlers for the produce/consume/
// ack data plane. Each handler is a free function that takes a
// *handlers.Set and returns an http.HandlerFunc; the router wires
// them up at startup.
package messaging

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
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

// Produce handles POST /v1/topics/{topic}/produce.
func Produce(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}

		var req produceRequest
		if !s.DecodeAndValidate(w, r, &req) {
			return
		}

		offset, partition, err := s.Deps.Broker.Produce(r.Context(), topicName, req.Key, []byte(req.Message))
		if err != nil {
			s.WriteBrokerError(w, "produce", err)
			return
		}
		s.WriteJSON(w, http.StatusOK, map[string]any{
			"offset":    offset,
			"partition": partition,
		})
	}
}
