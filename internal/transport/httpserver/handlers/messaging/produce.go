package messaging

import (
	"encoding/json"
	"errors"
	"io"
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

		// Read body once — may need to forward it to the partition owner.
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "read body: "+err.Error())
			return
		}

		var req produceRequest
		if err := json.Unmarshal(body, &req); err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}
		if err := req.Validate(); err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}

		if s.Deps.Router != nil {
			if s.Deps.Router.RouteProduce(r.Context(), w, r, topicName, req.Key, body) {
				return
			}
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
