package messaging

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// ackRequest carries an opaque receipt handle previously returned by
// Consume. Partition and offset are encoded inside the handle —
// sending them here would only invite tampering, so the request body
// is just the handle.
type ackRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
}

// Ack handles POST /v1/topics/{topic}/ack.
func Ack(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}

		var req ackRequest
		if !s.DecodeJSON(w, r, &req) {
			return
		}
		if req.ReceiptHandle == "" {
			s.WriteError(w, http.StatusBadRequest, "receipt_handle required")
			return
		}

		if err := s.Deps.Broker.Ack(r.Context(), topicName, req.ReceiptHandle); err != nil {
			s.WriteBrokerError(w, "ack", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
