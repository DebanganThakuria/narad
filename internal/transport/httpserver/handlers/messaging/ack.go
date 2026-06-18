package messaging

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// ackRequest carries an opaque receipt handle previously returned by
// Consume. Partition and offset are encoded inside the handle.
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

		// Read body once — may need to forward to the partition owner.
		body, ok := s.ReadBody(w, r, handlers.MaxAckBodyBytes)
		if !ok {
			return
		}

		var req ackRequest
		if !s.DecodeJSONBytes(w, body, &req) {
			return
		}
		if req.ReceiptHandle == "" {
			s.WriteError(w, http.StatusBadRequest, "receipt_handle required")
			return
		}

		if s.Deps.Router != nil {
			// Decode the handle to extract the partition for routing.
			// Full verification (nonce check) happens inside the broker.
			if h, err := consumer.DecodeHandle(req.ReceiptHandle); err == nil {
				if s.Deps.Router.RouteAck(r.Context(), w, r, topicName, h.Partition, body) {
					return
				}
			}
		}

		if err := s.Deps.Broker.Ack(r.Context(), topicName, req.ReceiptHandle); err != nil {
			s.WriteBrokerError(w, "ack", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
