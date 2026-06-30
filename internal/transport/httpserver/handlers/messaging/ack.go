package messaging

import (
	"net/http"
	"strings"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Ack handles POST /v1/topics/{topic}/ack?receipt_handle=...
func Ack(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}

		receiptHandle, found, err := receiptHandleFromRawQuery(r.URL.RawQuery)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid receipt_handle: "+err.Error())
			return
		}
		if !found || receiptHandle == "" {
			s.WriteError(w, http.StatusBadRequest, "receipt_handle required")
			return
		}

		h, err := consumer.DecodeHandle(receiptHandle)
		if err != nil {
			s.WriteBrokerError(w, "ack", err)
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteAck(r.Context(), w, r, topicName, h) {
				return
			}
		}

		if err := s.Deps.Broker.Ack(r.Context(), topicName, h); err != nil {
			s.WriteBrokerError(w, "ack", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func receiptHandleFromRawQuery(raw string) (string, bool, error) {
	for raw != "" {
		part := raw
		if idx := strings.IndexByte(raw, '&'); idx >= 0 {
			part = raw[:idx]
			raw = raw[idx+1:]
		} else {
			raw = ""
		}
		if part == "" {
			continue
		}

		key, value, hasValue := strings.Cut(part, "=")
		if key != "receipt_handle" {
			decodedKey, err := queryComponent(key)
			if err != nil {
				return "", false, err
			}
			if decodedKey != "receipt_handle" {
				continue
			}
		}
		if !hasValue || value == "" {
			return "", true, nil
		}
		decodedValue, err := queryComponent(value)
		if err != nil {
			return "", true, err
		}
		return decodedValue, true, nil
	}
	return "", false, nil
}
