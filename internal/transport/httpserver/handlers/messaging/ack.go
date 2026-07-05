// Package messaging holds the data-plane HTTP handlers — produce,
// consume, and ack under /v1/topics/{topic}. Each handler is a free
// function that takes a *handlers.Set and returns an
// http.HandlerFunc; the router wires them up at startup.
package messaging

import (
	"net/http"
	"net/url"
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

// receiptHandleFromRawQuery extracts receipt_handle by walking the raw
// query string directly: ack is on the hot path, and url.ParseQuery
// would allocate a map for parameters the handler ignores anyway.
// Components are unescaped only when they contain escape characters.
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
			if !strings.ContainsAny(key, "%+") {
				continue
			}
			unescaped, err := url.QueryUnescape(key)
			if err != nil {
				return "", false, err
			}
			if unescaped != "receipt_handle" {
				continue
			}
		}
		if !hasValue || value == "" {
			return "", true, nil
		}
		if strings.ContainsAny(value, "%+") {
			unescaped, err := url.QueryUnescape(value)
			if err != nil {
				return "", true, err
			}
			return unescaped, true, nil
		}
		return value, true, nil
	}
	return "", false, nil
}
