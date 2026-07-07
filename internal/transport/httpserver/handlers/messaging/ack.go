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
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// ackMode selects what an ack request does with the reservation.
type ackMode int

const (
	// ackCommit acknowledges the message (the default).
	ackCommit ackMode = iota
	// ackExtend renews the visibility window to a full fresh window
	// (extend=true): the slow consumer keeps its lease and acks later
	// with the same handle.
	ackExtend
	// ackNack releases the reservation immediately (extend=0): the
	// message becomes redeliverable right away.
	ackNack
)

// Ack handles POST /v1/topics/{topic}/ack?receipt_handle=...
//
// The optional extend parameter turns the request into a lease
// operation on the same handle: extend=true renews the visibility
// window (heartbeat for slow consumers), extend=0 releases it for
// immediate redelivery (negative ack). Both share ack's validation: a
// lapsed or superseded handle gets 410 Gone.
func Ack(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		topicName := r.PathValue("topic")
		if topicName == "" {
			s.WriteError(w, http.StatusBadRequest, "topic required")
			return
		}
		// Ack is covered by the consume grant: a consumer that cannot
		// ack what it consumed would only grow redelivery loops.
		if !s.Authorize(w, r, user.ActionConsume, topicName) {
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
		mode, ok := ackModeFromRawQuery(r.URL.RawQuery)
		if !ok {
			s.WriteError(w, http.StatusBadRequest, `invalid extend: want "true" (renew lease) or "0" (release for redelivery)`)
			return
		}

		h, err := consumer.DecodeHandle(receiptHandle)
		if err != nil {
			s.WriteBrokerError(w, "ack", err)
			return
		}
		if s.Deps.Router != nil {
			forwarded := false
			switch mode {
			case ackExtend:
				forwarded = s.Deps.Router.RouteExtendAck(r.Context(), w, r, topicName, h)
			case ackNack:
				forwarded = s.Deps.Router.RouteNack(r.Context(), w, r, topicName, h)
			default:
				forwarded = s.Deps.Router.RouteAck(r.Context(), w, r, topicName, h)
			}
			if forwarded {
				return
			}
		}

		var op string
		switch mode {
		case ackExtend:
			op, err = "extend ack", s.Deps.Broker.ExtendAck(r.Context(), topicName, h)
		case ackNack:
			op, err = "nack", s.Deps.Broker.Nack(r.Context(), topicName, h)
		default:
			op, err = "ack", s.Deps.Broker.Ack(r.Context(), topicName, h)
		}
		if err != nil {
			s.WriteBrokerError(w, op, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// ackModeFromRawQuery reads the optional extend parameter. ok=false
// means the value is unrecognized. Accepted: absent/"false" → commit,
// "true"/"1" → extend, "0" → nack (visibility zero, SQS-style).
func ackModeFromRawQuery(raw string) (ackMode, bool) {
	value, found, err := paramFromRawQuery(raw, "extend")
	if err != nil {
		return ackCommit, false
	}
	if !found {
		return ackCommit, true
	}
	switch value {
	case "true", "1":
		return ackExtend, true
	case "0":
		return ackNack, true
	case "false", "":
		return ackCommit, true
	default:
		return ackCommit, false
	}
}

// receiptHandleFromRawQuery extracts receipt_handle by walking the raw
// query string directly: ack is on the hot path, and url.ParseQuery
// would allocate a map for parameters the handler ignores anyway.
func receiptHandleFromRawQuery(raw string) (string, bool, error) {
	return paramFromRawQuery(raw, "receipt_handle")
}

// paramFromRawQuery extracts one parameter by walking the raw query
// string directly, avoiding url.ParseQuery's map allocation for
// parameters the handler ignores anyway. Components are unescaped only
// when they contain escape characters.
func paramFromRawQuery(raw, name string) (string, bool, error) {
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
		if key != name {
			if !strings.ContainsAny(key, "%+") {
				continue
			}
			unescaped, err := url.QueryUnescape(key)
			if err != nil {
				return "", false, err
			}
			if unescaped != name {
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
