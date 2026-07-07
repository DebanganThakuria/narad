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
	// ackModeInvalid reports an unrecognized extend value.
	ackModeInvalid
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

		receiptHandle, found, mode, err := ackParamsFromRawQuery(r.URL.RawQuery)
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid receipt_handle: "+err.Error())
			return
		}
		if !found || receiptHandle == "" {
			s.WriteError(w, http.StatusBadRequest, "receipt_handle required")
			return
		}
		if mode == ackModeInvalid {
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

// ackParamsFromRawQuery extracts receipt_handle and the optional extend
// parameter in ONE walk of the raw query string: ack is on the hot
// path, and url.ParseQuery would allocate a map for parameters the
// handler ignores anyway. Components are unescaped only when they
// contain escape characters, and the walk does the exact same work per
// request as it did before the extend parameter existed.
//
// Accepted extend values: absent/"false"/"" → commit, "true"/"1" →
// extend, "0" → nack (visibility zero, SQS-style); anything else
// reports ackModeInvalid.
func ackParamsFromRawQuery(raw string) (handle string, handleFound bool, mode ackMode, err error) {
	extendSeen := false
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
		// Literal matches skip the escape check entirely — the common
		// case pays exactly what the old single-parameter walker did.
		if key != "receipt_handle" && key != "extend" {
			if !strings.ContainsAny(key, "%+") {
				continue
			}
			unescaped, uerr := url.QueryUnescape(key)
			if uerr != nil {
				return "", false, ackModeInvalid, uerr
			}
			key = unescaped
		}
		switch key {
		case "receipt_handle":
			if handleFound {
				continue // first occurrence wins, matching the previous walker
			}
			handleFound = true
			if !hasValue || value == "" {
				continue
			}
			if strings.ContainsAny(value, "%+") {
				unescaped, uerr := url.QueryUnescape(value)
				if uerr != nil {
					return "", true, mode, uerr
				}
				value = unescaped
			}
			handle = value
		case "extend":
			if extendSeen {
				continue // first occurrence wins
			}
			extendSeen = true
			if strings.ContainsAny(value, "%+") {
				unescaped, uerr := url.QueryUnescape(value)
				if uerr != nil {
					return handle, handleFound, ackModeInvalid, nil
				}
				value = unescaped
			}
			switch {
			case !hasValue, value == "", value == "false":
				// mode stays ackCommit
			case value == "true", value == "1":
				mode = ackExtend
			case value == "0":
				mode = ackNack
			default:
				mode = ackModeInvalid
			}
		}
	}
	return handle, handleFound, mode, nil
}
