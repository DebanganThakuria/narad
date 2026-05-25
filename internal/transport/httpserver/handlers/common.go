// Package handlers carries the shared *Set and helper methods used
// by every HTTP handler in the per-domain subpackages
// (handlers/topics, handlers/messaging, handlers/health). The
// subpackages contain only the per-endpoint request types and
// handler functions; this file owns the dependencies and the
// JSON / error-mapping plumbing.
//
// Handler subpackage functions take a *Set and return an
// http.HandlerFunc:
//
//	func Create(s *handlers.Set) http.HandlerFunc { ... }
//
// The router wires them up at startup so the subpackages don't need
// to register routes themselves.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Router forwards requests to the partition-owning pod in a multi-node cluster.
// Nil in single-node mode — handlers skip all routing checks when it is nil.
type Router interface {
	RouteProduce(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName, key string, body []byte) bool
	RouteConsume(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, pinnedPartition *int) bool
	RouteAck(ctx context.Context, w http.ResponseWriter, r *http.Request, topicName string, partition int, body []byte) bool
	RouteCreateTopic(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) bool
}

// Deps is the bag of collaborators every handler needs.
type Deps struct {
	Broker         broker.Broker
	Logs           *runtime.Logs
	Logger         *slog.Logger
	MaxConsumeWait time.Duration
	// Router is optional. When set, requests are forwarded to the partition
	// owner instead of being handled locally on non-owner pods.
	Router Router
}

// Set is shared by every handler subpackage. The Deps field is
// exported so subpackages can reach the broker and logger; the
// helper methods below are the encoding / error-mapping primitives.
type Set struct {
	Deps Deps
}

// New panics on missing required deps — handlers are constructed
// once at startup, so failing here surfaces wiring bugs immediately.
func New(d Deps) *Set {
	if d.Broker == nil {
		panic("handlers: Broker is required")
	}
	if d.Logs == nil {
		d.Logs = runtime.NewLogs("", storage.DefaultOptions(), nil, nil)
	}
	if d.Logger == nil {
		panic("handlers: Logger is required")
	}
	return &Set{Deps: d}
}

// WriteJSON encodes v as the JSON response body with the given
// status. Nil values produce a header-only response.
func (s *Set) WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.Deps.Logger.Error("encode response", "err", err)
	}
}

// WriteError writes a `{"error": msg}` body with the given status.
func (s *Set) WriteError(w http.ResponseWriter, status int, msg string) {
	s.WriteJSON(w, status, map[string]string{"error": msg})
}

// DecodeJSON reads a JSON body in strict mode (unknown fields
// rejected). Responds to the client on failure and returns false.
func (s *Set) DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}

// Validator is implemented by request types that carry their own
// post-decode invariants.
type Validator interface {
	Validate() error
}

// DecodeAndValidate is DecodeJSON followed by dst.Validate(). Bad
// validation responds with 400 and returns false.
func (s *Set) DecodeAndValidate(w http.ResponseWriter, r *http.Request, dst Validator) bool {
	if !s.DecodeJSON(w, r, dst) {
		return false
	}
	if err := dst.Validate(); err != nil {
		s.WriteError(w, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

// WriteBrokerError maps a broker error to the right HTTP status. op
// is a short verb ("produce", "ack", …) used in log messages and
// the generic 5xx body.
//
// Receipt-handle errors are mapped to discrete codes so clients can
// distinguish "you sent garbage" (400) from "your handle was forged"
// (401) from "you took too long / it was already redelivered"
// (410). Out-of-order ack rejected by ackedAhead-cap maps to 503 —
// the head is genuinely stuck and the client should back off.
func (s *Set) WriteBrokerError(w http.ResponseWriter, op string, err error) {
	switch {
	case errors.Is(err, errs.ErrTopicNotFound):
		s.WriteError(w, http.StatusNotFound, "topic not found")
	case errors.Is(err, errs.ErrTopicAlreadyExists):
		s.WriteError(w, http.StatusConflict, "topic already exists")
	case errors.Is(err, errs.ErrHandleMalformed),
		errors.Is(err, errs.ErrHandleTopicMismatch):
		s.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrHandleStale):
		s.WriteError(w, http.StatusGone, err.Error())
	case errors.Is(err, errs.ErrAckedAheadFull):
		s.WriteError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, errs.ErrInvalidArgument),
		errors.Is(err, errs.ErrPartitionRequired):
		s.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errs.ErrNotPartitionOwner):
		s.WriteError(w, http.StatusMisdirectedRequest, err.Error())
	default:
		s.Deps.Logger.Error(op, "err", err)
		s.WriteError(w, http.StatusInternalServerError, op+" failed")
	}
}
