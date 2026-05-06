// Package handlers groups the HTTP request handlers and the small
// utilities they share (JSON encoding, error responses).
package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
)

// Deps is the constructor input for the handler set.
type Deps struct {
	Broker         broker.Broker
	Logger         *slog.Logger
	MaxConsumeWait time.Duration
}

// Set bundles the configured handlers. Methods on Set return http.Handler
// values that the router wires to specific (method, path) patterns.
type Set struct {
	deps Deps
}

// New constructs a handler Set. It panics if a required dependency is nil
// — handlers are only constructed once, at startup, so failing here keeps
// callers honest rather than letting nil deref happen on the first
// request.
func New(d Deps) *Set {
	if d.Broker == nil {
		panic("handlers: Broker is required")
	}
	if d.Logger == nil {
		panic("handlers: Logger is required")
	}
	return &Set{deps: d}
}

// writeJSON serialises v at the given status. On encode failure it logs
// and falls back to a minimal text 500.
func (s *Set) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		s.deps.Logger.Error("encode response", "err", err)
	}
}

// writeError emits {"error": msg} at the given status.
func (s *Set) writeError(w http.ResponseWriter, status int, msg string) {
	s.writeJSON(w, status, map[string]string{"error": msg})
}

// decodeJSON reads a JSON body with strict mode (unknown fields rejected).
// Returns false on failure, having already responded to the client.
func (s *Set) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}
