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

type Deps struct {
	Broker         broker.Broker
	Logger         *slog.Logger
	MaxConsumeWait time.Duration
}

type Set struct {
	deps Deps
}

// New panics on missing required deps — handlers are constructed once
// at startup, so failing here surfaces wiring bugs immediately.
func New(d Deps) *Set {
	if d.Broker == nil {
		panic("handlers: Broker is required")
	}
	if d.Logger == nil {
		panic("handlers: Logger is required")
	}
	return &Set{deps: d}
}

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

func (s *Set) writeError(w http.ResponseWriter, status int, msg string) {
	s.writeJSON(w, status, map[string]string{"error": msg})
}

// decodeJSON reads a JSON body in strict mode (unknown fields
// rejected). Responds to the client on failure and returns false.
func (s *Set) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return false
	}
	return true
}
