// Package topics holds the HTTP handlers for the /v1/topics surface
// (create/list/get/delete/alter). Each handler is a free function
// that takes a *handlers.Set and returns an http.HandlerFunc; the
// router wires them up at startup.
package topics

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type createRequest struct {
	Name                      string `json:"name"`
	Partitions                int    `json:"partitions"`
	ReplicationFactor         int    `json:"replication_factor"`
	RetentionMs               int64  `json:"retention_ms"`
	VisibilityTimeoutMs       int64  `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64  `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64  `json:"max_acked_ahead_per_partition"`
}

// Create handles POST /v1/topics.
func Create(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, "read body: "+err.Error())
			return
		}

		var req createRequest
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			s.WriteError(w, http.StatusBadRequest, "invalid json: "+err.Error())
			return
		}

		if s.Deps.Router != nil {
			if s.Deps.Router.RouteCreateTopic(r.Context(), w, r, body) {
				return
			}
		}

		t, err := s.Deps.Broker.CreateTopic(r.Context(), topics.CreateOpts{
			Name:                      req.Name,
			Partitions:                req.Partitions,
			ReplicationFactor:         req.ReplicationFactor,
			RetentionMs:               req.RetentionMs,
			VisibilityTimeoutMs:       req.VisibilityTimeoutMs,
			MaxInFlightPerPartition:   req.MaxInFlightPerPartition,
			MaxAckedAheadPerPartition: req.MaxAckedAheadPerPartition,
		})
		if err != nil {
			s.WriteBrokerError(w, "create topic", err)
			return
		}
		s.WriteJSON(w, http.StatusCreated, t)
	}
}
