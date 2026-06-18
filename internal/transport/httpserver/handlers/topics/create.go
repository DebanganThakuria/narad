// Package topics holds the HTTP handlers for the /v1/topics surface
// (create/list/get/delete/alter). Each handler is a free function
// that takes a *handlers.Set and returns an http.HandlerFunc; the
// router wires them up at startup.
package topics

import (
	"encoding/json"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type createRequest struct {
	Name                      string          `json:"name"`
	Partitions                int             `json:"partitions"`
	ReplicationFactor         int             `json:"replication_factor"`
	RetentionMs               int64           `json:"retention_ms"`
	VisibilityTimeoutMs       int64           `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64           `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64           `json:"max_acked_ahead_per_partition"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

// Create handles POST /v1/topics.
func Create(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, ok := s.ReadBody(w, r, handlers.MaxJSONBodyBytes)
		if !ok {
			return
		}

		var req createRequest
		if !s.DecodeJSONBytes(w, body, &req) {
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
			Schema:                    req.Schema,
		})
		if err != nil {
			s.WriteBrokerError(w, "create topic", err)
			return
		}
		s.WriteJSON(w, http.StatusCreated, t)
	}
}
