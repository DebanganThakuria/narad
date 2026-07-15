// Package topics holds the HTTP handlers for the /v1/topics surface
// (create/list/get/delete/alter). Each handler is a free function
// that takes a *handlers.Set and returns an http.HandlerFunc; the
// router wires them up at startup.
package topics

import (
	"encoding/json"
	"net/http"

	"github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type createRequest struct {
	Name                      string          `json:"name"`
	Partitions                int             `json:"partitions"`
	RetentionMs               int64           `json:"retention_ms"`
	VisibilityTimeoutMs       int64           `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64           `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64           `json:"max_acked_ahead_per_partition"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
	// Parent creates the topic as a fan-out child of an existing topic
	// in one call, which is what gets the child anti-affine partition
	// placement (the replica pattern). FanoutDelayMs > 0 additionally
	// makes it a delay child. Partitions == 0 with Parent set inherits
	// the parent's partition count.
	Parent        string `json:"parent,omitempty"`
	FanoutDelayMs int64  `json:"fanout_delay_ms,omitempty"`
	// Owner is server-assigned: whatever the client sends is discarded
	// and replaced with the authenticated identity before the request
	// is applied or forwarded.
	Owner string `json:"owner,omitempty"`
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
		if !s.Authorize(w, r, user.ActionCreate, req.Name) {
			return
		}
		// Create-as-child is also an attach: it links the new topic to
		// req.Parent, so it needs the same manage right on the parent as
		// POST /v1/topics/{parent}/children.
		if req.Parent != "" && !s.AuthorizeTopicManage(w, r, req.Parent) {
			return
		}

		// The creator becomes the topic owner. Never trust a
		// client-supplied owner; re-marshal so the leader-forwarded
		// body carries the identity this node authenticated.
		req.Owner = ""
		if id, ok := handlers.Identity(r); ok {
			req.Owner = id.Username
		}
		body, err := json.Marshal(req)
		if err != nil {
			s.WriteError(w, http.StatusInternalServerError, "encode create request")
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
			RetentionMs:               req.RetentionMs,
			VisibilityTimeoutMs:       req.VisibilityTimeoutMs,
			MaxInFlightPerPartition:   req.MaxInFlightPerPartition,
			MaxAckedAheadPerPartition: req.MaxAckedAheadPerPartition,
			Schema:                    req.Schema,
			Parent:                    req.Parent,
			FanoutDelayMs:             req.FanoutDelayMs,
			Owner:                     req.Owner,
		})
		if err != nil {
			s.WriteBrokerError(w, "create topic", err)
			return
		}
		s.WriteJSON(w, http.StatusCreated, t)
	}
}
