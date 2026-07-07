package topics

// Fan-out child management:
//
//	POST   /v1/topics/{parent}/children          — attach a child
//	DELETE /v1/topics/{parent}/children/{child}  — detach a child
//	GET    /v1/topics/{parent}/children          — list children + lag
//
// Attach and detach are admin-or-owner operations on the parent (same
// rule as alter/delete) and, like every metadata write, are forwarded
// to the cluster leader in multi-node mode.

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

type attachChildRequest struct {
	Child string `json:"child"`
}

// Validate implements handlers.Validator.
func (r attachChildRequest) Validate() error {
	if r.Child == "" {
		return errors.New("child is required")
	}
	return nil
}

// AttachChild handles POST /v1/topics/{parent}/children.
func AttachChild(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parent := r.PathValue("parent")
		if parent == "" {
			s.WriteError(w, http.StatusBadRequest, "parent topic required")
			return
		}
		var req attachChildRequest
		if !s.DecodeAndValidate(w, r, &req) {
			return
		}
		if !s.AuthorizeTopicManage(w, r, parent) {
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteAttachChild(r.Context(), w, r, parent, req.Child) {
				return
			}
		}
		if err := s.Deps.Broker.AttachChild(r.Context(), parent, req.Child); err != nil {
			s.WriteBrokerError(w, "attach child", err)
			return
		}
		t, err := s.Deps.Broker.GetTopic(r.Context(), parent)
		if err != nil {
			s.WriteBrokerError(w, "attach child", err)
			return
		}
		s.WriteJSON(w, http.StatusOK, t)
	}
}

// DetachChild handles DELETE /v1/topics/{parent}/children/{child}.
func DetachChild(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parent := r.PathValue("parent")
		child := r.PathValue("child")
		if parent == "" || child == "" {
			s.WriteError(w, http.StatusBadRequest, "parent and child topics required")
			return
		}
		if !s.AuthorizeTopicManage(w, r, parent) {
			return
		}
		if s.Deps.Router != nil {
			if s.Deps.Router.RouteDetachChild(r.Context(), w, r, parent, child) {
				return
			}
		}
		if err := s.Deps.Broker.DetachChild(r.Context(), parent, child); err != nil {
			s.WriteBrokerError(w, "detach child", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// childStatus is one attached child in the list-children response.
// LagMessages sums parent-partition high-watermark minus cursor over
// every reporting cursor; LagComplete is false while some cursors have
// not reported (owner unreachable or cursor not anchored yet), making
// the lag a lower bound.
type childStatus struct {
	Name        string `json:"name"`
	LagMessages int64  `json:"lag_messages"`
	LagComplete bool   `json:"lag_complete"`
}

type childrenResponse struct {
	Parent   string        `json:"parent"`
	Children []childStatus `json:"children"`
}

// ListChildren handles GET /v1/topics/{parent}/children.
func ListChildren(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		parent := r.PathValue("parent")
		if parent == "" {
			s.WriteError(w, http.StatusBadRequest, "parent topic required")
			return
		}
		t, err := s.Deps.Broker.GetTopic(r.Context(), parent)
		if err != nil {
			s.WriteBrokerError(w, "list children", err)
			return
		}

		stats, err := s.Deps.Broker.FanoutCursorStats(r.Context(), parent)
		if err != nil {
			s.WriteBrokerError(w, "list children", err)
			return
		}
		remoteComplete := true
		if s.Deps.Router != nil {
			stats, remoteComplete = s.Deps.Router.CollectFanoutCursors(r.Context(), parent, stats)
		}

		type lagAgg struct {
			lag     int64
			cursors int
		}
		byChild := map[string]*lagAgg{}
		for _, stat := range stats {
			agg := byChild[stat.Child]
			if agg == nil {
				agg = &lagAgg{}
				byChild[stat.Child] = agg
			}
			agg.cursors++
			agg.lag += max(0, stat.HighWatermark-stat.NextOffset)
		}

		resp := childrenResponse{Parent: parent, Children: []childStatus{}}
		for _, child := range t.Children {
			status := childStatus{Name: child}
			if agg := byChild[child]; agg != nil {
				status.LagMessages = agg.lag
				status.LagComplete = remoteComplete && agg.cursors == t.Partitions
			}
			resp.Children = append(resp.Children, status)
		}
		s.WriteJSON(w, http.StatusOK, resp)
	}
}
