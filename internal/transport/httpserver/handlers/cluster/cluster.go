// Package cluster carries the operator-facing HTTP handlers for partition
// rebalance and decommission: marking a node for decommission (draining),
// and reading the in-flight moves and per-member placement. Every route
// here is admin-only, reads included: member addresses, liveness, drain
// state, and the topic names of in-flight moves are cluster topology, not
// something a principal with a single produce grant should see. The
// mutation is a metastore write, so it runs on the leader; followers
// forward via the router, exactly like user and topic writes.
package cluster

import (
	"net/http"
	"slices"
	"strconv"

	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Decommission handles POST /v1/cluster/members/{id}/decommission: mark
// a node draining. Admin only. Draining a node makes the controller shed
// its partitions onto the others and, once drained, remove it from the
// Raft voter set.
//
// The request is preflighted from this node's replica first: a
// decommission that could never complete safely (too few voters left, no
// alive majority left, no node to receive its partitions, a dead node
// that owns partitions) is refused with 409 and the reasons, and nothing
// is written. The leader checks again when the request is forwarded.
// ?dry_run=true answers 200 with the verdict and writes nothing; it is
// never forwarded.
func Decommission(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		id := r.PathValue("id")
		if id == "" {
			s.WriteError(w, http.StatusBadRequest, "member id required")
			return
		}
		dryRun, err := boolQuery(r, "dry_run")
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if !preflightDecommission(s, w, r, id, dryRun) {
			return
		}
		setDraining(s, w, r, id, false)
	}
}

// CancelDecommission handles DELETE /v1/cluster/members/{id}/decommission:
// clear a node's drain, so it keeps its partitions and receives again.
// Admin only. A cancel is never refused, and it cannot be dry-run: a
// ?dry_run=true here is answered 400 rather than cancelling for real.
func CancelDecommission(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		id := r.PathValue("id")
		if id == "" {
			s.WriteError(w, http.StatusBadRequest, "member id required")
			return
		}
		if dryRun, err := boolQuery(r, "dry_run"); err != nil || dryRun {
			s.WriteError(w, http.StatusBadRequest, "dry_run applies to a decommission, not to its cancel")
			return
		}
		setDraining(s, w, r, id, true)
	}
}

// setDraining writes the drain flag on the leader (forwarding from a
// follower) and audits it.
func setDraining(s *handlers.Set, w http.ResponseWriter, r *http.Request, id string, cancel bool) {
	if s.Deps.Router != nil && s.Deps.Router.RouteDecommissionMember(r.Context(), w, r, id, cancel) {
		return // forwarded to the leader; response already written
	}
	if err := s.Deps.Metastore.SetMemberDraining(r.Context(), id, !cancel); err != nil {
		s.WriteBrokerError(w, "decommission", err)
		return
	}
	event := "cluster.decommission"
	if cancel {
		event = "cluster.decommission.cancel"
	}
	s.Audit(r, event, id)
	w.WriteHeader(http.StatusNoContent)
}

// DryRunView is the 200 body of POST .../decommission?dry_run=true.
type DryRunView struct {
	Member            string               `json:"member"`
	WouldDecommission bool                 `json:"would_decommission"`
	Reasons           []controller.Blocker `json:"reasons"`
	Voter             bool                 `json:"voter"`
	OwnedPartitions   int                  `json:"owned_partitions"`
	InboundMoves      int                  `json:"inbound_moves"`
}

// preflightDecommission judges the decommission of id from this node's
// replica. It answers a dry run itself, refuses an unsafe decommission
// with 409, and reports whether the request may go on to the write.
func preflightDecommission(s *handlers.Set, w http.ResponseWriter, r *http.Request, id string, dryRun bool) bool {
	views, err := controller.DecommissionViews(r.Context(), s.Deps.Metastore)
	if err != nil {
		s.WriteError(w, http.StatusServiceUnavailable, "decommission preflight: "+err.Error())
		return false
	}
	v, ok := views[id]
	if !ok {
		s.WriteError(w, http.StatusNotFound, "member not found")
		return false
	}
	reasons := controller.DecommissionPreflight(v)
	if dryRun {
		s.WriteJSON(w, http.StatusOK, DryRunView{
			Member: id, WouldDecommission: len(reasons) == 0, Reasons: nonNil(reasons),
			Voter: slices.Contains(v.Voters, id), OwnedPartitions: v.Owned, InboundMoves: v.Inbound,
		})
		return false
	}
	if len(reasons) > 0 {
		s.WriteJSON(w, http.StatusConflict, controller.NewDecommissionRefusal(id, reasons))
		return false
	}
	return true
}

// boolQuery parses the boolean query parameter name; absent is false.
func boolQuery(r *http.Request, name string) (bool, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return false, nil
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false, &queryError{name: name, value: raw}
	}
	return v, nil
}

type queryError struct{ name, value string }

func (e *queryError) Error() string {
	return e.name + " must be true or false, got " + strconv.Quote(e.value)
}

// nonNil returns s, or an empty slice for nil, so JSON says [].
func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
