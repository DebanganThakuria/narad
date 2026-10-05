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
	"sort"
	"strconv"

	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Decommission handles POST /v1/cluster/members/{id}/decommission (mark a
// node draining) and DELETE (cancel a drain). Admin only. Draining a node
// makes the controller shed its partitions onto the others and, once
// drained, remove it from the Raft voter set.
//
// A POST is preflighted from this node's replica first: a decommission
// that could never complete safely (too few voters left, no alive
// majority left, no node to receive its partitions, a dead node that
// owns partitions) is refused with 409 and the reasons, and nothing is
// written. The leader checks again when the request is forwarded.
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
		cancel := r.Method == http.MethodDelete
		dryRun, err := boolQuery(r, "dry_run")
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if cancel && dryRun {
			s.WriteError(w, http.StatusBadRequest, "dry_run applies to a decommission, not to its cancel")
			return
		}
		if !cancel && !preflightDecommission(s, w, r, id, dryRun) {
			return
		}

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
}

// MoveView is one in-flight partition move in the GET /v1/cluster/moves
// response.
type MoveView struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	From      string `json:"from"`
	To        string `json:"to"`
}

// Moves handles GET /v1/cluster/moves: every partition currently mid-move
// (an assignment with a target set). Admin only. Served from the local
// metastore replica; no leader hop needed for a read.
func Moves(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		topics, _, err := s.Deps.Metastore.ListTopics(r.Context(), metastore.ListOptions{})
		if err != nil {
			s.WriteBrokerError(w, "list topics", err)
			return
		}
		moves := []MoveView{}
		for _, t := range topics {
			assignments, err := s.Deps.Metastore.ListAssignments(t.Name)
			if err != nil {
				s.WriteBrokerError(w, "list assignments", err)
				return
			}
			for _, a := range assignments {
				if a.TargetID != "" {
					moves = append(moves, MoveView{Topic: a.Topic, Partition: a.Partition, From: a.OwnerID, To: a.TargetID})
				}
			}
		}
		sort.Slice(moves, func(i, j int) bool {
			if moves[i].Topic != moves[j].Topic {
				return moves[i].Topic < moves[j].Topic
			}
			return moves[i].Partition < moves[j].Partition
		})
		s.WriteJSON(w, http.StatusOK, map[string]any{"moves": moves})
	}
}

// MemberView is one member in the GET /v1/cluster/members response: its
// status plus how many partitions it owns and how many are moving off it.
type MemberView struct {
	ID              string `json:"id"`
	Addr            string `json:"addr"`
	Status          string `json:"status"`
	Draining        bool   `json:"draining"`
	OwnedPartitions int    `json:"owned_partitions"`
	OutboundMoves   int    `json:"outbound_moves"`
}

// Members handles GET /v1/cluster/members: every registered member with its
// live placement counts, the operator's view of a rebalance or drain in
// progress. Admin only. Served from the local replica.
func Members(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		members, err := s.Deps.Metastore.ListMembers()
		if err != nil {
			s.WriteBrokerError(w, "list members", err)
			return
		}
		owned := map[string]int{}
		outbound := map[string]int{}
		topics, _, err := s.Deps.Metastore.ListTopics(r.Context(), metastore.ListOptions{})
		if err != nil {
			s.WriteBrokerError(w, "list topics", err)
			return
		}
		for _, t := range topics {
			assignments, err := s.Deps.Metastore.ListAssignments(t.Name)
			if err != nil {
				s.WriteBrokerError(w, "list assignments", err)
				return
			}
			for _, a := range assignments {
				owned[a.OwnerID]++
				if a.TargetID != "" {
					outbound[a.OwnerID]++
				}
			}
		}
		views := make([]MemberView, 0, len(members))
		for _, m := range members {
			views = append(views, MemberView{
				ID: m.ID, Addr: m.Addr, Status: string(m.Status), Draining: m.Draining,
				OwnedPartitions: owned[m.ID], OutboundMoves: outbound[m.ID],
			})
		}
		sort.Slice(views, func(i, j int) bool { return views[i].ID < views[j].ID })
		s.WriteJSON(w, http.StatusOK, map[string]any{"members": views})
	}
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
