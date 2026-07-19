// Package cluster carries the operator-facing HTTP handlers for partition
// rebalance and decommission: marking a node for decommission (draining),
// and reading the in-flight moves and per-member placement. The mutation is
// a metastore write, so it runs on the leader — followers forward via the
// router, exactly like user and topic writes.
package cluster

import (
	"net/http"
	"sort"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Decommission handles POST /v1/cluster/members/{id}/decommission (mark a
// node draining) and DELETE (cancel a drain). Admin only. Draining a node
// makes the controller shed its partitions onto the others and, once
// drained, remove it from the Raft voter set.
func Decommission(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := handlers.Identity(r)
		if ok && !caller.IsAdmin() {
			s.WriteError(w, http.StatusForbidden, "admin privileges required")
			return
		}
		id := r.PathValue("id")
		if id == "" {
			s.WriteError(w, http.StatusBadRequest, "member id required")
			return
		}
		cancel := r.Method == http.MethodDelete

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
// (an assignment with a target set). Served from the local metastore replica
// — no leader hop needed for a read.
func Moves(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
// live placement counts — the operator's view of a rebalance or drain in
// progress. Served from the local replica.
func Members(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
