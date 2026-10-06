package cluster

import (
	"cmp"
	"context"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Member liveness as the views report it (MoveView.FromStatus and
// ToStatus).
const (
	nodeAlive       = "alive"
	nodeDead        = "dead"
	nodeDraining    = "draining"
	nodeNotAMember  = "not_a_member"
	targetNotMember = "target_not_member"
)

// detailBudget bounds the whole ?detail=true status fan-out, and
// detailConcurrency how many members are asked at once.
const (
	detailBudget      = 2 * time.Second
	detailConcurrency = 8
)

// MoveView is one in-flight partition move in the GET /v1/cluster/moves
// response.
type MoveView struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	From      string `json:"from"`
	To        string `json:"to"`
	// FromStatus and ToStatus are each side's liveness: alive, dead,
	// draining or not_a_member.
	FromStatus string `json:"from_status"`
	ToStatus   string `json:"to_status"`
	// Blocked says why the move cannot progress as things stand:
	// source_dead, target_dead or target_not_member. Empty while it can.
	Blocked string `json:"blocked,omitempty"`
	// Worker is the destination's own report of the move (?detail=true),
	// and WorkerError why it could not be read.
	Worker      *nodewire.MoveState `json:"worker,omitempty"`
	WorkerError string              `json:"worker_error,omitempty"`
}

// Moves handles GET /v1/cluster/moves: every partition currently mid-move
// (an assignment with a target set), with each side's liveness and why a
// move is blocked. Admin only. Served from the local metastore replica;
// no leader hop needed for a read. ?detail=true also asks each target
// node for its move worker's state.
func Moves(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		detail, err := boolQuery(r, "detail")
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		members, err := s.Deps.Metastore.ListMembers()
		if err != nil {
			s.WriteBrokerError(w, "list members", err)
			return
		}
		byID := make(map[string]metastore.Member, len(members))
		for _, m := range members {
			byID[m.ID] = m
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
					moves = append(moves, moveView(a, byID))
				}
			}
		}
		slices.SortFunc(moves, func(a, b MoveView) int {
			if c := strings.Compare(a.Topic, b.Topic); c != 0 {
				return c
			}
			return cmp.Compare(a.Partition, b.Partition)
		})
		if detail {
			addMoveWorkers(r.Context(), s, moves, byID)
		}
		s.WriteJSON(w, http.StatusOK, map[string]any{"moves": moves})
	}
}

// moveView is one move with each side's liveness and its blocked reason.
func moveView(a metastore.Assignment, byID map[string]metastore.Member) MoveView {
	v := MoveView{
		Topic: a.Topic, Partition: a.Partition, From: a.OwnerID, To: a.TargetID,
		FromStatus: liveness(byID, a.OwnerID), ToStatus: liveness(byID, a.TargetID),
	}
	switch {
	case v.FromStatus == nodeDead || v.FromStatus == nodeNotAMember:
		v.Blocked = controller.MoveBlockedSourceDead
	case v.ToStatus == nodeNotAMember:
		v.Blocked = targetNotMember
	case v.ToStatus == nodeDead:
		v.Blocked = controller.MoveBlockedTargetDead
	}
	return v
}

func liveness(byID map[string]metastore.Member, id string) string {
	m, ok := byID[id]
	switch {
	case !ok:
		return nodeNotAMember
	case m.Status == metastore.MemberDead:
		return nodeDead
	case m.Draining:
		return nodeDraining
	}
	return nodeAlive
}

// addMoveWorkers asks every move target for its status and attaches the
// worker state of each move it runs.
func addMoveWorkers(ctx context.Context, s *handlers.Set, moves []MoveView, byID map[string]metastore.Member) {
	var targets []metastore.Member
	seen := map[string]bool{}
	for _, mv := range moves {
		if m, ok := byID[mv.To]; ok && !seen[mv.To] {
			seen[mv.To] = true
			targets = append(targets, m)
		}
	}
	statuses := fetchStatuses(ctx, s, targets)
	for i := range moves {
		res, ok := statuses[moves[i].To]
		switch {
		case !ok:
			moves[i].WorkerError = "the target is not a member"
		case res.err != "":
			moves[i].WorkerError = res.err
		default:
			for _, ws := range res.status.Moves {
				if ws.Topic == moves[i].Topic && ws.Partition == moves[i].Partition {
					moves[i].Worker = &ws
					break
				}
			}
		}
	}
}

// MemberView is one member in the GET /v1/cluster/members response: its
// status, its place in Raft, how many partitions it owns and how many are
// moving off it, and why its decommission is blocked.
type MemberView struct {
	ID              string `json:"id"`
	Addr            string `json:"addr"`
	Status          string `json:"status"`
	Draining        bool   `json:"draining"`
	OwnedPartitions int    `json:"owned_partitions"`
	OutboundMoves   int    `json:"outbound_moves"`
	// Voter and Leader are the member's place in the Raft configuration.
	Voter  bool `json:"voter"`
	Leader bool `json:"leader"`
	// HeartbeatAgeSeconds is how long ago the leader last stamped its
	// heartbeat, by this node's clock.
	HeartbeatAgeSeconds int64 `json:"heartbeat_age_seconds"`
	// DecommissionBlocked lists, for a draining member, the reasons its
	// decommission cannot progress that the replicated state shows (the
	// leader's log line and narad_decommission_blocked also cover its
	// dispatch backlog).
	DecommissionBlocked []controller.Blocker `json:"decommission_blocked,omitempty"`
	// NodeStatus is the member's own report (?detail=true), and
	// StatusError why it could not be read.
	NodeStatus  *nodewire.NodeStatus `json:"node_status,omitempty"`
	StatusError string               `json:"status_error,omitempty"`
}

// Members handles GET /v1/cluster/members: every registered member with
// its live placement counts and Raft role, the operator's view of a
// rebalance or drain in progress. Admin only. Served from the local
// replica without any node RPC; ?detail=true also asks every member for
// its own status (dispatch backlog, quarantined copies, move workers),
// in parallel within a short budget.
func Members(s *handlers.Set) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.RequireAdmin(w, r); !ok {
			return
		}
		detail, err := boolQuery(r, "detail")
		if err != nil {
			s.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		views, err := controller.DecommissionViews(r.Context(), s.Deps.Metastore)
		if err != nil {
			s.WriteBrokerError(w, "read cluster state", err)
			return
		}
		now := time.Now().Unix()
		out := make([]MemberView, 0, len(views))
		var members []metastore.Member
		for _, v := range views {
			m := v.Node
			members = append(members, m)
			mv := MemberView{
				ID: m.ID, Addr: m.Addr, Status: string(m.Status), Draining: m.Draining,
				OwnedPartitions: v.Owned, OutboundMoves: v.Outbound,
				Voter: slices.Contains(v.Voters, m.ID), Leader: m.ID == v.LeaderID,
				HeartbeatAgeSeconds: max(0, now-m.LastHeartbeat),
			}
			if m.Draining {
				mv.DecommissionBlocked = controller.DecommissionBlockers(v)
			}
			out = append(out, mv)
		}
		slices.SortFunc(out, func(a, b MemberView) int { return strings.Compare(a.ID, b.ID) })
		if detail {
			statuses := fetchStatuses(r.Context(), s, members)
			for i := range out {
				res := statuses[out[i].ID]
				if res.err != "" {
					out[i].StatusError = res.err
					continue
				}
				st := res.status
				out[i].NodeStatus = &st
			}
		}
		s.WriteJSON(w, http.StatusOK, map[string]any{"members": out})
	}
}

// statusResult is one member's status, or why it could not be read.
type statusResult struct {
	status nodewire.NodeStatus
	err    string
}

// fetchStatuses asks every member for its own status, in parallel and
// within detailBudget overall.
func fetchStatuses(ctx context.Context, s *handlers.Set, members []metastore.Member) map[string]statusResult {
	out := make(map[string]statusResult, len(members))
	if s.Deps.NodeStatus == nil {
		for _, m := range members {
			out[m.ID] = statusResult{err: "node status is not available on this node"}
		}
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, detailBudget)
	defer cancel()
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, detailConcurrency)
	)
	for _, m := range members {
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				mu.Lock()
				out[m.ID] = statusResult{err: "not asked within the status budget"}
				mu.Unlock()
				return
			}
			defer func() { <-sem }()
			st, err := s.Deps.NodeStatus(ctx, m)
			res := statusResult{status: st}
			if err != nil {
				res = statusResult{err: err.Error()}
			}
			mu.Lock()
			out[m.ID] = res
			mu.Unlock()
		})
	}
	wg.Wait()
	return out
}
