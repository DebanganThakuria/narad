package controller

// Why a decommission cannot progress, as one pure verdict the leader's
// decommission pass, the members view and the decommission preflight all
// share, so an operator reads the same reason everywhere.

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// Decommission blocked reasons (Blocker.Code, the reason label of
// narad_decommission_blocked).
const (
	// BlockedBelowMinVoters: removing the voter would leave fewer than
	// MinVoters voters.
	BlockedBelowMinVoters = "below_min_voters"
	// BlockedNoHealthyMajority: the voters left alive after the removal
	// would not be a majority of the new configuration.
	BlockedNoHealthyMajority = "no_healthy_majority"
	// BlockedMoveTarget: a move still aims at the node; the leader
	// clears such moves and waits for them to go, except a move whose
	// source is dead or gone, which needs an operator.
	BlockedMoveTarget = "move_target"
	// BlockedDispatchBacklog: the node's ingress WAL still holds records
	// it accepted and has not handed to their owners, or may still take
	// some: the node does not refuse client produce yet, or is still
	// answering produce it admitted before it started to.
	BlockedDispatchBacklog = "dispatch_backlog"
	// BlockedNodeStatusUnavailable: the node's dispatch backlog cannot
	// be read (it is dead or unreachable).
	BlockedNodeStatusUnavailable = "node_status_unavailable"
	// BlockedNoReceivers: the node owns partitions and no alive node
	// that is not draining can take them.
	BlockedNoReceivers = "no_receivers"
	// BlockedOwnerDead: the node is dead and owns partitions whose data
	// is only on its disk.
	BlockedOwnerDead = "owner_dead"
	// BlockedMoveBudgetFull: the node owns partitions, nothing is moving
	// off it, and MaxInFlightMoves is used up by other moves.
	BlockedMoveBudgetFull = "move_budget_full"
	// BlockedLeaderTransfer: the node leads; leadership is handed to
	// another voter before it can be removed.
	BlockedLeaderTransfer = "leader_transfer"
)

// DecommissionReasons lists every Blocker code.
var DecommissionReasons = []string{
	BlockedBelowMinVoters, BlockedNoHealthyMajority, BlockedMoveTarget,
	BlockedDispatchBacklog, BlockedNodeStatusUnavailable, BlockedNoReceivers,
	BlockedOwnerDead, BlockedMoveBudgetFull, BlockedLeaderTransfer,
}

// stallReasons are the blocked reasons that need an operator (logged at
// error); the others clear on their own (logged at warn).
var stallReasons = map[string]bool{
	BlockedBelowMinVoters:        true,
	BlockedNoHealthyMajority:     true,
	BlockedNodeStatusUnavailable: true,
	BlockedNoReceivers:           true,
	BlockedOwnerDead:             true,
}

// Blocker is one reason a decommission cannot progress.
type Blocker struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// stall marks a reason that needs an operator although its code
	// usually clears on its own (a move_target held by a dead source).
	stall bool
}

func (b Blocker) Error() string { return b.Code + ": " + b.Message }

// needsOperator reports whether the reason needs an operator (logged at
// error) rather than clearing on its own (logged at warn).
func (b Blocker) needsOperator() bool { return b.stall || stallReasons[b.Code] }

// NodeStatus is the part of a node's own status the decommission pass
// reads before it takes the node out of Raft. The node reads Draining,
// then ProduceInFlight, then DispatchBacklog, so a zero backlog is only
// final once it reports Draining with nothing in flight: no client
// produce can reach its ingress WAL after that.
type NodeStatus struct {
	// Draining is the node's own replica's view of its drain flag; it
	// refuses client produce once it is set.
	Draining bool
	// ProduceInFlight is how many client produce requests the node
	// admitted and has not answered yet.
	ProduceInFlight int64
	// DispatchBacklog is how many records the node's ingress WAL
	// accepted and has not yet handed to their owners.
	DispatchBacklog uint64
	// QuarantinedCopies is how many partition copies the node set aside
	// instead of deleting. They do not hold the removal up (nothing can
	// discard them), but the removal says so at error: the node's volume
	// may hold the only instance of some records.
	QuarantinedCopies int
}

// ErrNodeStatusUnsupported is the error Config.NodeStatus returns for a
// node whose release predates node status (3.0.x). Decommission then
// removes it without reading its dispatch backlog, as 3.0.x did, and
// says so with a warning.
var ErrNodeStatusUnsupported = errors.New("the node's release cannot report its status")

// NodeStatusResult is one attempt to read a node's status.
type NodeStatusResult struct {
	Status NodeStatus
	Err    error
}

// DecommissionView is what DecommissionBlockers judges one node from.
type DecommissionView struct {
	// Node is the member being decommissioned.
	Node metastore.Member
	// Members is the whole member table.
	Members []metastore.Member
	// Voters is the Raft voter set.
	Voters []string
	// LeaderID is the current Raft leader's ID.
	LeaderID string
	// MinVoters is the voter floor; <= 0 means DefaultMinVoters.
	MinVoters int
	// MaxInFlightMoves is the move budget; <= 0 means
	// DefaultMaxInFlightMoves.
	MaxInFlightMoves int
	// Owned is how many partitions the node owns, Outbound how many of
	// them are moving off it, and Inbound how many moves aim at it.
	Owned, Outbound, Inbound int
	// InboundFromDeadSource is how many of the Inbound moves come from
	// a source that is dead or no longer a member. The node may hold the
	// only live copy of those partitions, so nothing clears them on its
	// own.
	InboundFromDeadSource int
	// InFlight is how many moves are in flight cluster-wide.
	InFlight int
	// Status is the node's own status; nil when it was not read (the
	// replicated-state view), and the status reasons are then skipped.
	Status *NodeStatusResult
}

// CheckVoterRemoval reports whether removing voter id from voters keeps
// Raft safe: at least minVoters voters must remain (DefaultMinVoters when
// minVoters <= 0), and the voters left with an alive member record must
// be a strict majority of them. Counting dead voters as healthy let the
// removal of a live voter from 2k+1 voters with k dead leave 2k voters
// with only k alive, a configuration that can never elect a leader. A
// node that is not a voter needs no Raft change and is always allowed.
// The error is a Blocker.
func CheckVoterRemoval(voters []string, members []metastore.Member, id string, minVoters int) error {
	if minVoters <= 0 {
		minVoters = DefaultMinVoters
	}
	if !slices.Contains(voters, id) {
		return nil
	}
	remaining := slices.DeleteFunc(slices.Clone(voters), func(v string) bool { return v == id })
	if len(remaining) < minVoters {
		return Blocker{Code: BlockedBelowMinVoters, Message: fmt.Sprintf(
			"removing it would leave %d voters, fewer than the %d a cluster keeps; add a node first", len(remaining), minVoters)}
	}
	alive := make(map[string]bool, len(members))
	for _, m := range members {
		if m.Status == metastore.MemberAlive {
			alive[m.ID] = true
		}
	}
	healthy := 0
	for _, v := range remaining {
		if alive[v] {
			healthy++
		}
	}
	if healthy*2 <= len(remaining) {
		return Blocker{Code: BlockedNoHealthyMajority, Message: fmt.Sprintf(
			"removing it would leave %d voters of which only %d are alive, not a majority; bring the dead voters back first", len(remaining), healthy)}
	}
	return nil
}

// DecommissionBlockers returns every reason the decommission of v.Node
// cannot progress right now, or none while it can (its partitions are
// moving off, or it is ready to be removed).
func DecommissionBlockers(v DecommissionView) []Blocker {
	var out []Blocker
	if v.Owned > 0 {
		out = append(out, ownershipBlockers(v)...)
	}
	switch {
	case v.InboundFromDeadSource > 0:
		out = append(out, Blocker{Code: BlockedMoveTarget, stall: true, Message: fmt.Sprintf(
			"%d moves aim at it, %d of them from a source that is dead or no longer a member; it may hold the only live copy of those partitions, so the leader does not clear them, and it waits for each copy to be force-promoted and moved off, or for the source to come back. If dropping that copy is really intended, abort the move by hand (narad cluster moves abort <topic> <partition>)", v.Inbound, v.InboundFromDeadSource)})
	case v.Inbound > 0:
		out = append(out, Blocker{Code: BlockedMoveTarget, Message: fmt.Sprintf(
			"%d moves still aim at it; the leader clears them and removes it once they are gone", v.Inbound)})
	}
	var voterErr Blocker
	if errors.As(CheckVoterRemoval(v.Voters, v.Members, v.Node.ID, v.MinVoters), &voterErr) {
		out = append(out, voterErr)
	}
	if len(out) > 0 {
		return out
	}
	// Owns nothing and safe to remove: only the removal itself is left.
	if v.Node.ID == v.LeaderID && slices.Contains(v.Voters, v.Node.ID) {
		return []Blocker{{Code: BlockedLeaderTransfer, Message: "it leads the cluster; leadership moves to another voter first"}}
	}
	if b, ok := statusBlocker(v); ok {
		return []Blocker{b}
	}
	return nil
}

// DecommissionPreflight returns the reasons a decommission of v.Node
// could never complete safely as the cluster stands, judged as if it
// were draining: too few voters, no alive majority left, no node to
// receive its partitions, or a dead node that owns partitions. The
// transient reasons (moves, backlog, budget, leadership) are left out.
func DecommissionPreflight(v DecommissionView) []Blocker {
	var out []Blocker
	var voterErr Blocker
	if errors.As(CheckVoterRemoval(v.Voters, v.Members, v.Node.ID, v.MinVoters), &voterErr) {
		out = append(out, voterErr)
	}
	if v.Owned > 0 {
		for _, b := range ownershipBlockers(v) {
			if b.Code == BlockedOwnerDead || b.Code == BlockedNoReceivers {
				out = append(out, b)
			}
		}
	}
	return out
}

// ownershipBlockers names why a node that still owns partitions cannot
// shed them, or nothing while its partitions can move off.
func ownershipBlockers(v DecommissionView) []Blocker {
	if v.Node.Status == metastore.MemberDead {
		return []Blocker{{Code: BlockedOwnerDead, Message: fmt.Sprintf(
			"it is dead and owns %d partitions whose data is only on its disk; bring it back so they can move off, or cancel the decommission", v.Owned)}}
	}
	receivers := 0
	for _, m := range v.Members {
		if m.ID != v.Node.ID && m.Status == metastore.MemberAlive && !m.Draining {
			receivers++
		}
	}
	if receivers == 0 {
		return []Blocker{{Code: BlockedNoReceivers, Message: fmt.Sprintf(
			"it owns %d partitions and no alive node that is not draining can take them; add a node or cancel a decommission", v.Owned)}}
	}
	budget := v.MaxInFlightMoves
	if budget <= 0 {
		budget = DefaultMaxInFlightMoves
	}
	if v.Outbound == 0 && v.InFlight >= budget {
		return []Blocker{{Code: BlockedMoveBudgetFull, Message: fmt.Sprintf(
			"it owns %d partitions and all %d move slots are taken by other moves; it drains once they finish", v.Owned, budget)}}
	}
	return nil
}

// statusBlocker names why the node's own status keeps it in Raft.
func statusBlocker(v DecommissionView) (Blocker, bool) {
	r := v.Status
	switch {
	case r == nil:
		return Blocker{}, false
	case errors.Is(r.Err, ErrNodeStatusUnsupported):
		return Blocker{}, false
	case r.Err != nil:
		return Blocker{Code: BlockedNodeStatusUnavailable, Message: fmt.Sprintf(
			"its dispatch backlog cannot be read (%v); records only its ingress WAL holds would be lost with it; bring it back or cancel the decommission", r.Err)}, true
	case !r.Status.Draining:
		// Its replica has not applied the drain yet, so it still takes
		// client produce: a zero backlog read now is not final.
		return Blocker{Code: BlockedDispatchBacklog, Message: "it does not refuse client produce yet (its replica has not applied the drain), so its ingress WAL may still take records; it is removed once it does and its WAL is empty"}, true
	case r.Status.ProduceInFlight > 0:
		return Blocker{Code: BlockedDispatchBacklog, Message: fmt.Sprintf(
			"it is still answering %d client produce requests it admitted before it started refusing them; it is removed once they are answered and its ingress WAL is empty", r.Status.ProduceInFlight)}, true
	case r.Status.DispatchBacklog > 0:
		return Blocker{Code: BlockedDispatchBacklog, Message: fmt.Sprintf(
			"its ingress WAL still holds %d accepted records not yet handed to their owners; it is removed once they are", r.Status.DispatchBacklog)}, true
	}
	return Blocker{}, false
}

// ClusterReader is the replica view DecommissionViews reads.
// *metastore.Store implements it, on any node.
type ClusterReader interface {
	ListMembers() ([]metastore.Member, error)
	Voters() ([]string, error)
	LeaderID() string
	ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
}

// DecommissionViews reads one DecommissionView per member from r, judged
// with the default MinVoters and MaxInFlightMoves (the ones serve runs
// the controller with) and without node status. The decommission
// preflight and the members view use it on whichever node they run.
func DecommissionViews(ctx context.Context, r ClusterReader) (map[string]DecommissionView, error) {
	members, err := r.ListMembers()
	if err != nil {
		return nil, err
	}
	voters, err := r.Voters()
	if err != nil {
		return nil, err
	}
	topics, _, err := r.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return nil, err
	}
	status := memberStatuses(members)
	owned, outbound, inbound, fromDead := map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}
	inFlight := 0
	for _, t := range topics {
		assignments, err := r.ListAssignments(t.Name)
		if err != nil {
			return nil, err
		}
		for _, a := range assignments {
			owned[a.OwnerID]++
			if a.TargetID != "" {
				outbound[a.OwnerID]++
				inbound[a.TargetID]++
				if sourceGone(a.OwnerID, status) {
					fromDead[a.TargetID]++
				}
				inFlight++
			}
		}
	}
	leaderID := r.LeaderID()
	views := make(map[string]DecommissionView, len(members))
	for _, m := range members {
		views[m.ID] = DecommissionView{
			Node: m, Members: members, Voters: voters, LeaderID: leaderID,
			MinVoters: DefaultMinVoters, MaxInFlightMoves: DefaultMaxInFlightMoves,
			Owned: owned[m.ID], Outbound: outbound[m.ID], Inbound: inbound[m.ID],
			InboundFromDeadSource: fromDead[m.ID], InFlight: inFlight,
		}
	}
	return views, nil
}

// DecommissionRefusal is the 409 body of a decommission the preflight
// refused, on the receiving node and on the leader alike: the first
// reason's message as the error, and every reason.
type DecommissionRefusal struct {
	Error   string    `json:"error"`
	Reasons []Blocker `json:"reasons"`
}

// NewDecommissionRefusal builds the refusal body for reasons (at least
// one).
func NewDecommissionRefusal(id string, reasons []Blocker) DecommissionRefusal {
	return DecommissionRefusal{
		Error:   "decommission of " + id + " refused: " + reasons[0].Message,
		Reasons: reasons,
	}
}

// memberStatuses maps each member's ID to its status.
func memberStatuses(members []metastore.Member) map[string]metastore.MemberStatus {
	out := make(map[string]metastore.MemberStatus, len(members))
	for _, m := range members {
		out[m.ID] = m.Status
	}
	return out
}

// sourceGone reports whether a move's source, the partition's owner, is
// dead or no longer a member: the move's target may then hold the only
// live copy of the partition.
func sourceGone(owner string, status map[string]metastore.MemberStatus) bool {
	st, ok := status[owner]
	return !ok || st == metastore.MemberDead
}
