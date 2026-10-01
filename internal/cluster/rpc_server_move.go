package cluster

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The partition-move ownership writes (CompleteMove, AbortMove) are
// metastore-level Raft writes, so they must run on the leader. The
// destination node proposing the flip is usually NOT the leader, so it
// forwards here; the store's Raft apply enforces leadership: a stray
// forward to a non-leader surfaces as a 503.
//
// A failed flip is not proof the flip did not happen (a deposed leader's
// entry can commit under the next one, and a reply can be lost), so the
// destination resolves every error with the leader (GetAssignment below)
// before it undoes anything. The status tells it which errors settle the
// outcome: 409 is the compare-and-set refusing (the owner or target is
// not what the destination expects), 404 a partition with no assignment
// (the topic is gone), and 503 anything else, whose outcome is unknown.

func (s *RPCServer) handleCompleteMove(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCompleteMoveRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid complete move request: "+err.Error())
	}
	if err := s.store.CompleteMove(rpcRequestContext(), req.Topic, req.Partition, req.ExpectedOwner, req.TargetID); err != nil {
		switch {
		case errors.Is(err, errs.ErrInvalidArgument):
			return errorResponse(http.StatusConflict, "complete move refused: "+err.Error())
		case errors.Is(err, errs.ErrNotFound):
			return errorResponse(http.StatusNotFound, "complete move failed: "+err.Error())
		}
		return errorResponse(http.StatusServiceUnavailable, "complete move failed: "+err.Error())
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handleAbortMove(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeAbortMoveRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid abort move request: "+err.Error())
	}
	if err := s.store.AbortMove(rpcRequestContext(), req.Topic, req.Partition, req.ExpectedTarget); err != nil {
		return errorResponse(http.StatusServiceUnavailable, "abort move failed: "+err.Error())
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

// handleGetAssignment returns the leader's view of a partition
// assignment. Callers use it for authoritative confirmation before
// destructive decisions (the stale-copy sweep, a move resolving a flip
// whose reply it did not get), so only the leader answers, and it reads
// behind a Raft barrier: a leader's FSM can lag its log (a fresh leader
// replaying, or a flip whose entry is still in the pipeline), and a stale
// read here could make a caller undo a flip that commits. A follower
// answers 421: its replica can lag the leader, and a caller that reached
// it resolved an old leader. The answer is marked leader_read (additive
// JSON; releases before it answered from any node's replica and never
// marked it).
func (s *RPCServer) handleGetAssignment(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeGetAssignmentRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid get assignment request: "+err.Error())
	}
	if !s.store.IsLeader() {
		return errorResponse(http.StatusMisdirectedRequest, "get assignment: not the metastore leader")
	}
	if err := s.store.Barrier(); err != nil {
		return errorResponse(http.StatusServiceUnavailable, "get assignment: barrier: "+err.Error())
	}
	a, err := s.store.GetAssignment(req.Topic, req.Partition)
	switch {
	case errors.Is(err, errs.ErrNotFound):
		return jsonResponse(http.StatusNotFound, assignmentMissing{Error: "assignment not found", LeaderRead: true})
	case err != nil:
		return errorResponse(http.StatusServiceUnavailable, "get assignment: "+err.Error())
	}
	return jsonResponse(http.StatusOK, assignmentReply{Assignment: a, LeaderRead: true})
}
