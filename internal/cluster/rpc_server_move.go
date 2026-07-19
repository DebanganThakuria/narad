package cluster

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The partition-move ownership writes (CompleteMove, AbortMove) are
// metastore-level Raft writes, so they must run on the leader. The
// destination node proposing the flip is usually NOT the leader, so it
// forwards here; the store's Raft apply enforces leadership — a stray
// forward to a non-leader surfaces as a 503, which the destination's move
// worker treats as "flip not done, retry".

func (s *RPCServer) handleCompleteMove(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeCompleteMoveRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid complete move request: "+err.Error())
	}
	if err := s.store.CompleteMove(rpcRequestContext(), req.Topic, req.Partition, req.ExpectedOwner, req.TargetID); err != nil {
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
