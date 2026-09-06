package cluster

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// appliedIndexResponse is the body of a successful applied-index probe.
type appliedIndexResponse struct {
	AppliedIndex uint64 `json:"applied_index"`
}

// handleAppliedIndex answers a follower's applied-index probe. Only the
// leader answers: a forwarded write completed on the leader after its
// FSM applied the entry, so the leader's applied index is a bound the
// follower can wait for. Any other node's index says nothing about that
// write, so a node that lost leadership between the write and the probe
// answers 503 and the follower answers the client without waiting
// (replication delivers the write within milliseconds either way).
func (s *RPCServer) handleAppliedIndex(payload []byte) nodewire.Response {
	if err := nodewire.DecodeAppliedIndexRequest(payload); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid applied index request: "+err.Error())
	}
	if s.store == nil {
		return errorResponse(http.StatusInternalServerError, "metastore unavailable")
	}
	if !s.store.IsLeader() {
		return errorResponse(http.StatusServiceUnavailable, "node is not the leader")
	}
	index := s.store.AppliedIndex()
	return jsonResponse(http.StatusOK, appliedIndexResponse{AppliedIndex: index})
}
