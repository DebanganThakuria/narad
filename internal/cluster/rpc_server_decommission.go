package cluster

import (
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handleDecommissionMember runs on the leader (followers forward here): it
// marks a member draining, or clears the drain when Cancel is set. The
// controller's rebalance/decommission passes do the rest — shedding the
// node's partitions and, once drained, removing it from the Raft voter set.
func (s *RPCServer) handleDecommissionMember(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeDecommissionRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid decommission request: "+err.Error())
	}
	if err := s.store.SetMemberDraining(rpcRequestContext(), req.ID, !req.Cancel); err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return errorResponse(http.StatusNotFound, "member not found")
		}
		return errorResponse(http.StatusServiceUnavailable, "decommission write failed: "+err.Error())
	}
	return nodewire.Response{Status: http.StatusNoContent}
}
