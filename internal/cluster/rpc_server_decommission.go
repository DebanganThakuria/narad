package cluster

import (
	"context"
	"errors"
	"net/http"

	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handleDecommissionMember runs on the leader (followers forward here): it
// marks a member draining, or clears the drain when Cancel is set. The
// controller's rebalance/decommission passes do the rest — shedding the
// node's partitions and, once drained, removing it from the Raft voter set.
//
// A decommission is preflighted again here, on the leader's replica: the
// node that received the request checked it too, but a 3.0.x follower
// forwards without checking. A decommission that could never complete
// safely is refused with 409 and the reasons, and nothing is written.
func (s *RPCServer) handleDecommissionMember(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeDecommissionRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid decommission request: "+err.Error())
	}
	if !req.Cancel {
		if res, refused := s.preflightDecommission(req.ID); refused {
			return res
		}
	}
	if err := s.store.SetMemberDraining(rpcRequestContext(), req.ID, !req.Cancel); err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return errorResponse(http.StatusNotFound, "member not found")
		}
		return errorResponse(http.StatusServiceUnavailable, "decommission write failed: "+err.Error())
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

// preflightDecommission answers 409 when the decommission of id could
// never complete safely; refused is false when it may proceed (or the
// member is unknown, which the write itself answers with 404).
func (s *RPCServer) preflightDecommission(id string) (res nodewire.Response, refused bool) {
	views, err := controller.DecommissionViews(context.Background(), s.store)
	if err != nil {
		return errorResponse(http.StatusServiceUnavailable, "decommission preflight: "+err.Error()), true
	}
	v, ok := views[id]
	if !ok {
		return nodewire.Response{}, false
	}
	reasons := controller.DecommissionPreflight(v)
	if len(reasons) == 0 {
		return nodewire.Response{}, false
	}
	return jsonResponse(http.StatusConflict, controller.NewDecommissionRefusal(id, reasons)), true
}
