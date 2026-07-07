package cluster

// Fan-out RPC handlers. Attach and detach are Raft writes, so they run
// on the leader — followers forward via Router.RouteAttachChild /
// RouteDetachChild. Cursor stats are served by every parent-partition
// owner and merged by the API node. Authorization happens at the HTTP
// ingress node; the cluster port is peer-only and trusted.

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func (s *RPCServer) handleAttachChild(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeChildLinkRequest(payload, nodewire.OpAttachChild)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid attach child request: "+err.Error())
	}
	if err := s.broker.AttachChild(rpcRequestContext(), req.Parent, req.Child); err != nil {
		return s.brokerError("attach child", err)
	}
	t, err := s.broker.GetTopic(rpcRequestContext(), req.Parent)
	if err != nil {
		return s.brokerError("attach child", err)
	}
	return jsonResponse(http.StatusOK, t)
}

func (s *RPCServer) handleDetachChild(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeChildLinkRequest(payload, nodewire.OpDetachChild)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid detach child request: "+err.Error())
	}
	if err := s.broker.DetachChild(rpcRequestContext(), req.Parent, req.Child); err != nil {
		return s.brokerError("detach child", err)
	}
	return nodewire.Response{Status: http.StatusNoContent}
}

func (s *RPCServer) handleFanoutCursors(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTopicNameRequest(payload, nodewire.OpFanoutCursors)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid fanout cursors request: "+err.Error())
	}
	stats, err := s.broker.FanoutCursorStats(rpcRequestContext(), req.Topic)
	if err != nil {
		return s.brokerError("fanout cursors", err)
	}
	return jsonResponse(http.StatusOK, stats)
}
