package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// SetNodeStatus wires what this node answers to OpNodeStatus: status
// builds the reply from the components that own each field (the ingress
// dispatch backlog, the quarantine inventory, the move workers). It must
// be cheap: it runs under the control-op bound. Call before serving; a
// server without it answers the op as unsupported, like a 3.0.x node.
func (s *RPCServer) SetNodeStatus(status func(ctx context.Context) nodewire.NodeStatus) {
	s.nodeStatus = status
}

// handleNodeStatus answers a node-status probe.
func (s *RPCServer) handleNodeStatus(ctx context.Context, payload []byte) nodewire.Response {
	if err := nodewire.DecodeNodeStatusRequest(payload); err != nil {
		return errorResponse(http.StatusBadRequest, "invalid node status request: "+err.Error())
	}
	if s.nodeStatus == nil {
		return errorResponse(http.StatusBadRequest, fmt.Sprintf("unsupported rpc operation %d", nodewire.OpNodeStatus))
	}
	return jsonResponse(http.StatusOK, s.nodeStatus(ctx))
}

// ErrNodeStatusUnsupported reports a peer that cannot answer OpNodeStatus
// (it runs 3.0.x, whose server answers 400 "unsupported rpc operation"):
// reachable, but its status is unknown. Callers treat that as unknown,
// never as unhealthy.
var ErrNodeStatusUnsupported = errors.New("peer does not support node status (an older release)")

// NodeStatus asks the peer at addr for its own status. A peer that
// predates the operation yields ErrNodeStatusUnsupported; a transport
// failure is returned as is. Fields a newer peer adds are ignored.
func (c *PeerClient) NodeStatus(ctx context.Context, addr string) (nodewire.NodeStatus, error) {
	res, err := c.send(ctx, addr, "node_status", laneControl, nodewire.EncodeNodeStatusRequest(), nil)
	if err != nil {
		return nodewire.NodeStatus{}, err
	}
	switch {
	case res.Status == http.StatusOK:
	case isUnsupportedOp(res):
		return nodewire.NodeStatus{}, ErrNodeStatusUnsupported
	default:
		return nodewire.NodeStatus{}, fmt.Errorf("node status: peer answered %d", res.Status)
	}
	var st nodewire.NodeStatus
	if err := json.Unmarshal(res.Body, &st); err != nil {
		return nodewire.NodeStatus{}, fmt.Errorf("node status: decode reply: %w", err)
	}
	return st, nil
}
