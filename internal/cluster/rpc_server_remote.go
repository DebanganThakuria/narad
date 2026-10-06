package cluster

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// SetRemotePlane wires the remote plane: OpRemoteWrite and
// OpRemoteCheck are answered 501 until it is set. Call before serving.
func (s *RPCServer) SetRemotePlane(p *RemotePlane) { s.remote = p }

// handleRemoteWrite serves a forwarded remote write. It runs ungated:
// an attach on the leader calls every member and waits on the target,
// so a control slot held here could deadlock against the members'
// answers. The registry's per-node write limit and the links'
// per-parent single flight bound it instead.
func (s *RPCServer) handleRemoteWrite(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeRemoteWriteRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid remote write request")
	}
	if s.remote == nil {
		return errorResponse(http.StatusNotImplemented, "remotes are not available on this node")
	}
	return s.remote.ServeWrite(rpcRequestContext(), req)
}

// handleRemoteCheck serves a member-side check, ungated for the same
// reason; the per-remote check limiter bounds it.
func (s *RPCServer) handleRemoteCheck(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeRemoteCheckRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid remote check request")
	}
	if s.remote == nil {
		return errorResponse(http.StatusNotImplemented, "remotes are not available on this node")
	}
	return s.remote.ServeCheck(rpcRequestContext(), req)
}
