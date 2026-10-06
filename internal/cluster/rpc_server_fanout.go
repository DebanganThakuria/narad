package cluster

// Fan-out RPC handlers. Attach and detach are Raft writes, so they run
// on the leader — followers forward via Router.RouteAttachChild /
// RouteDetachChild. Cursor stats are served by every parent-partition
// owner and merged by the API node. The ingress node authorizes attach
// and detach against its own replica and forwards the caller; the
// leader re-checks that caller's rights under the topics' locks (see
// actorContext). The cluster port is peer-only and authenticated.

import (
	"context"
	"net/http"
	"strconv"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// SetFanoutRunner gives the RPC server the node's fan-out runner. Call
// before serving.
func (s *RPCServer) SetFanoutRunner(r *FanoutRunner) { s.fanout = r }

func (s *RPCServer) handleAttachChild(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeChildLinkRequest(payload, nodewire.OpAttachChild)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid attach child request: "+err.Error())
	}
	ctx, refusal := s.actorContext(req.Actor)
	if refusal != nil {
		return *refusal
	}
	if err := s.broker.AttachChild(ctx, req.Parent, req.Child, req.DelayMs); err != nil {
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
	// A remote child's delete must run the unshipped check, which only
	// the remote-aware delete (OpRemoteWrite child.delete) does; no
	// forwarding path may skip it.
	if child, err := s.broker.GetTopic(rpcRequestContext(), req.Child); err == nil && child.IsRemoteChild() {
		return s.brokerError("detach child", remoteAwareDeleteRequired(req.Child))
	}
	ctx, refusal := s.actorContext(req.Actor)
	if refusal != nil {
		return *refusal
	}
	if err := s.broker.DetachChild(ctx, req.Parent, req.Child); err != nil {
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
	stats = s.fanout.OverlayRemoteCursorStats(req.Topic, stats)
	return jsonResponse(http.StatusOK, stats)
}

// remoteAwareDeleteRequired is the 409 a leader answers a raw delete or
// detach of a stub, or of a parent with remote children, with.
func remoteAwareDeleteRequired(name string) error {
	return errs.RemoteChildError(errs.ErrRemoteAwareDeleteRequired,
		"use the remote-aware delete: "+strconv.Quote(name)+" is, or has, a remote child whose records may not be shipped yet")
}

// remoteLinked reports whether t is a stub or a parent with remote
// children, reading the children from b.
func remoteLinked(b interface {
	GetTopic(context.Context, string) (topic.Topic, error)
}, t topic.Topic,
) bool {
	if t.IsRemoteChild() {
		return true
	}
	for _, name := range t.Children {
		if c, err := b.GetTopic(rpcRequestContext(), name); err == nil && c.IsRemoteChild() {
			return true
		}
	}
	return false
}
