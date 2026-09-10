package cluster

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handleTokenRegister takes a peer's batched token delta: the topics it
// now wants to hear about and the ones it no longer does. Registering
// reserves nothing, so it is cheap and needs no reply body.
//
// Never gated behind the messaging semaphore. A registration is
// bookkeeping, and queueing it behind produce commits would delay the
// very consumers it exists to wake.
func (s *RPCServer) handleTokenRegister(payload []byte) nodewire.Response {
	delta, err := nodewire.DecodeTokenDelta(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid token delta: "+err.Error())
	}
	if s.tokens == nil {
		// This node is not wired for the token protocol (single-node, or
		// a test). Silently accepting is right: the peer falls back to
		// polling and nothing breaks.
		return nodewire.Response{Status: http.StatusNoContent}
	}
	if delta.From == "" {
		return errorResponse(http.StatusBadRequest, "token delta missing sender address")
	}
	s.tokens.ApplyDelta(rpcRequestContext(), delta)
	return nodewire.Response{Status: http.StatusNoContent}
}

// handleTokenNotify answers an owner spending one of our tokens. The
// reply is a single verdict byte and must come back promptly: while it
// is outstanding the owner is holding a record for us, so a slow answer
// costs somebody else a delivery.
//
// Answering "pass" whenever we are not wired for tokens, or have no
// consumer waiting, is the safe direction: the owner offers the record
// to another peer immediately rather than waiting out its deadline.
func (s *RPCServer) handleTokenNotify(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeTokenNotifyRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid token notify: "+err.Error())
	}
	// The sender's address is the only thing a woken consumer has to aim
	// its claim at, so a notification without one is unanswerable. Taking
	// it anyway is worse than refusing: waking a consumer answers
	// "claiming", which makes the owner hold that record for its whole
	// deadline while the claim goes to an address that cannot be dialled.
	// handleTokenRegister refuses an empty From for the same reason.
	if req.From == "" {
		return errorResponse(http.StatusBadRequest, "token notify missing sender address")
	}
	claiming := false
	if s.demand != nil {
		claiming = s.demand.WakeOneWaiter(req.Topic, req.From)
	}
	return nodewire.Response{
		Status:      http.StatusOK,
		ContentType: "application/octet-stream",
		Body:        nodewire.EncodeTokenNotifyReply(nodewire.TokenNotifyReply{Claiming: claiming}),
	}
}

// localDemand is the requester half's surface: it knows whether this
// node has a consumer parked on a topic and can wake one to claim.
// *Router satisfies it.
type localDemand interface {
	// WakeOneWaiter hands the owner's address to a parked consumer and
	// reports whether one took it. False is a pass: nobody here wants
	// this any more, so the owner should offer it elsewhere at once.
	WakeOneWaiter(topicName, from string) bool
}

// SetTokenHolder wires the owner half: the store of tokens peers have
// left with this node. Call before serving; nil disables the protocol,
// and peers fall back to polling.
func (s *RPCServer) SetTokenHolder(h *tokenHolder) { s.tokens = h }

// SetLocalDemand wires the requester half: how an inbound notification
// reaches a consumer parked on this node.
func (s *RPCServer) SetLocalDemand(d localDemand) { s.demand = d }
