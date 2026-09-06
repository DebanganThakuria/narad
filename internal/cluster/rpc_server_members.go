package cluster

import (
	"errors"
	"net/http"
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func (s *RPCServer) handleRegisterMember(payload []byte) nodewire.Response {
	if s.store == nil {
		return errorResponse(http.StatusInternalServerError, "metastore unavailable")
	}
	req, err := nodewire.DecodeMemberRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid member request: "+err.Error())
	}

	// LastHeartbeat is stamped HERE, on the leader's clock, and the
	// sender's value is ignored. The controller compares it with the
	// leader's clock (checkHeartbeats), so a member whose clock ran 30 s
	// slow was marked dead while healthy (partitions rerouted and moved
	// away) and one whose clock ran fast was never marked dead after it
	// really died. The value still lives in the Raft proposal, so the
	// FSM stays deterministic.
	member := metastore.Member{
		ID:            strings.TrimSpace(req.ID),
		Addr:          strings.TrimSpace(req.Addr),
		ClusterAddr:   strings.TrimSpace(req.ClusterAddr),
		Status:        metastore.MemberStatus(strings.TrimSpace(req.Status)),
		LastHeartbeat: s.clock().Unix(),
	}
	if member.ID == "" {
		return errorResponse(http.StatusBadRequest, "member id is required")
	}
	if member.Addr == "" {
		return errorResponse(http.StatusBadRequest, "member addr is required")
	}
	if member.Status == "" {
		member.Status = metastore.MemberAlive
	}
	if member.Status != metastore.MemberAlive && member.Status != metastore.MemberDead {
		return errorResponse(http.StatusBadRequest, "member status is invalid")
	}
	if err := s.store.RegisterMember(rpcRequestContext(), member); err != nil {
		if errors.Is(err, metastore.ErrMemberRemoved) {
			// A decommissioned pod still heartbeating: refuse quietly so
			// it is never resurrected as an alive member.
			return errorResponse(http.StatusGone, "member was removed from the cluster")
		}
		if s.logger != nil {
			s.logger.Error("register member", "member", member.ID, "err", err)
		}
		return errorResponse(http.StatusConflict, "register member failed")
	}
	return nodewire.Response{Status: http.StatusNoContent}
}
