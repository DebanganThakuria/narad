package cluster

import (
	"net/http"
	"strings"
	"time"

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

	member := metastore.Member{
		ID:            strings.TrimSpace(req.ID),
		Addr:          strings.TrimSpace(req.Addr),
		ClusterAddr:   strings.TrimSpace(req.ClusterAddr),
		Status:        metastore.MemberStatus(strings.TrimSpace(req.Status)),
		LastHeartbeat: req.LastHeartbeat,
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
	if member.LastHeartbeat == 0 {
		member.LastHeartbeat = time.Now().Unix()
	}

	if err := s.store.RegisterMember(rpcRequestContext(), member); err != nil {
		if s.logger != nil {
			s.logger.Error("register member", "member", member.ID, "err", err)
		}
		return errorResponse(http.StatusConflict, "register member failed")
	}
	return nodewire.Response{Status: http.StatusNoContent}
}
