package cluster

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handleJoinCluster admits a scale-out node into the Raft voter set.
// Only the leader can change the configuration; a non-leader answers
// 421 and the joiner simply tries its next configured peer. AddVoter is
// idempotent, so a retried join (lost reply, joiner restart) is safe.
func (s *RPCServer) handleJoinCluster(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeJoinClusterRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid join request: "+err.Error())
	}
	if req.ID == "" || req.ClusterAddr == "" {
		return errorResponse(http.StatusBadRequest, "join request requires id and cluster addr")
	}
	if !s.store.IsLeader() {
		return errorResponse(http.StatusMisdirectedRequest, "not the metastore leader")
	}
	if err := s.store.AddVoter(req.ID, req.ClusterAddr); err != nil {
		if s.logger != nil {
			s.logger.Error("join cluster: add voter", "id", req.ID, "addr", req.ClusterAddr, "err", err)
		}
		return errorResponse(http.StatusServiceUnavailable, "add voter failed")
	}
	if s.logger != nil {
		s.logger.Info("cluster join: added voter", "id", req.ID, "cluster_addr", req.ClusterAddr)
	}
	return jsonResponse(http.StatusOK, map[string]string{"status": "joined"})
}
