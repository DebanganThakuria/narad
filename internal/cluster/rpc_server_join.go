package cluster

import (
	"net/http"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handleJoinCluster admits a scale-out node into the Raft voter set.
// Only the leader can change the configuration; a non-leader answers
// 421 and the joiner simply tries its next configured peer. AddVoter is
// idempotent, so a retried join (lost reply, joiner restart) is safe.
//
// Two more answers carry meaning for the joiner:
//   - 412: this node has no Raft configuration at all (never
//     bootstrapped, or itself waiting for admission). It is not evidence
//     that a cluster exists, which is what an initial member with no
//     state asks before deciding whether to bootstrap.
//   - 409: the ID was removed by decommission. The old incarnation (still
//     running, or restarted with its old volume) is refused so it cannot
//     undo the decommission; a node that declares an EMPTY data
//     directory is a deliberate re-add and is readmitted.
func (s *RPCServer) handleJoinCluster(payload []byte) nodewire.Response {
	req, err := nodewire.DecodeJoinClusterRequest(payload)
	if err != nil {
		return errorResponse(http.StatusBadRequest, "invalid join request: "+err.Error())
	}
	if req.ID == "" || req.ClusterAddr == "" {
		return errorResponse(http.StatusBadRequest, "join request requires id and cluster addr")
	}
	if has, err := s.store.HasRaftConfiguration(); err != nil || !has {
		return errorResponse(http.StatusPreconditionFailed, "no raft configuration on this node")
	}
	if !s.store.IsLeader() {
		return errorResponse(http.StatusMisdirectedRequest, "not the metastore leader")
	}
	removed, err := s.store.MemberRemoved(req.ID)
	if err != nil {
		return errorResponse(http.StatusServiceUnavailable, "membership lookup failed")
	}
	if removed {
		if !req.Fresh {
			return errorResponse(http.StatusConflict, "node was decommissioned and removed from the cluster; delete its data directory (volume) to rejoin as a new node")
		}
		if err := s.store.ReadmitMember(rpcRequestContext(), req.ID); err != nil {
			if s.logger != nil {
				s.logger.Error("join cluster: readmit member", "id", req.ID, "err", err)
			}
			return errorResponse(http.StatusServiceUnavailable, "readmit member failed")
		}
		if s.logger != nil {
			s.logger.Info("cluster join: readmitting a previously decommissioned id with a fresh data directory", "id", req.ID)
		}
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
