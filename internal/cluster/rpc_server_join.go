package cluster

import (
	"net/http"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// handleJoinCluster admits a scale-out node into the Raft
// configuration. Only the leader can change the configuration; a
// non-leader answers 421 naming the leader's member address when its
// replica knows it (notLeaderJoinResponse), which the joiner asks next.
//
// A node not yet in the configuration is staged as a Raft NON-VOTER
// (metastore.AdmitJoiner): it replicates but does not count toward
// quorum, so a joiner the voters cannot reach costs nothing. It sends
// the same request again once its replica has caught up, and the leader
// promotes it to voter when it has led long enough to judge heartbeats,
// is not failing to heartbeat the joiner, and the joiner's member record
// is alive and not draining; otherwise the answer is "deferred" with the
// reason, and the joiner asks again later. A join from a voter changes
// nothing but its address, so a retried join (lost reply, joiner
// restart, a voter's leaderless join loop) is safe. The 200 body is
// {"status": staged|deferred|promoted|voter, "reason": "..."}.
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
		return notLeaderJoinResponse(s.store)
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
	adm, err := s.store.AdmitJoiner(req.ID, req.ClusterAddr)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("join cluster: admission failed", "id", req.ID, "cluster_addr", req.ClusterAddr, "err", err)
		}
		return errorResponse(http.StatusServiceUnavailable, "admission failed")
	}
	if s.logger != nil {
		switch adm.Status {
		case metastore.JoinStaged:
			s.logger.Info("cluster join: admitted as a raft non-voter; promoted to voter when it asks again, caught up", "id", req.ID, "cluster_addr", req.ClusterAddr)
		case metastore.JoinPromoted:
			s.logger.Info("cluster join: promoted a non-voter to voter", "id", req.ID, "cluster_addr", req.ClusterAddr)
		case metastore.JoinDeferred:
			s.logger.Debug("cluster join: promotion deferred", "id", req.ID, "reason", adm.Reason)
		}
	}
	return jsonResponse(http.StatusOK, map[string]string{"status": adm.Status, "reason": adm.Reason})
}
