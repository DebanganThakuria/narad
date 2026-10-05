package cluster

import (
	"fmt"
	"maps"
	"net/http"
	"time"

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
//   - 409 with code older_release: the joiner applies fewer Raft entry
//     types than every member (refuseOlderJoiner).
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
	if res, refused := s.refuseOlderJoiner(req); refused {
		return res
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

// JoinCodeOlderRelease is the "code" of a 409 join answer refusing a
// joiner that applies fewer Raft entry types than every member. A 409
// without it is the removed-ID refusal.
const JoinCodeOlderRelease = "older_release"

// olderReleaseLogEvery is how often the leader logs refusing one
// joiner as older than every member: the joiner asks again every two
// seconds, and one error line a minute says enough.
const olderReleaseLogEvery = time.Minute

// refuseOlderJoiner refuses a node not yet in the Raft configuration
// that applies fewer Raft entry types than every current member
// (metastore.Store.MinMemberEntryTypes): the leader proposes an entry
// type once every member knows it, so such a cluster may already use
// entries the joiner would skip (3.0.x) or stop on (3.1.0 and later). A
// joiner that reports nothing (3.0.x) applies the 3.0.x set. A server
// already in the configuration is never refused: it is a member, and its
// own heartbeat holds newer types back.
//
// 409 is deliberate: a 3.0.x node with an empty data directory reads
// only 200, 421 and 409 as "a cluster exists" before deciding whether to
// bootstrap, so any other status could make it start a rival cluster.
// Its join loop logs the 409 with this body, which says what to do.
func (s *RPCServer) refuseOlderJoiner(req nodewire.JoinClusterRequest) (nodewire.Response, bool) {
	inConfig, err := s.store.RaftServer(req.ID)
	if err != nil {
		return errorResponse(http.StatusServiceUnavailable, "raft configuration lookup failed"), true
	}
	if inConfig {
		return nodewire.Response{}, false
	}
	lowest, err := s.store.MinMemberEntryTypes(req.ID)
	if err != nil {
		return errorResponse(http.StatusServiceUnavailable, "membership lookup failed"), true
	}
	joiner := metastore.ReportedEntryTypes(req.EntryTypes)
	if joiner >= lowest {
		return nodewire.Response{}, false
	}
	s.logOlderReleaseJoin(req.ID, joiner, lowest)
	return jsonResponse(http.StatusConflict, map[string]string{
		"error": fmt.Sprintf("this node runs an older release than every member of the cluster: it applies Raft entry types up to %d, every member applies up to %d or more, so the cluster may already use entries it would skip; upgrade it to the cluster's release before it joins", joiner, lowest),
		"code":  JoinCodeOlderRelease,
	}), true
}

// logOlderReleaseJoin logs a refused older joiner at error, at most once
// per olderReleaseLogEvery for each joiner ID.
func (s *RPCServer) logOlderReleaseJoin(id string, joiner, lowest uint32) {
	if s.logger == nil {
		return
	}
	now := s.clock()
	s.olderReleaseMu.Lock()
	last, seen := s.olderReleaseLogged[id]
	due := !seen || now.Sub(last) >= olderReleaseLogEvery
	if due {
		if s.olderReleaseLogged == nil {
			s.olderReleaseLogged = make(map[string]time.Time)
		}
		maps.DeleteFunc(s.olderReleaseLogged, func(_ string, at time.Time) bool { return now.Sub(at) >= olderReleaseLogEvery })
		s.olderReleaseLogged[id] = now
	}
	s.olderReleaseMu.Unlock()
	if due {
		s.logger.Error("cluster join refused: the joiner runs an older release than every member; upgrade it to the cluster's release",
			"id", id, "joiner_entry_types", joiner, "member_entry_types_min", lowest)
	}
}
