package cluster

import (
	"net/http"
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// notLeaderJoinResponse is a non-leader's answer to a join: 421 with a
// JSON body naming the leader, so a joiner whose pinned peer list does
// not include the current leader (the chart pins it to the first
// initialClusterSize pods, and leadership moves freely) goes straight to
// it instead of retrying 421 until leadership happens to move back:
//
//	{"error": "not the metastore leader", "leader_id": "...", "leader_addr": "..."}
//
// leader_addr is the leader's node-RPC (member) address from this
// node's replica; either field is empty when unknown. The fields are
// additive: an older joiner reads only the status and walks its peers.
func notLeaderJoinResponse(store *metastore.Store) nodewire.Response {
	id, addr := joinLeaderHint(store)
	return jsonResponse(http.StatusMisdirectedRequest, map[string]string{
		"error":       "not the metastore leader",
		"leader_id":   id,
		"leader_addr": addr,
	})
}

// joinLeaderHint resolves the current leader's ID and member address
// from the local replica: the leader's member record, else the member
// whose cluster address matches the leader's Raft address. A "dead"
// mark on the leader's record is ignored: the node is leading, so the
// mark is stale.
func joinLeaderHint(store *metastore.Store) (id, addr string) {
	id = store.LeaderID()
	if id == "" {
		return "", ""
	}
	if m, err := store.GetMember(id); err == nil && strings.TrimSpace(m.Addr) != "" {
		return id, strings.TrimSpace(m.Addr)
	}
	leaderRaft := store.LeaderAddr()
	if leaderRaft == "" {
		return id, ""
	}
	members, err := store.ListMembers()
	if err != nil {
		return id, ""
	}
	for _, m := range members {
		if strings.TrimSpace(m.ClusterAddr) != "" && strings.TrimSpace(m.Addr) != "" && netaddr.ClusterAddrMatchesPeer(leaderRaft, m.ClusterAddr) {
			return id, strings.TrimSpace(m.Addr)
		}
	}
	return id, ""
}
