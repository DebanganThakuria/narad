package cluster

import (
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

// ownerAddr returns the API address of the pod that owns (topicName, partition),
// or "" if this pod is the owner or the assignment/member cannot be resolved.
func (rt *Router) ownerAddr(topicName string, p int) string {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return ""
	}
	entry, ok := routes.byPartition[p]
	if !ok {
		return ""
	}
	return rt.consumeOwnerAddr(entry)
}

// consumeOwnerAddr returns the address to forward a consume/ack to, or ""
// when the entry is local or unreachable.
func (rt *Router) consumeOwnerAddr(entry routeEntry) string {
	if entry.ownerID == rt.selfID {
		return ""
	}
	if entry.ownerAlive {
		return entry.ownerAddr
	}
	// No follower replication: a dead owner's partition is unavailable
	// until the owner restarts and its volume reattaches.
	return ""
}

// produceOwnerAddr resolves a produce forward for a route entry: local means
// this pod owns the partition and should handle the produce itself; an empty
// addr with local=false means the owner is dead and the partition must be
// skipped.
func (rt *Router) produceOwnerAddr(entry routeEntry) (addr string, local bool) {
	if !entry.ownerAlive {
		return "", false
	}
	if entry.ownerID == rt.selfID {
		return "", true
	}
	return entry.ownerAddr, false
}

func (rt *Router) leaderMemberAddr() string {
	if addr := rt.memberAddrByClusterAddr(rt.store.LeaderAddr()); addr != "" {
		return addr
	}
	return rt.memberAddrByID(rt.store.LeaderID())
}

func (rt *Router) memberAddrByClusterAddr(clusterAddr string) string {
	members, err := rt.store.ListMembers()
	if err != nil {
		return ""
	}
	for _, member := range members {
		if member.Status == metastore.MemberDead {
			continue
		}
		if !memberMatchesClusterAddr(member, clusterAddr) {
			continue
		}
		if strings.TrimSpace(member.ID) == strings.TrimSpace(rt.selfID) {
			return ""
		}
		return member.Addr
	}
	return ""
}

func (rt *Router) memberAddrByID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" || id == strings.TrimSpace(rt.selfID) {
		return ""
	}
	member, err := rt.store.GetMember(id)
	if err != nil || member.Status == metastore.MemberDead {
		return ""
	}
	return strings.TrimSpace(member.Addr)
}

func memberMatchesClusterAddr(member metastore.Member, clusterAddr string) bool {
	if strings.TrimSpace(member.ClusterAddr) != "" {
		return netaddr.ClusterAddrMatchesPeer(clusterAddr, member.ClusterAddr)
	}
	return netaddr.ClusterAddrMatchesPeer(clusterAddr, member.Addr)
}
