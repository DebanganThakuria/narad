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
	return rt.consumeOwnerAddrForRoute(entry)
}

func (rt *Router) consumeOwnerAddrForRoute(entry routeEntry) string {
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

func (rt *Router) produceOwnerAddrForRoute(entry routeEntry) (string, bool) {
	ownerAddr, writable := rt.produceAssignmentWritableForRoute(entry)
	if !writable {
		return "", false
	}
	if entry.ownerID == rt.selfID {
		return "", true
	}
	return ownerAddr, false
}

func (rt *Router) produceAssignmentWritableForRoute(entry routeEntry) (string, bool) {
	if !entry.ownerAlive {
		return "", false
	}
	return entry.ownerAddr, true
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

func (rt *Router) leaderMemberAddr() string {
	if addr := rt.memberAddrByClusterAddr(rt.store.LeaderAddr()); addr != "" {
		return addr
	}
	return rt.memberAddrByID(rt.store.LeaderID())
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
