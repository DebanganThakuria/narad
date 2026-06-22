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

func (rt *Router) produceOwnerAddr(topicName string, p int) (string, bool) {
	routes, ok := rt.routesForTopic(topicName)
	if !ok {
		return "", false
	}
	entry, ok := routes.byPartition[p]
	if !ok {
		return "", false
	}
	return rt.produceOwnerAddrForRoute(entry)
}

func (rt *Router) consumeOwnerAddrForRoute(entry routeEntry) string {
	if entry.ownerID == rt.selfID {
		return ""
	}
	if entry.ownerAlive {
		return entry.ownerAddr
	}
	if entry.followerID == "" || entry.followerID == rt.selfID || !entry.followerAlive {
		return ""
	}
	return entry.followerAddr
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

func (rt *Router) produceAssignmentWritable(a metastore.Assignment) (string, bool) {
	owner, err := rt.store.GetMember(a.OwnerID)
	if err != nil || owner.Status != metastore.MemberAlive {
		return "", false
	}
	if a.FollowerID == "" {
		return owner.Addr, true
	}
	follower, err := rt.store.GetMember(a.FollowerID)
	return owner.Addr, err == nil && follower.Status == metastore.MemberAlive
}

func (rt *Router) produceAssignmentWritableForRoute(entry routeEntry) (string, bool) {
	if !entry.ownerAlive {
		return "", false
	}
	if entry.followerID == "" {
		return entry.ownerAddr, true
	}
	return entry.ownerAddr, entry.followerAlive
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

func memberMatchesClusterAddr(member metastore.Member, clusterAddr string) bool {
	if strings.TrimSpace(member.ClusterAddr) != "" {
		return netaddr.ClusterAddrMatchesPeer(clusterAddr, member.ClusterAddr)
	}
	return netaddr.ClusterAddrMatchesPeer(clusterAddr, member.Addr)
}
