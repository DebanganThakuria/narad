package cluster

import (
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

// ownerAddr returns the API address of the pod that owns (topicName, partition),
// or "" if this pod is the owner or the assignment/member cannot be resolved.
func (rt *Router) ownerAddr(topicName string, p int) string {
	a, err := rt.store.GetAssignment(topicName, p)
	if err != nil {
		return ""
	}
	if a.OwnerID == rt.selfID {
		return ""
	}
	m, err := rt.store.GetMember(a.OwnerID)
	if err == nil && m.Status != metastore.MemberDead {
		return m.Addr
	}
	if a.FollowerID == "" || a.FollowerID == rt.selfID {
		return ""
	}
	fm, err := rt.store.GetMember(a.FollowerID)
	if err != nil || fm.Status == metastore.MemberDead {
		return ""
	}
	return fm.Addr
}

func (rt *Router) produceOwnerAddr(topicName string, p int) (string, bool) {
	a, err := rt.store.GetAssignment(topicName, p)
	if err != nil {
		return "", false
	}
	ownerAddr, writable := rt.produceAssignmentWritable(a)
	if !writable {
		return "", false
	}
	if a.OwnerID == rt.selfID {
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
