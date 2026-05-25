package cluster

import (
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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

func clusterAddrMatchesPeer(clusterAddr, peerAddr string) bool {
	clusterAddr = strings.TrimSpace(clusterAddr)
	peerAddr = strings.TrimSpace(peerAddr)
	if clusterAddr == "" || peerAddr == "" {
		return false
	}
	if clusterAddr == peerAddr {
		return true
	}
	if strings.HasPrefix(clusterAddr, ":") {
		return strings.HasSuffix(peerAddr, clusterAddr)
	}
	if strings.HasPrefix(peerAddr, ":") {
		return strings.HasSuffix(clusterAddr, peerAddr)
	}
	return false
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
		if strings.TrimSpace(member.ID) == strings.TrimSpace(rt.selfID) && clusterAddrMatchesPeer(clusterAddr, member.Addr) {
			return ""
		}
		if clusterAddrMatchesPeer(clusterAddr, member.Addr) {
			return member.Addr
		}
	}
	return ""
}
