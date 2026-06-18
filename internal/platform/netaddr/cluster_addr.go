package netaddr

import "strings"

func ClusterAddrMatchesPeer(clusterAddr, peerAddr string) bool {
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
