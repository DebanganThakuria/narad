// Package netaddr holds helpers for comparing cluster network addresses.
package netaddr

import "strings"

// ClusterAddrMatchesPeer reports whether clusterAddr and peerAddr refer to
// the same listener. Both are trimmed; an exact match wins, and a bare
// ":port" form matches any host on that port (the wildcard-bind case).
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
