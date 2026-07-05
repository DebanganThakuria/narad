// Package netaddr compares listen addresses with peer addresses that
// may or may not carry a host part.
package netaddr

import "strings"

// ClusterAddrMatchesPeer reports whether a node's cluster listen address
// refers to the same endpoint as a configured peer address.
//
// A node listening on all interfaces (":9101") cannot know which host
// name peers use to reach it, so a port-only address on either side
// matches any address on the other side with the same ":port" suffix.
// When both sides carry a host, they must match exactly.
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
