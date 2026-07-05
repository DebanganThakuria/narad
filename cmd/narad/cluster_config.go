package main

import (
	"net"
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

// configPeersToMetastore converts the configured peer list — which every
// node shares verbatim — into metastore peers, dropping this node's own
// entry: it is the local voter, not a remote peer.
func configPeersToMetastore(nodeID, clusterAddr string, peers []config.ClusterPeer) []metastore.Peer {
	if len(peers) == 0 {
		return nil
	}
	out := make([]metastore.Peer, 0, len(peers)-1)
	for _, peer := range peers {
		if peer.ID == nodeID && netaddr.ClusterAddrMatchesPeer(clusterAddr, peer.Addr) {
			continue
		}
		out = append(out, metastore.Peer{ID: peer.ID, Addr: peer.Addr})
	}
	return out
}

// advertisedClusterAddr resolves the cluster address this node advertises
// to peers. When cluster.addr is port-only (":9101") the bind address is
// not reachable from other hosts, so the node's own hostful entry in the
// shared peer list is advertised instead; otherwise cluster.addr stands.
func advertisedClusterAddr(nodeID, clusterAddr string, peers []config.ClusterPeer) string {
	for _, peer := range peers {
		if peer.ID != nodeID || !netaddr.ClusterAddrMatchesPeer(clusterAddr, peer.Addr) {
			continue
		}
		if strings.HasPrefix(clusterAddr, ":") && !strings.HasPrefix(strings.TrimSpace(peer.Addr), ":") {
			return peer.Addr
		}
		return clusterAddr
	}
	return clusterAddr
}

// advertisedMemberAddr resolves the HTTP address this node advertises in
// cluster membership. A port-only http.addr borrows the host from the
// advertised cluster address so peers get a reachable endpoint; if no
// host can be derived, the raw http.addr is kept as-is.
func advertisedMemberAddr(nodeID, httpAddr, clusterAddr string, peers []config.ClusterPeer) string {
	httpAddr = strings.TrimSpace(httpAddr)
	if httpAddr == "" || !strings.HasPrefix(httpAddr, ":") {
		return httpAddr
	}

	advertised := advertisedClusterAddr(nodeID, clusterAddr, peers)
	host, _, err := net.SplitHostPort(strings.TrimSpace(advertised))
	if err != nil || strings.TrimSpace(host) == "" {
		return httpAddr
	}
	return net.JoinHostPort(host, strings.TrimPrefix(httpAddr, ":"))
}
