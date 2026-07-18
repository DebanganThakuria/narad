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
// cluster membership. A non-routable host — port-only (":7942") OR an
// unspecified bind ("0.0.0.0:7942", "[::]:7942") — borrows the host from
// the advertised cluster address so peers get a reachable endpoint.
// Unspecified matters because "0.0.0.0:port" is the ordinary way to
// write "bind every interface": advertising it verbatim makes every
// peer resolve the leader as 0.0.0.0 (i.e. itself) and member
// registration silently fails. If no host can be derived, the raw
// http.addr is kept as-is (and memberAddrLikelyUnroutable warns).
func advertisedMemberAddr(nodeID, httpAddr, clusterAddr string, peers []config.ClusterPeer) string {
	httpAddr = strings.TrimSpace(httpAddr)
	if httpAddr == "" {
		return httpAddr
	}

	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return httpAddr // not host:port shaped; leave it alone
	}
	if !hostNeedsAdvertise(host) {
		return httpAddr
	}

	advertised := advertisedClusterAddr(nodeID, clusterAddr, peers)
	advHost, _, err := net.SplitHostPort(strings.TrimSpace(advertised))
	if err != nil || strings.TrimSpace(advHost) == "" {
		return httpAddr
	}
	return net.JoinHostPort(advHost, port)
}

// hostNeedsAdvertise reports whether a bind host is non-routable to
// peers: empty (port-only) or an unspecified address (0.0.0.0, ::).
func hostNeedsAdvertise(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return true
	}
	return false
}

// memberAddrLikelyUnroutable reports whether the resolved member address
// would still be unreachable by peers — a signal to warn at startup that
// cluster membership will not converge.
func memberAddrLikelyUnroutable(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return true
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false // odd shape; don't cry wolf
	}
	return hostNeedsAdvertise(host)
}
