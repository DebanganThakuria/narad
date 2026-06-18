package main

import (
	"net"
	"strings"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

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

func advertisedMemberAddr(nodeID, httpAddr, clusterAddr string, peers []config.ClusterPeer) string {
	httpAddr = strings.TrimSpace(httpAddr)
	if httpAddr == "" || !strings.HasPrefix(httpAddr, ":") {
		return httpAddr
	}

	clusterAdvertiseAddr := advertisedClusterAddr(nodeID, clusterAddr, peers)
	host, _, err := net.SplitHostPort(strings.TrimSpace(clusterAdvertiseAddr))
	if err != nil || strings.TrimSpace(host) == "" {
		return httpAddr
	}
	return net.JoinHostPort(host, strings.TrimPrefix(httpAddr, ":"))
}
