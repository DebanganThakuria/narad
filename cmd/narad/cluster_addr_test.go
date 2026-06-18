package main

import "github.com/debanganthakuria/narad/internal/platform/netaddr"

func clusterAddrMatchesPeer(clusterAddr, peerAddr string) bool {
	return netaddr.ClusterAddrMatchesPeer(clusterAddr, peerAddr)
}
