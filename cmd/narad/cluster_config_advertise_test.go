package main

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// advertisedMemberAddr must make a node's membership address routable by
// peers regardless of how the operator wrote the bind: port-only,
// 0.0.0.0 (the common "all interfaces" idiom), or IPv6 unspecified. A
// verbatim 0.0.0.0 is the silent-cluster-death case chaos testing found.
func TestAdvertisedMemberAddrRoutability(t *testing.T) {
	peers := []config.ClusterPeer{
		{ID: "narad-0", Addr: "10.0.0.10:7943"},
		{ID: "narad-1", Addr: "10.0.0.11:7943"},
	}
	for _, tt := range []struct {
		name, httpAddr, clusterAddr, want string
	}{
		{"port only", ":7942", "10.0.0.10:7943", "10.0.0.10:7942"},
		{"ipv4 unspecified", "0.0.0.0:7942", "10.0.0.10:7943", "10.0.0.10:7942"},
		{"ipv6 unspecified", "[::]:7942", "10.0.0.10:7943", "10.0.0.10:7942"},
		{"already routable kept", "192.168.1.5:7942", "10.0.0.10:7943", "192.168.1.5:7942"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := advertisedMemberAddr("narad-0", tt.httpAddr, tt.clusterAddr, peers)
			if got != tt.want {
				t.Fatalf("advertisedMemberAddr(%q) = %q, want %q", tt.httpAddr, got, tt.want)
			}
			if memberAddrLikelyUnroutable(got) {
				t.Fatalf("resolved %q still flagged unroutable", got)
			}
		})
	}
}

func TestMemberAddrLikelyUnroutable(t *testing.T) {
	for _, tt := range []struct {
		addr string
		want bool
	}{
		{"0.0.0.0:7942", true},
		{"[::]:7942", true},
		{":7942", true},
		{"", true},
		{"10.0.0.5:7942", false},
		{"narad-0:7942", false},
	} {
		if got := memberAddrLikelyUnroutable(tt.addr); got != tt.want {
			t.Fatalf("memberAddrLikelyUnroutable(%q) = %v, want %v", tt.addr, got, tt.want)
		}
	}
}
