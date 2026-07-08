package main

import "testing"

func TestJoinOnlyNode(t *testing.T) {
	cases := []struct {
		name    string
		nodeID  string
		initial []string
		want    bool
	}{
		{"empty list: every node bootstraps (legacy/static)", "narad-3", nil, false},
		{"initial member bootstraps", "narad-1", []string{"narad-0", "narad-1", "narad-2"}, false},
		{"scale-out node joins", "narad-3", []string{"narad-0", "narad-1", "narad-2"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinOnlyNode(tc.nodeID, tc.initial); got != tc.want {
				t.Fatalf("joinOnlyNode(%q, %v) = %v, want %v", tc.nodeID, tc.initial, got, tc.want)
			}
		})
	}
}

func TestPeerMemberAddr(t *testing.T) {
	cases := []struct {
		peerClusterAddr string
		httpAddr        string
		want            string
	}{
		{"narad-0.narad-headless.narad.svc.cluster.local:7943", ":7942", "narad-0.narad-headless.narad.svc.cluster.local:7942"},
		{"10.0.0.9:7943", "0.0.0.0:7942", "10.0.0.9:7942"},
		{"no-port-here", ":7942", ""},
		{"host:7943", "", ""},
	}
	for _, tc := range cases {
		if got := peerMemberAddr(tc.peerClusterAddr, tc.httpAddr); got != tc.want {
			t.Fatalf("peerMemberAddr(%q, %q) = %q, want %q", tc.peerClusterAddr, tc.httpAddr, got, tc.want)
		}
	}
}
