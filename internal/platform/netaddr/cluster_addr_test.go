package netaddr

import "testing"

func TestClusterAddrMatchesPeer(t *testing.T) {
	tests := []struct {
		name        string
		clusterAddr string
		peerAddr    string
		want        bool
	}{
		{name: "rejects empty cluster addr", clusterAddr: "", peerAddr: "127.0.0.1:9101"},
		{name: "rejects empty peer addr", clusterAddr: ":9101", peerAddr: ""},
		{name: "rejects whitespace only cluster addr", clusterAddr: "   ", peerAddr: "127.0.0.1:9101"},
		{name: "accepts exact hostful address", clusterAddr: "127.0.0.1:9101", peerAddr: "127.0.0.1:9101", want: true},
		{name: "rejects different host with same port", clusterAddr: "127.0.0.1:9101", peerAddr: "127.0.0.2:9101"},
		{name: "rejects different port with same host", clusterAddr: "127.0.0.1:9101", peerAddr: "127.0.0.1:9102"},
		{name: "accepts cluster port against IPv4 peer", clusterAddr: ":9101", peerAddr: "10.0.0.1:9101", want: true},
		{name: "accepts cluster port against hostname peer", clusterAddr: ":9101", peerAddr: "narad-0.default.svc:9101", want: true},
		{name: "accepts cluster port against IPv6 peer", clusterAddr: ":9101", peerAddr: "[::1]:9101", want: true},
		{name: "rejects cluster port with different peer port", clusterAddr: ":9101", peerAddr: "narad-0.default.svc:9102"},
		{name: "accepts hostful addr against port-only peer", clusterAddr: "narad-0.default.svc:9101", peerAddr: ":9101", want: true},
		{name: "rejects hostful addr against different port-only peer", clusterAddr: "narad-0.default.svc:9101", peerAddr: ":9102"},
		{name: "trims whitespace before matching", clusterAddr: " :9101 ", peerAddr: " narad-0.default.svc:9101 ", want: true},
		{name: "rejects short port suffix false positive", clusterAddr: ":101", peerAddr: "host:1101"},
		{name: "rejects long port suffix false positive", clusterAddr: ":19101", peerAddr: "host:29101"},
		{name: "rejects bare peer port text", clusterAddr: ":9101", peerAddr: "9101"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClusterAddrMatchesPeer(tt.clusterAddr, tt.peerAddr); got != tt.want {
				t.Fatalf("ClusterAddrMatchesPeer(%q, %q) = %v, want %v", tt.clusterAddr, tt.peerAddr, got, tt.want)
			}
		})
	}
}
