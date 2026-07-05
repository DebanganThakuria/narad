package main

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/netaddr"
)

// clusterAddrMatchesPeer is a test-local alias for the netaddr matcher
// that configPeersToMetastore and advertisedClusterAddr rely on; the
// exhaustive cases below pin down the matching rules serve depends on.
func clusterAddrMatchesPeer(clusterAddr, peerAddr string) bool {
	return netaddr.ClusterAddrMatchesPeer(clusterAddr, peerAddr)
}

func TestClusterAddrMatchesPeer(t *testing.T) {
	cases := []struct {
		name        string
		clusterAddr string
		peerAddr    string
		want        bool
	}{
		{name: "AcceptsHostfulPeerForPortShorthand", clusterAddr: ":9101", peerAddr: "127.0.0.1:9101", want: true},
		{name: "RejectsHostfulPeerDifferentPortForPortShorthand", clusterAddr: ":9101", peerAddr: "127.0.0.1:9102", want: false},
		{name: "ExactAddr", clusterAddr: "127.0.0.1:9101", peerAddr: "127.0.0.1:9101", want: true},
		{name: "RejectsEmptyAddr", clusterAddr: "", peerAddr: "127.0.0.1:9101", want: false},
		{name: "RejectsEmptyPeerAddr", clusterAddr: ":9101", peerAddr: "", want: false},
		{name: "HandlesPortOnlyPeerAddr", clusterAddr: "127.0.0.1:9101", peerAddr: ":9101", want: true},
		{name: "RejectsHostfulMismatch", clusterAddr: "127.0.0.1:9101", peerAddr: "127.0.0.1:9102", want: false},
		{name: "RejectsDifferentHostSamePortWhenHostful", clusterAddr: "127.0.0.1:9101", peerAddr: "127.0.0.2:9101", want: false},
		{name: "AcceptsClusterPortAgainstAdvertisedHostAddr", clusterAddr: ":9101", peerAddr: "narad-0.local:9101", want: true},
		{name: "AcceptsAdvertisedHostAddrAgainstClusterPort", clusterAddr: "narad-0.local:9101", peerAddr: ":9101", want: true},
		{name: "RejectsDifferentPortWithClusterPort", clusterAddr: ":9101", peerAddr: "narad-0.local:9102", want: false},
		{name: "RejectsDifferentPortWithAdvertisedHostAddr", clusterAddr: "narad-0.local:9101", peerAddr: ":9102", want: false},
		{name: "RejectsDifferentHostWhenBothHostful", clusterAddr: "narad-0.local:9101", peerAddr: "narad-1.local:9101", want: false},
		{name: "AcceptsSameHostfulAddr", clusterAddr: "narad-0.local:9101", peerAddr: "narad-0.local:9101", want: true},
		{name: "RejectsWhitespaceOnlyClusterAddr", clusterAddr: "   ", peerAddr: "127.0.0.1:9101", want: false},
		{name: "RejectsWhitespaceOnlyPeerAddr", clusterAddr: ":9101", peerAddr: "   ", want: false},
		{name: "TrimsWhitespace", clusterAddr: " :9101 ", peerAddr: " 127.0.0.1:9101 ", want: true},
		{name: "RejectsDifferentTrimmedPort", clusterAddr: " :9101 ", peerAddr: " 127.0.0.1:9102 ", want: false},
		{name: "RejectsDifferentTrimmedHostfulAddr", clusterAddr: " 127.0.0.1:9101 ", peerAddr: " 127.0.0.2:9101 ", want: false},
		{name: "AcceptsTrimmedExactHostfulAddr", clusterAddr: " 127.0.0.1:9101 ", peerAddr: " 127.0.0.1:9101 ", want: true},
		{name: "RejectsDifferentTrimmedExactHostfulAddr", clusterAddr: " 127.0.0.1:9101 ", peerAddr: " 127.0.0.1:9102 ", want: false},
		{name: "RejectsDifferentTrimmedHostfulHostname", clusterAddr: " narad-0.local:9101 ", peerAddr: " narad-1.local:9101 ", want: false},
		{name: "AcceptsTrimmedClusterPortAgainstHostname", clusterAddr: " :9101 ", peerAddr: " narad-0.local:9101 ", want: true},
		{name: "AcceptsTrimmedHostnameAgainstClusterPort", clusterAddr: " narad-0.local:9101 ", peerAddr: " :9101 ", want: true},
		{name: "RejectsDifferentTrimmedHostnamePort", clusterAddr: " :9101 ", peerAddr: " narad-0.local:9102 ", want: false},
		{name: "RejectsDifferentTrimmedHostnameAgainstClusterPort", clusterAddr: " narad-0.local:9101 ", peerAddr: " :9102 ", want: false},
		{name: "AcceptsTrimmedSameHostname", clusterAddr: " narad-0.local:9101 ", peerAddr: " narad-0.local:9101 ", want: true},
		{name: "RejectsDifferentTrimmedSameHostnamePort", clusterAddr: " narad-0.local:9101 ", peerAddr: " narad-0.local:9102 ", want: false},
		{name: "RejectsDifferentTrimmedHostname", clusterAddr: " narad-0.local:9101 ", peerAddr: " narad-1.local:9101 ", want: false},
		{name: "RejectsDifferentTrimmedHostnameAndPort", clusterAddr: " narad-0.local:9101 ", peerAddr: " narad-1.local:9102 ", want: false},
		{name: "AcceptsLocalhostAgainstClusterPort", clusterAddr: ":9101", peerAddr: "localhost:9101", want: true},
		{name: "RejectsLocalhostDifferentPort", clusterAddr: ":9101", peerAddr: "localhost:9102", want: false},
		{name: "AcceptsIPAddressAgainstClusterPort", clusterAddr: ":9101", peerAddr: "10.0.0.1:9101", want: true},
		{name: "RejectsIPAddressDifferentPort", clusterAddr: ":9101", peerAddr: "10.0.0.1:9102", want: false},
		{name: "AcceptsHostnameAgainstClusterPort", clusterAddr: ":9101", peerAddr: "narad-0.default.svc:9101", want: true},
		{name: "RejectsHostnameDifferentPort", clusterAddr: ":9101", peerAddr: "narad-0.default.svc:9102", want: false},
		{name: "AcceptsHostfulClusterAddrAgainstSamePeerAddr", clusterAddr: "localhost:9101", peerAddr: "localhost:9101", want: true},
		{name: "RejectsHostfulClusterAddrAgainstDifferentPeerAddr", clusterAddr: "localhost:9101", peerAddr: "localhost:9102", want: false},
		{name: "RejectsDifferentHostnameSamePortForHostfulClusterAddr", clusterAddr: "localhost:9101", peerAddr: "narad-0.default.svc:9101", want: false},
		{name: "AcceptsPeerPortAgainstHostfulClusterAddr", clusterAddr: "localhost:9101", peerAddr: ":9101", want: true},
		{name: "RejectsDifferentPeerPortAgainstHostfulClusterAddr", clusterAddr: "localhost:9101", peerAddr: ":9102", want: false},
		{name: "AcceptsHostfulPeerAgainstClusterPortWithHostname", clusterAddr: ":9101", peerAddr: "localhost:9101", want: true},
		{name: "RejectsHostfulPeerAgainstClusterPortWithDifferentPort", clusterAddr: ":9101", peerAddr: "localhost:9102", want: false},
		{name: "RejectsDifferentHostfulClusterAndPeerAddr", clusterAddr: "localhost:9101", peerAddr: "127.0.0.1:9101", want: false},
		{name: "AcceptsIPv6HostfulAddrExact", clusterAddr: "[::1]:9101", peerAddr: "[::1]:9101", want: true},
		{name: "RejectsIPv6HostfulAddrDifferentPort", clusterAddr: "[::1]:9101", peerAddr: "[::1]:9102", want: false},
		{name: "AcceptsClusterPortAgainstIPv6PeerAddr", clusterAddr: ":9101", peerAddr: "[::1]:9101", want: true},
		{name: "RejectsClusterPortAgainstIPv6PeerAddrDifferentPort", clusterAddr: ":9101", peerAddr: "[::1]:9102", want: false},
		{name: "AcceptsIPv6HostfulClusterAddrAgainstClusterPortPeer", clusterAddr: "[::1]:9101", peerAddr: ":9101", want: true},
		{name: "RejectsIPv6HostfulClusterAddrAgainstClusterPortPeerDifferentPort", clusterAddr: "[::1]:9101", peerAddr: ":9102", want: false},
		{name: "RejectsDifferentIPv6HostfulAddrs", clusterAddr: "[::1]:9101", peerAddr: "[::2]:9101", want: false},
		{name: "AcceptsIPv6LoopbackAgainstClusterPort", clusterAddr: ":9101", peerAddr: "[::1]:9101", want: true},
		{name: "RejectsIPv6LoopbackAgainstClusterPortDifferentPort", clusterAddr: ":9101", peerAddr: "[::1]:9102", want: false},
		{name: "RejectsClusterPortPrefixOnlyMismatch", clusterAddr: ":9101", peerAddr: "example.com:19101", want: false},
		{name: "RejectsPeerPortPrefixOnlyMismatch", clusterAddr: "example.com:9101", peerAddr: ":19101", want: false},
		{name: "RejectsShortSuffixMismatch", clusterAddr: ":101", peerAddr: "example.com:9101", want: false},
		{name: "RejectsPeerShortSuffixMismatch", clusterAddr: "example.com:9101", peerAddr: ":101", want: false},
		{name: "AcceptsSamePortShorthand", clusterAddr: ":9101", peerAddr: ":9101", want: true},
		{name: "RejectsDifferentPortShorthand", clusterAddr: ":9101", peerAddr: ":9102", want: false},
		{name: "RejectsWhitespacePortShorthandMismatch", clusterAddr: " :9101 ", peerAddr: " :9102 ", want: false},
		{name: "AcceptsWhitespacePortShorthandMatch", clusterAddr: " :9101 ", peerAddr: " :9101 ", want: true},
		{name: "RejectsWhitespaceHostfulMismatchSamePort", clusterAddr: " localhost:9101 ", peerAddr: " example.com:9101 ", want: false},
		{name: "AcceptsWhitespaceHostfulExactMatch", clusterAddr: " localhost:9101 ", peerAddr: " localhost:9101 ", want: true},
		{name: "RejectsWhitespaceHostfulDifferentPort", clusterAddr: " localhost:9101 ", peerAddr: " localhost:9102 ", want: false},
		{name: "RejectsWhitespaceClusterPortAgainstDifferentHostfulPort", clusterAddr: " :9101 ", peerAddr: " localhost:9102 ", want: false},
		{name: "RejectsWhitespaceHostfulAgainstDifferentClusterPort", clusterAddr: " localhost:9101 ", peerAddr: " :9102 ", want: false},
		{name: "AcceptsWhitespaceClusterPortAgainstHostfulPort", clusterAddr: " :9101 ", peerAddr: " localhost:9101 ", want: true},
		{name: "AcceptsWhitespaceHostfulAgainstClusterPort", clusterAddr: " localhost:9101 ", peerAddr: " :9101 ", want: true},
		{name: "RejectsClusterPortAgainstBarePortText", clusterAddr: ":9101", peerAddr: "9101", want: false},
		{name: "RejectsBarePortTextAgainstClusterPort", clusterAddr: "9101", peerAddr: ":9101", want: false},
		{name: "RejectsBarePortTextAgainstHostfulAddr", clusterAddr: "9101", peerAddr: "localhost:9101", want: false},
		{name: "RejectsHostfulAddrAgainstBarePortText", clusterAddr: "localhost:9101", peerAddr: "9101", want: false},
		{name: "RejectsBareHostTextAgainstClusterPort", clusterAddr: "localhost", peerAddr: ":9101", want: false},
		{name: "RejectsClusterPortAgainstBareHostText", clusterAddr: ":9101", peerAddr: "localhost", want: false},
		{name: "RejectsBareHostTextAgainstHostfulAddr", clusterAddr: "localhost", peerAddr: "localhost:9101", want: false},
		{name: "RejectsHostfulAddrAgainstBareHostText", clusterAddr: "localhost:9101", peerAddr: "localhost", want: false},
		{name: "RejectsSameSuffixButWrongPortBoundary", clusterAddr: ":9101", peerAddr: "host:19101", want: false},
		{name: "RejectsWrongPortBoundaryAgainstClusterPortPeer", clusterAddr: "host:19101", peerAddr: ":9101", want: false},
		{name: "AcceptsDomainHostnameAgainstClusterPort", clusterAddr: ":9101", peerAddr: "narad-0.example.internal:9101", want: true},
		{name: "RejectsDomainHostnameDifferentPortAgainstClusterPort", clusterAddr: ":9101", peerAddr: "narad-0.example.internal:9102", want: false},
		{name: "AcceptsClusterPortAgainstUppercaseHostnameSamePort", clusterAddr: ":9101", peerAddr: "NARAD-0.EXAMPLE.INTERNAL:9101", want: true},
		{name: "RejectsHostfulCaseDifferenceWhenNotPortShorthand", clusterAddr: "narad-0.example.internal:9101", peerAddr: "NARAD-0.EXAMPLE.INTERNAL:9101", want: false},
		{name: "AcceptsIPv4AgainstClusterPortSamePort", clusterAddr: ":9101", peerAddr: "192.168.1.10:9101", want: true},
		{name: "RejectsIPv4AgainstClusterPortDifferentPort", clusterAddr: ":9101", peerAddr: "192.168.1.10:9102", want: false},
		{name: "AcceptsIPv4ClusterAddrAgainstPortPeer", clusterAddr: "192.168.1.10:9101", peerAddr: ":9101", want: true},
		{name: "RejectsIPv4ClusterAddrAgainstDifferentPortPeer", clusterAddr: "192.168.1.10:9101", peerAddr: ":9102", want: false},
		{name: "RejectsDifferentIPv4HostsWhenBothHostful", clusterAddr: "192.168.1.10:9101", peerAddr: "192.168.1.11:9101", want: false},
		{name: "AcceptsSameIPv4HostfulAddr", clusterAddr: "192.168.1.10:9101", peerAddr: "192.168.1.10:9101", want: true},
		{name: "RejectsDifferentIPv4PortSameHost", clusterAddr: "192.168.1.10:9101", peerAddr: "192.168.1.10:9102", want: false},
		{name: "AcceptsLoopbackIPv4AgainstClusterPort", clusterAddr: ":9101", peerAddr: "127.0.0.1:9101", want: true},
		{name: "RejectsLoopbackIPv4DifferentPortAgainstClusterPort", clusterAddr: ":9101", peerAddr: "127.0.0.1:9102", want: false},
		{name: "AcceptsLoopbackIPv4HostfulAgainstClusterPortPeer", clusterAddr: "127.0.0.1:9101", peerAddr: ":9101", want: true},
		{name: "RejectsLoopbackIPv4HostfulAgainstDifferentClusterPortPeer", clusterAddr: "127.0.0.1:9101", peerAddr: ":9102", want: false},
		{name: "RejectsLoopbackIPv4AgainstDifferentHostfulAddr", clusterAddr: "127.0.0.1:9101", peerAddr: "localhost:9101", want: false},
		{name: "RejectsClusterPortAgainstHostfulAddrWithExtraSuffix", clusterAddr: ":9101", peerAddr: "localhost:91010", want: false},
		{name: "RejectsHostfulAddrAgainstClusterPortWithExtraSuffix", clusterAddr: "localhost:91010", peerAddr: ":9101", want: false},
		{name: "AcceptsHostfulAddrAgainstClusterPortWithMatchingSuffixBoundary", clusterAddr: "localhost:9101", peerAddr: ":9101", want: true},
		{name: "AcceptsClusterPortAgainstHostfulAddrWithMatchingSuffixBoundary", clusterAddr: ":9101", peerAddr: "localhost:9101", want: true},
		{name: "RejectsPortSubstringOnlyMatch", clusterAddr: ":9101", peerAddr: "localhost:29101", want: false},
		{name: "RejectsPortSubstringOnlyMatchReverse", clusterAddr: "localhost:29101", peerAddr: ":9101", want: false},
		{name: "AcceptsPeerAddrFromSharedListForClusterPortUseCase", clusterAddr: ":9101", peerAddr: "127.0.0.1:9101", want: true},
		{name: "RejectsWrongSharedListPeerAddrForClusterPortUseCase", clusterAddr: ":9101", peerAddr: "127.0.0.1:9102", want: false},
		{name: "AcceptsSharedListHostnamePeerAddrForClusterPortUseCase", clusterAddr: ":9101", peerAddr: "narad-0.default.svc.cluster.local:9101", want: true},
		{name: "RejectsSharedListHostnamePeerAddrWrongPortForClusterPortUseCase", clusterAddr: ":9101", peerAddr: "narad-0.default.svc.cluster.local:9102", want: false},
		{name: "RejectsHostfulDifferentCaseAgainstHostfulAddr", clusterAddr: "LOCALHOST:9101", peerAddr: "localhost:9101", want: false},
		{name: "AllowsCaseDifferenceWhenUsingClusterPortShorthand", clusterAddr: ":9101", peerAddr: "LOCALHOST:9101", want: true},
		{name: "AllowsCaseDifferenceWhenPeerUsesPortShorthand", clusterAddr: "LOCALHOST:9101", peerAddr: ":9101", want: true},
		{name: "RejectsHostfulDifferentCaseDifferentPort", clusterAddr: "LOCALHOST:9101", peerAddr: "localhost:9102", want: false},
		{name: "RejectsClusterPortShorthandAgainstCaseDifferentWrongPort", clusterAddr: ":9101", peerAddr: "LOCALHOST:9102", want: false},
		{name: "RejectsHostfulAddrAgainstPortShorthandWrongPortWithCaseDifference", clusterAddr: "LOCALHOST:9101", peerAddr: ":9102", want: false},
		{name: "RejectsPortShorthandAgainstLongerPortEnding", clusterAddr: ":9101", peerAddr: "host:99101", want: false},
		{name: "RejectsLongerPortEndingAgainstPortShorthand", clusterAddr: "host:99101", peerAddr: ":9101", want: false},
		{name: "AcceptsPortShorthandAgainstExactPortBoundary", clusterAddr: ":9101", peerAddr: "host:9101", want: true},
		{name: "AcceptsExactPortBoundaryAgainstPortShorthand", clusterAddr: "host:9101", peerAddr: ":9101", want: true},
		{name: "RejectsEmptyAfterTrimHostfulAddr", clusterAddr: "   ", peerAddr: " host:9101 ", want: false},
		{name: "RejectsEmptyAfterTrimPeerAddrHostful", clusterAddr: " host:9101 ", peerAddr: "   ", want: false},
		{name: "RejectsExactHostfulMismatchAfterTrim", clusterAddr: " host-a:9101 ", peerAddr: " host-b:9101 ", want: false},
		{name: "AcceptsExactHostfulMatchAfterTrim", clusterAddr: " host-a:9101 ", peerAddr: " host-a:9101 ", want: true},
		{name: "RejectsExactHostfulDifferentPortAfterTrim", clusterAddr: " host-a:9101 ", peerAddr: " host-a:9102 ", want: false},
		{name: "AcceptsPortShorthandAgainstTrimmedHostfulAddr", clusterAddr: " :9101 ", peerAddr: " host-a:9101 ", want: true},
		{name: "RejectsPortShorthandAgainstTrimmedHostfulDifferentPort", clusterAddr: " :9101 ", peerAddr: " host-a:9102 ", want: false},
		{name: "AcceptsTrimmedHostfulAddrAgainstPortShorthand", clusterAddr: " host-a:9101 ", peerAddr: " :9101 ", want: true},
		{name: "RejectsTrimmedHostfulAddrAgainstDifferentPortShorthand", clusterAddr: " host-a:9101 ", peerAddr: " :9102 ", want: false},
		{name: "RejectsHostfulAgainstSamePortDifferentHostAfterTrim", clusterAddr: " host-a:9101 ", peerAddr: " host-b:9101 ", want: false},
		{name: "AcceptsIPv6TrimmedAgainstPortShorthand", clusterAddr: " [::1]:9101 ", peerAddr: " :9101 ", want: true},
		{name: "RejectsIPv6TrimmedAgainstDifferentPortShorthand", clusterAddr: " [::1]:9101 ", peerAddr: " :9102 ", want: false},
		{name: "AcceptsPortShorthandAgainstTrimmedIPv6", clusterAddr: " :9101 ", peerAddr: " [::1]:9101 ", want: true},
		{name: "RejectsPortShorthandAgainstTrimmedIPv6DifferentPort", clusterAddr: " :9101 ", peerAddr: " [::1]:9102 ", want: false},
		{name: "RejectsTrimmedDifferentIPv6Hosts", clusterAddr: " [::1]:9101 ", peerAddr: " [::2]:9101 ", want: false},
		{name: "AcceptsTrimmedSameIPv6Hosts", clusterAddr: " [::1]:9101 ", peerAddr: " [::1]:9101 ", want: true},
		{name: "RejectsTrimmedSameIPv6HostDifferentPort", clusterAddr: " [::1]:9101 ", peerAddr: " [::1]:9102 ", want: false},
		{name: "RejectsShortPortSuffixFalsePositive", clusterAddr: ":101", peerAddr: "host:1101", want: false},
		{name: "RejectsShortPortSuffixFalsePositiveReverse", clusterAddr: "host:1101", peerAddr: ":101", want: false},
		{name: "AcceptsLongPortSuffixExactMatch", clusterAddr: ":19101", peerAddr: "host:19101", want: true},
		{name: "AcceptsLongPortSuffixExactMatchReverse", clusterAddr: "host:19101", peerAddr: ":19101", want: true},
		{name: "RejectsDifferentLongPortSuffix", clusterAddr: ":19101", peerAddr: "host:29101", want: false},
		{name: "RejectsDifferentLongPortSuffixReverse", clusterAddr: "host:29101", peerAddr: ":19101", want: false},
		{name: "AcceptsPortOnlyMatchForIPv4HostfulClusterAddr", clusterAddr: "192.168.1.10:9101", peerAddr: ":9101", want: true},
		{name: "RejectsDifferentPortOnlyMatchForIPv4HostfulClusterAddr", clusterAddr: "192.168.1.10:9101", peerAddr: ":9102", want: false},
		{name: "AcceptsPortOnlyMatchForIPv6HostfulClusterAddr", clusterAddr: "[::1]:9101", peerAddr: ":9101", want: true},
		{name: "RejectsDifferentPortOnlyMatchForIPv6HostfulClusterAddr", clusterAddr: "[::1]:9101", peerAddr: ":9102", want: false},
		{name: "AcceptsPortOnlyMatchForHostnameClusterAddr", clusterAddr: "narad-0.default.svc:9101", peerAddr: ":9101", want: true},
		{name: "RejectsDifferentPortOnlyMatchForHostnameClusterAddr", clusterAddr: "narad-0.default.svc:9101", peerAddr: ":9102", want: false},
		{name: "RejectsPortOnlyNearMatchForHostfulClusterAddr", clusterAddr: "narad-0.default.svc:9101", peerAddr: ":19101", want: false},
		{name: "RejectsPortOnlyNearMatchForClusterPortAddr", clusterAddr: ":9101", peerAddr: "host:19101", want: false},
		{name: "RejectsTrimmedNearMatchPortOnlyForHostfulClusterAddr", clusterAddr: " narad-0.default.svc:9101 ", peerAddr: " :19101 ", want: false},
		{name: "RejectsTrimmedNearMatchHostfulForClusterPortAddr", clusterAddr: " :9101 ", peerAddr: " host:19101 ", want: false},
		{name: "AcceptsPortSuffixMatchForPortShorthand", clusterAddr: ":9101", peerAddr: "example.com:9101", want: true},
		{name: "RejectsPortSuffixMatchWhenBothHostful", clusterAddr: "example.com:9101", peerAddr: "other.com:9101", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := clusterAddrMatchesPeer(tc.clusterAddr, tc.peerAddr); got != tc.want {
				t.Fatalf("clusterAddrMatchesPeer(%q, %q) = %v, want %v", tc.clusterAddr, tc.peerAddr, got, tc.want)
			}
		})
	}
}

func TestConfigPeersToMetastoreRemovesLocalVoter(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}, {ID: "node-3", Addr: "127.0.0.1:9103"}})
	if len(got) != 2 {
		t.Fatalf("len(configPeersToMetastore()) = %d, want 2", len(got))
	}
	if got[0].ID != "node-2" || got[0].Addr != "127.0.0.1:9102" {
		t.Fatalf("peer[0] = %+v", got[0])
	}
	if got[1].ID != "node-3" || got[1].Addr != "127.0.0.1:9103" {
		t.Fatalf("peer[1] = %+v", got[1])
	}
}

func TestConfigPeersToMetastoreRemovesLocalVoterForClusterPortAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}, {ID: "node-3", Addr: "127.0.0.1:9103"}})
	if len(got) != 2 {
		t.Fatalf("len(configPeersToMetastore()) = %d, want 2", len(got))
	}
	if got[0].ID != "node-2" || got[0].Addr != "127.0.0.1:9102" {
		t.Fatalf("peer[0] = %+v", got[0])
	}
	if got[1].ID != "node-3" || got[1].Addr != "127.0.0.1:9103" {
		t.Fatalf("peer[1] = %+v", got[1])
	}
}

func TestAdvertisedClusterAddrUsesPeerAddrForClusterPort(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}, {ID: "node-3", Addr: "127.0.0.1:9103"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q, want %q", got, "127.0.0.1:9101")
	}
}

func TestAdvertisedClusterAddrFallsBackToClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}, {ID: "node-3", Addr: "127.0.0.1:9103"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q, want %q", got, ":9101")
	}
}

func TestAdvertisedMemberAddrUsesHTTPAddrWhenHostful(t *testing.T) {
	got := advertisedMemberAddr("node-1", "127.0.0.1:7942", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}})
	if got != "127.0.0.1:7942" {
		t.Fatalf("advertisedMemberAddr() = %q, want %q", got, "127.0.0.1:7942")
	}
}

func TestAdvertisedMemberAddrUsesClusterHostForPortOnlyHTTPAddr(t *testing.T) {
	got := advertisedMemberAddr("node-1", ":7942", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}})
	if got != "127.0.0.1:7942" {
		t.Fatalf("advertisedMemberAddr() = %q, want %q", got, "127.0.0.1:7942")
	}
}

func TestAdvertisedMemberAddrUsesPeerHostForPortOnlyClusterAddr(t *testing.T) {
	got := advertisedMemberAddr("node-1", ":7942", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.local:9101"}})
	if got != "narad-0.local:7942" {
		t.Fatalf("advertisedMemberAddr() = %q, want %q", got, "narad-0.local:7942")
	}
}

func TestAdvertisedMemberAddrHandlesIPv6ClusterHost(t *testing.T) {
	got := advertisedMemberAddr("node-1", ":7942", "[::1]:9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9101"}})
	if got != "[::1]:7942" {
		t.Fatalf("advertisedMemberAddr() = %q, want %q", got, "[::1]:7942")
	}
}

func TestAdvertisedMemberAddrFallsBackWhenClusterHostCannotBeResolved(t *testing.T) {
	got := advertisedMemberAddr("node-1", ":7942", ":9101", nil)
	if got != ":7942" {
		t.Fatalf("advertisedMemberAddr() = %q, want %q", got, ":7942")
	}
}

func TestAdvertisedClusterAddrAcceptsAdvertisedAddrForClusterPortUseCase(t *testing.T) {
	if advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}}) != "127.0.0.1:9101" {
		t.Fatal("advertisedClusterAddr() did not return peer addr")
	}
}

func TestAdvertisedClusterAddrFallsBackToClusterAddrWhenPeerMissing(t *testing.T) {
	if advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}}) != ":9101" {
		t.Fatal("advertisedClusterAddr() did not fall back to cluster addr")
	}
}

func TestConfigPeersToMetastoreRemovesLocalPeerForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestConfigPeersToMetastoreKeepsRemotePeersForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}, {ID: "node-3", Addr: "127.0.0.1:9103"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrAcceptsExactPeerAddrForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrFallsBackForHostfulClusterAddrWhenPeerMissing(t *testing.T) {
	got := advertisedClusterAddr("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRemovesLocalPeerForExactHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrRejectsNearMatchButWrongPortInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:19101"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsNearMatchButWrongPortInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:19101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrRejectsDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsDifferentNodeIDInPeerRemovalEvenWhenAddrMatches(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrAcceptsWhitespaceInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}})
	if got != " :9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreAcceptsWhitespaceInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrRejectsWhitespaceWrongPortInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}})
	if got != " :9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsWhitespaceWrongPortInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrAcceptsExactWhitespaceHostfulMatchInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}})
	if got != " 127.0.0.1:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreAcceptsExactWhitespaceHostfulMatchInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrRejectsHostfulWhitespaceDifferentHostInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.2:9101 "}})
	if got != " 127.0.0.1:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsHostfulWhitespaceDifferentHostInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.2:9101 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrRejectsHostfulWhitespaceDifferentPortInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}})
	if got != " 127.0.0.1:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsHostfulWhitespaceDifferentPortInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrAcceptsHostnameAdvertiseAddrForClusterPortUseCase(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9101"}})
	if got != "narad-0.default.svc.cluster.local:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRemovesHostnameAdvertiseAddrForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9101"}, {ID: "node-2", Addr: "narad-1.default.svc.cluster.local:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrRejectsHostnameAdvertiseAddrWrongPortForClusterPortUseCase(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreKeepsHostnameAdvertiseAddrWrongPortForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9102"}, {ID: "node-2", Addr: "narad-1.default.svc.cluster.local:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrHandlesNilPeerListInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", nil)
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreHandlesNilPeerListInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", nil)
	if got != nil {
		t.Fatalf("configPeersToMetastore() = %+v, want nil", got)
	}
}

func TestAdvertisedClusterAddrHandlesEmptyPeerListInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreHandlesEmptyPeerListInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{})
	if got != nil {
		t.Fatalf("configPeersToMetastore() = %+v, want nil", got)
	}
}

func TestAdvertisedClusterAddrUsesFirstMatchingPeerForAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-1", Addr: "localhost:9101"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRemovesOnlyMatchingLocalPeer(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-1", Addr: "127.0.0.1:9102"}, {ID: "node-2", Addr: "127.0.0.1:9103"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrRejectsMatchingAddrForDifferentNodeIDInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9101"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreKeepsMatchingAddrForDifferentNodeIDInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9101"}, {ID: "node-3", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrAcceptsExactMatchForAdvertisedAddrWhenClusterAddrAlreadyHostful(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: "localhost:9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrFallsBackWhenClusterAddrHostfulButPeerUsesPortOnly(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRemovesPeerWhenClusterAddrHostfulAndPeerUsesPortOnly(t *testing.T) {
	got := configPeersToMetastore("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}, {ID: "node-2", Addr: "localhost:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestConfigPeersToMetastoreRejectsPeerWhenClusterAddrHostfulAndPeerUsesDifferentPortOnly(t *testing.T) {
	got := configPeersToMetastore("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}, {ID: "node-2", Addr: "localhost:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrAdvertisedLookupUsesPortOnlyPeerWhenClusterAddrHostful(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrAdvertisedLookupRejectsDifferentPortOnlyPeerWhenClusterAddrHostful(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsPeerRemovalForDifferentNodeIDEvenWithPortOnlyMatch(t *testing.T) {
	got := configPeersToMetastore("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-x", Addr: ":9101"}, {ID: "node-2", Addr: "localhost:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrRejectsAdvertisedLookupForDifferentNodeIDEvenWithPortOnlyMatch(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-x", Addr: ":9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreAcceptsPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrAcceptsPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}})
	if got != "narad-0.default.svc:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrRejectsDifferentPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}})
	if got != "narad-0.default.svc:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsDifferentPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreAcceptsTrimmedPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9101 "}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrAcceptsTrimmedPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9101 "}})
	if got != " narad-0.default.svc:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrRejectsTrimmedDifferentPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9102 "}})
	if got != " narad-0.default.svc:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsTrimmedDifferentPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9102 "}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreAcceptsHostfulPeerRemovalForClusterPortUseCaseWithHostname(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9101"}, {ID: "node-2", Addr: "host:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrAcceptsHostfulAdvertisedLookupForClusterPortUseCaseWithHostname(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9101"}})
	if got != "host:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrRejectsHostfulDifferentPortAdvertisedLookupForClusterPortUseCaseWithHostname(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsHostfulDifferentPortPeerRemovalForClusterPortUseCaseWithHostname(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9102"}, {ID: "node-2", Addr: "host:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreAcceptsHostfulPeerRemovalForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9101"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrAcceptsHostfulAdvertisedLookupForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9101"}})
	if got != "[::1]:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrRejectsHostfulDifferentPortAdvertisedLookupForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsHostfulDifferentPortPeerRemovalForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9102"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreAcceptsHostfulPeerRemovalForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9101"}, {ID: "node-2", Addr: "10.0.0.2:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestAdvertisedClusterAddrAcceptsHostfulAdvertisedLookupForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9101"}})
	if got != "10.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestAdvertisedClusterAddrRejectsHostfulDifferentPortAdvertisedLookupForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreRejectsHostfulDifferentPortPeerRemovalForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9102"}, {ID: "node-2", Addr: "10.0.0.2:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreRemovesLocalPeerForExactHostfulAddrHostname(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.local:9101"}, {ID: "node-2", Addr: "narad-1.local:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestConfigPeersToMetastoreRemovesLocalPeerForExactHostfulAddrIPv6(t *testing.T) {
	got := configPeersToMetastore("node-1", "[::1]:9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9101"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestConfigPeersToMetastoreKeepsDifferentHostSamePortForExactHostfulAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.2:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreKeepsDifferentHostSamePortForExactHostnameAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-1.local:9101"}, {ID: "node-2", Addr: "narad-1.local:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreKeepsDifferentHostSamePortForExactIPv6Addr(t *testing.T) {
	got := configPeersToMetastore("node-1", "[::1]:9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::2]:9101"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrRejectsExactHostfulDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreKeepsExactHostfulDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrRejectsExactHostnameDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-x", Addr: "narad-0.local:9101"}})
	if got != "narad-0.local:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreKeepsExactHostnameDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-x", Addr: "narad-0.local:9101"}, {ID: "node-2", Addr: "narad-1.local:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestAdvertisedClusterAddrRejectsExactIPv6DifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", "[::1]:9101", []config.ClusterPeer{{ID: "node-x", Addr: "[::1]:9101"}})
	if got != "[::1]:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestConfigPeersToMetastoreKeepsExactIPv6DifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := configPeersToMetastore("node-1", "[::1]:9101", []config.ClusterPeer{{ID: "node-x", Addr: "[::1]:9101"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestConfigPeersToMetastoreReturnsAllWhenLocalVoterMissing(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}, {ID: "node-3", Addr: "127.0.0.1:9103"}})
	if len(got) != 2 {
		t.Fatalf("len(configPeersToMetastore()) = %d, want 2", len(got))
	}
}
