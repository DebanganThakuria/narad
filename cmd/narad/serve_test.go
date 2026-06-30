package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

func TestServeFlagsApplyTo(t *testing.T) {
	cfg := config.Default()
	flags := serveFlags{
		port:        9001,
		addr:        "127.0.0.1:9002",
		clusterPort: 9100,
		nodeID:      "node-a",
		dataDir:     "/tmp/narad-data",
		logLevel:    "debug",
		logFormat:   "json",
		pprofAddr:   "127.0.0.1:6060",
	}

	flags.applyTo(cfg)

	if cfg.HTTP.Addr != "127.0.0.1:9002" {
		t.Fatalf("HTTP.Addr = %q, want %q", cfg.HTTP.Addr, "127.0.0.1:9002")
	}
	if cfg.Cluster.Addr != ":9100" {
		t.Fatalf("Cluster.Addr = %q, want %q", cfg.Cluster.Addr, ":9100")
	}
	if cfg.Cluster.NodeID != "node-a" {
		t.Fatalf("Cluster.NodeID = %q, want %q", cfg.Cluster.NodeID, "node-a")
	}
	if cfg.Storage.DataDir != "/tmp/narad-data" {
		t.Fatalf("DataDir = %q, want %q", cfg.Storage.DataDir, "/tmp/narad-data")
	}
	if cfg.Log.Level != "debug" || cfg.Log.Format != "json" {
		t.Fatalf("Log config = %+v", cfg.Log)
	}
	if cfg.HTTP.PprofAddr != "127.0.0.1:6060" {
		t.Fatalf("HTTP.PprofAddr = %q, want %q", cfg.HTTP.PprofAddr, "127.0.0.1:6060")
	}
}

func TestLoadServeConfigReturnsNilOnHelp(t *testing.T) {
	cfg, err := loadServeConfig([]string{"-help"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg != nil {
		t.Fatalf("loadServeConfig() cfg = %+v, want nil", cfg)
	}
}

func TestLoadServeConfigAppliesFlags(t *testing.T) {
	cfg, err := loadServeConfig([]string{"--addr", "127.0.0.1:9123", "--cluster-port", "9456", "--log-level", "debug", "--pprof-addr", "127.0.0.1:6060"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9123" {
		t.Fatalf("HTTP.Addr = %q, want %q", cfg.HTTP.Addr, "127.0.0.1:9123")
	}
	if cfg.Cluster.Addr != ":9456" {
		t.Fatalf("Cluster.Addr = %q, want %q", cfg.Cluster.Addr, ":9456")
	}
	if cfg.Log.Level != "debug" {
		t.Fatalf("Log.Level = %q, want %q", cfg.Log.Level, "debug")
	}
	if cfg.HTTP.PprofAddr != "127.0.0.1:6060" {
		t.Fatalf("HTTP.PprofAddr = %q, want %q", cfg.HTTP.PprofAddr, "127.0.0.1:6060")
	}
}

func TestLoadServeConfigLoadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "narad.json")
	if err := os.WriteFile(path, []byte(`{"http":{"addr":"127.0.0.1:8111"},"cluster":{"addr":"127.0.0.1:8112"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := loadServeConfig([]string{"--config", path})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:8111" || cfg.Cluster.Addr != "127.0.0.1:8112" {
		t.Fatalf("loadServeConfig() cfg = %+v", cfg)
	}
}

func TestResolveNodeIDUsesConfigOverride(t *testing.T) {
	nodeID, err := resolveNodeID(&config.Config{Cluster: config.ClusterConfig{NodeID: "node-a"}})
	if err != nil {
		t.Fatalf("resolveNodeID() error = %v", err)
	}
	if nodeID != "node-a" {
		t.Fatalf("resolveNodeID() = %q, want %q", nodeID, "node-a")
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

func TestClusterAddrMatchesPeer(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "127.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
	if clusterAddrMatchesPeer(":9101", "127.0.0.1:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerExactAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("127.0.0.1:9101", "127.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsEmptyAddr(t *testing.T) {
	if clusterAddrMatchesPeer("", "127.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsEmptyPeerAddr(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerHandlesPortOnlyPeerAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("127.0.0.1:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulMismatch(t *testing.T) {
	if clusterAddrMatchesPeer("127.0.0.1:9101", "127.0.0.1:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentHostSamePortWhenHostful(t *testing.T) {
	if clusterAddrMatchesPeer("127.0.0.1:9101", "127.0.0.2:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsClusterPortAgainstAdvertisedHostAddr(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "narad-0.local:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerAcceptsAdvertisedHostAddrAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer("narad-0.local:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortWithClusterPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "narad-0.local:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortWithAdvertisedHostAddr(t *testing.T) {
	if clusterAddrMatchesPeer("narad-0.local:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentHostWhenBothHostful(t *testing.T) {
	if clusterAddrMatchesPeer("narad-0.local:9101", "narad-1.local:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsSameHostfulAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("narad-0.local:9101", "narad-0.local:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceOnlyClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer("   ", "127.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceOnlyPeerAddr(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "   ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerTrimsWhitespace(t *testing.T) {
	if !clusterAddrMatchesPeer(" :9101 ", " 127.0.0.1:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedPort(t *testing.T) {
	if clusterAddrMatchesPeer(" :9101 ", " 127.0.0.1:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedHostfulAddr(t *testing.T) {
	if clusterAddrMatchesPeer(" 127.0.0.1:9101 ", " 127.0.0.2:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedExactHostfulAddr(t *testing.T) {
	if !clusterAddrMatchesPeer(" 127.0.0.1:9101 ", " 127.0.0.1:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedExactHostfulAddr(t *testing.T) {
	if clusterAddrMatchesPeer(" 127.0.0.1:9101 ", " 127.0.0.1:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedHostfulHostname(t *testing.T) {
	if clusterAddrMatchesPeer(" narad-0.local:9101 ", " narad-1.local:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedClusterPortAgainstHostname(t *testing.T) {
	if !clusterAddrMatchesPeer(" :9101 ", " narad-0.local:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedHostnameAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(" narad-0.local:9101 ", " :9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedHostnamePort(t *testing.T) {
	if clusterAddrMatchesPeer(" :9101 ", " narad-0.local:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedHostnameAgainstClusterPort(t *testing.T) {
	if clusterAddrMatchesPeer(" narad-0.local:9101 ", " :9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedSameHostname(t *testing.T) {
	if !clusterAddrMatchesPeer(" narad-0.local:9101 ", " narad-0.local:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedSameHostnamePort(t *testing.T) {
	if clusterAddrMatchesPeer(" narad-0.local:9101 ", " narad-0.local:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedHostname(t *testing.T) {
	if clusterAddrMatchesPeer(" narad-0.local:9101 ", " narad-1.local:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentTrimmedHostnameAndPort(t *testing.T) {
	if clusterAddrMatchesPeer(" narad-0.local:9101 ", " narad-1.local:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsLocalhostAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsLocalhostDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "localhost:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsIPAddressAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "10.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsIPAddressDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "10.0.0.1:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsHostnameAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "narad-0.default.svc:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsHostnameDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "narad-0.default.svc:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulClusterAddrAgainstSamePeerAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("localhost:9101", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulClusterAddrAgainstDifferentPeerAddr(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:9101", "localhost:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentHostnameSamePortForHostfulClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:9101", "narad-0.default.svc:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPeerPortAgainstHostfulClusterAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("localhost:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPeerPortAgainstHostfulClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulPeerAgainstClusterPortWithHostname(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulPeerAgainstClusterPortWithDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "localhost:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentHostfulClusterAndPeerAddr(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:9101", "127.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsIPv6HostfulAddrExact(t *testing.T) {
	if !clusterAddrMatchesPeer("[::1]:9101", "[::1]:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsIPv6HostfulAddrDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer("[::1]:9101", "[::1]:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsClusterPortAgainstIPv6PeerAddr(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "[::1]:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsClusterPortAgainstIPv6PeerAddrDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "[::1]:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsIPv6HostfulClusterAddrAgainstClusterPortPeer(t *testing.T) {
	if !clusterAddrMatchesPeer("[::1]:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsIPv6HostfulClusterAddrAgainstClusterPortPeerDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer("[::1]:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentIPv6HostfulAddrs(t *testing.T) {
	if clusterAddrMatchesPeer("[::1]:9101", "[::2]:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsIPv6LoopbackAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "[::1]:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsIPv6LoopbackAgainstClusterPortDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "[::1]:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPortSuffixMatchOnlyForPortShorthand(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "example.com:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
	if clusterAddrMatchesPeer("example.com:9101", "other.com:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsClusterPortPrefixOnlyMismatch(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "example.com:19101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsPeerPortPrefixOnlyMismatch(t *testing.T) {
	if clusterAddrMatchesPeer("example.com:9101", ":19101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsShortSuffixMismatch(t *testing.T) {
	if clusterAddrMatchesPeer(":101", "example.com:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsPeerShortSuffixMismatch(t *testing.T) {
	if clusterAddrMatchesPeer("example.com:9101", ":101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsSamePortShorthand(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortShorthand(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespacePortShorthandMismatch(t *testing.T) {
	if clusterAddrMatchesPeer(" :9101 ", " :9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsWhitespacePortShorthandMatch(t *testing.T) {
	if !clusterAddrMatchesPeer(" :9101 ", " :9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceHostfulMismatchSamePort(t *testing.T) {
	if clusterAddrMatchesPeer(" localhost:9101 ", " example.com:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsWhitespaceHostfulExactMatch(t *testing.T) {
	if !clusterAddrMatchesPeer(" localhost:9101 ", " localhost:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceHostfulDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(" localhost:9101 ", " localhost:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceClusterPortAgainstDifferentHostfulPort(t *testing.T) {
	if clusterAddrMatchesPeer(" :9101 ", " localhost:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceHostfulAgainstDifferentClusterPort(t *testing.T) {
	if clusterAddrMatchesPeer(" localhost:9101 ", " :9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsWhitespaceClusterPortAgainstHostfulPort(t *testing.T) {
	if !clusterAddrMatchesPeer(" :9101 ", " localhost:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerAcceptsWhitespaceHostfulAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(" localhost:9101 ", " :9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsClusterPortAgainstBarePortText(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsBarePortTextAgainstClusterPort(t *testing.T) {
	if clusterAddrMatchesPeer("9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsBarePortTextAgainstHostfulAddr(t *testing.T) {
	if clusterAddrMatchesPeer("9101", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulAddrAgainstBarePortText(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:9101", "9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsBareHostTextAgainstClusterPort(t *testing.T) {
	if clusterAddrMatchesPeer("localhost", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsClusterPortAgainstBareHostText(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "localhost") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsBareHostTextAgainstHostfulAddr(t *testing.T) {
	if clusterAddrMatchesPeer("localhost", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulAddrAgainstBareHostText(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:9101", "localhost") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsSameSuffixButWrongPortBoundary(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "host:19101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsWrongPortBoundaryAgainstClusterPortPeer(t *testing.T) {
	if clusterAddrMatchesPeer("host:19101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsDomainHostnameAgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "narad-0.example.internal:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDomainHostnameDifferentPortAgainstClusterPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "narad-0.example.internal:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsClusterPortAgainstUppercaseHostnameSamePort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "NARAD-0.EXAMPLE.INTERNAL:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulCaseDifferenceWhenNotPortShorthand(t *testing.T) {
	if clusterAddrMatchesPeer("narad-0.example.internal:9101", "NARAD-0.EXAMPLE.INTERNAL:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsIPv4AgainstClusterPortSamePort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "192.168.1.10:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsIPv4AgainstClusterPortDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "192.168.1.10:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsIPv4ClusterAddrAgainstPortPeer(t *testing.T) {
	if !clusterAddrMatchesPeer("192.168.1.10:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsIPv4ClusterAddrAgainstDifferentPortPeer(t *testing.T) {
	if clusterAddrMatchesPeer("192.168.1.10:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentIPv4HostsWhenBothHostful(t *testing.T) {
	if clusterAddrMatchesPeer("192.168.1.10:9101", "192.168.1.11:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsSameIPv4HostfulAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("192.168.1.10:9101", "192.168.1.10:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentIPv4PortSameHost(t *testing.T) {
	if clusterAddrMatchesPeer("192.168.1.10:9101", "192.168.1.10:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsLoopbackIPv4AgainstClusterPort(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "127.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsLoopbackIPv4DifferentPortAgainstClusterPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "127.0.0.1:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsLoopbackIPv4HostfulAgainstClusterPortPeer(t *testing.T) {
	if !clusterAddrMatchesPeer("127.0.0.1:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsLoopbackIPv4HostfulAgainstDifferentClusterPortPeer(t *testing.T) {
	if clusterAddrMatchesPeer("127.0.0.1:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsLoopbackIPv4AgainstDifferentHostfulAddr(t *testing.T) {
	if clusterAddrMatchesPeer("127.0.0.1:9101", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsClusterPortAgainstHostfulAddrWithExtraSuffix(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "localhost:91010") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulAddrAgainstClusterPortWithExtraSuffix(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:91010", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulAddrAgainstClusterPortWithMatchingSuffixBoundary(t *testing.T) {
	if !clusterAddrMatchesPeer("localhost:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerAcceptsClusterPortAgainstHostfulAddrWithMatchingSuffixBoundary(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsPortSubstringOnlyMatch(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "localhost:29101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsPortSubstringOnlyMatchReverse(t *testing.T) {
	if clusterAddrMatchesPeer("localhost:29101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPeerAddrFromSharedListForClusterPortUseCase(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "127.0.0.1:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsWrongSharedListPeerAddrForClusterPortUseCase(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "127.0.0.1:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsSharedListHostnamePeerAddrForClusterPortUseCase(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "narad-0.default.svc.cluster.local:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsSharedListHostnamePeerAddrWrongPortForClusterPortUseCase(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "narad-0.default.svc.cluster.local:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsAdvertisedAddrForClusterPortUseCase(t *testing.T) {
	if advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}}) != "127.0.0.1:9101" {
		t.Fatal("advertisedClusterAddr() did not return peer addr")
	}
}

func TestClusterAddrMatchesPeerFallsBackToClusterAddrWhenPeerMissing(t *testing.T) {
	if advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}}) != ":9101" {
		t.Fatal("advertisedClusterAddr() did not fall back to cluster addr")
	}
}

func TestClusterAddrMatchesPeerRemovesLocalPeerForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerKeepsRemotePeersForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}, {ID: "node-3", Addr: "127.0.0.1:9103"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerAcceptsExactPeerAddrForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerFallsBackForHostfulClusterAddrWhenPeerMissing(t *testing.T) {
	got := advertisedClusterAddr("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9102"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRemovesLocalPeerForExactHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerRejectsNearMatchButWrongPortInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:19101"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsNearMatchButWrongPortInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:19101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentNodeIDInPeerRemovalEvenWhenAddrMatches(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerAcceptsWhitespaceInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}})
	if got != " :9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsWhitespaceInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceWrongPortInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}})
	if got != " :9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsWhitespaceWrongPortInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " :9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerAcceptsExactWhitespaceHostfulMatchInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}})
	if got != " 127.0.0.1:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsExactWhitespaceHostfulMatchInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9101 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulWhitespaceDifferentHostInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.2:9101 "}})
	if got != " 127.0.0.1:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulWhitespaceDifferentHostInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.2:9101 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulWhitespaceDifferentPortInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}})
	if got != " 127.0.0.1:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulWhitespaceDifferentPortInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", " 127.0.0.1:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " 127.0.0.1:9102 "}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerAcceptsHostnameAdvertiseAddrForClusterPortUseCase(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9101"}})
	if got != "narad-0.default.svc.cluster.local:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRemovesHostnameAdvertiseAddrForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9101"}, {ID: "node-2", Addr: "narad-1.default.svc.cluster.local:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostnameAdvertiseAddrWrongPortForClusterPortUseCase(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerKeepsHostnameAdvertiseAddrWrongPortForClusterPortUseCase(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.default.svc.cluster.local:9102"}, {ID: "node-2", Addr: "narad-1.default.svc.cluster.local:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerHandlesNilPeerListInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", nil)
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerHandlesNilPeerListInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", nil)
	if got != nil {
		t.Fatalf("configPeersToMetastore() = %+v, want nil", got)
	}
}

func TestClusterAddrMatchesPeerHandlesEmptyPeerListInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerHandlesEmptyPeerListInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{})
	if got != nil {
		t.Fatalf("configPeersToMetastore() = %+v, want nil", got)
	}
}

func TestClusterAddrMatchesPeerUsesFirstMatchingPeerForAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-1", Addr: "localhost:9101"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRemovesOnlyMatchingLocalPeer(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.1:9101"}, {ID: "node-1", Addr: "127.0.0.1:9102"}, {ID: "node-2", Addr: "127.0.0.1:9103"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsMatchingAddrForDifferentNodeIDInAdvertisedLookup(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9101"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerKeepsMatchingAddrForDifferentNodeIDInPeerRemoval(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-2", Addr: "127.0.0.1:9101"}, {ID: "node-3", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentCaseAgainstHostfulAddr(t *testing.T) {
	if clusterAddrMatchesPeer("LOCALHOST:9101", "localhost:9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAllowsCaseDifferenceWhenUsingClusterPortShorthand(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "LOCALHOST:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerAllowsCaseDifferenceWhenPeerUsesPortShorthand(t *testing.T) {
	if !clusterAddrMatchesPeer("LOCALHOST:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentCaseDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer("LOCALHOST:9101", "localhost:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsClusterPortShorthandAgainstCaseDifferentWrongPort(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "LOCALHOST:9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulAddrAgainstPortShorthandWrongPortWithCaseDifference(t *testing.T) {
	if clusterAddrMatchesPeer("LOCALHOST:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsPortShorthandAgainstLongerPortEnding(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "host:99101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsLongerPortEndingAgainstPortShorthand(t *testing.T) {
	if clusterAddrMatchesPeer("host:99101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPortShorthandAgainstExactPortBoundary(t *testing.T) {
	if !clusterAddrMatchesPeer(":9101", "host:9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerAcceptsExactPortBoundaryAgainstPortShorthand(t *testing.T) {
	if !clusterAddrMatchesPeer("host:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsEmptyAfterTrimHostfulAddr(t *testing.T) {
	if clusterAddrMatchesPeer("   ", " host:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsEmptyAfterTrimPeerAddrHostful(t *testing.T) {
	if clusterAddrMatchesPeer(" host:9101 ", "   ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsExactHostfulMismatchAfterTrim(t *testing.T) {
	if clusterAddrMatchesPeer(" host-a:9101 ", " host-b:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsExactHostfulMatchAfterTrim(t *testing.T) {
	if !clusterAddrMatchesPeer(" host-a:9101 ", " host-a:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsExactHostfulDifferentPortAfterTrim(t *testing.T) {
	if clusterAddrMatchesPeer(" host-a:9101 ", " host-a:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPortShorthandAgainstTrimmedHostfulAddr(t *testing.T) {
	if !clusterAddrMatchesPeer(" :9101 ", " host-a:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsPortShorthandAgainstTrimmedHostfulDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(" :9101 ", " host-a:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedHostfulAddrAgainstPortShorthand(t *testing.T) {
	if !clusterAddrMatchesPeer(" host-a:9101 ", " :9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsTrimmedHostfulAddrAgainstDifferentPortShorthand(t *testing.T) {
	if clusterAddrMatchesPeer(" host-a:9101 ", " :9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulAgainstSamePortDifferentHostAfterTrim(t *testing.T) {
	if clusterAddrMatchesPeer(" host-a:9101 ", " host-b:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsIPv6TrimmedAgainstPortShorthand(t *testing.T) {
	if !clusterAddrMatchesPeer(" [::1]:9101 ", " :9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsIPv6TrimmedAgainstDifferentPortShorthand(t *testing.T) {
	if clusterAddrMatchesPeer(" [::1]:9101 ", " :9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPortShorthandAgainstTrimmedIPv6(t *testing.T) {
	if !clusterAddrMatchesPeer(" :9101 ", " [::1]:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsPortShorthandAgainstTrimmedIPv6DifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(" :9101 ", " [::1]:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsTrimmedDifferentIPv6Hosts(t *testing.T) {
	if clusterAddrMatchesPeer(" [::1]:9101 ", " [::2]:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedSameIPv6Hosts(t *testing.T) {
	if !clusterAddrMatchesPeer(" [::1]:9101 ", " [::1]:9101 ") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsTrimmedSameIPv6HostDifferentPort(t *testing.T) {
	if clusterAddrMatchesPeer(" [::1]:9101 ", " [::1]:9102 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsShortPortSuffixFalsePositive(t *testing.T) {
	if clusterAddrMatchesPeer(":101", "host:1101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsShortPortSuffixFalsePositiveReverse(t *testing.T) {
	if clusterAddrMatchesPeer("host:1101", ":101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsLongPortSuffixExactMatch(t *testing.T) {
	if !clusterAddrMatchesPeer(":19101", "host:19101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerAcceptsLongPortSuffixExactMatchReverse(t *testing.T) {
	if !clusterAddrMatchesPeer("host:19101", ":19101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentLongPortSuffix(t *testing.T) {
	if clusterAddrMatchesPeer(":19101", "host:29101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentLongPortSuffixReverse(t *testing.T) {
	if clusterAddrMatchesPeer("host:29101", ":19101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsExactMatchForAdvertisedAddrWhenClusterAddrAlreadyHostful(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: "localhost:9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerFallsBackWhenClusterAddrHostfulButPeerUsesPortOnly(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRemovesPeerWhenClusterAddrHostfulAndPeerUsesPortOnly(t *testing.T) {
	got := configPeersToMetastore("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}, {ID: "node-2", Addr: "localhost:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerRejectsPeerWhenClusterAddrHostfulAndPeerUsesDifferentPortOnly(t *testing.T) {
	got := configPeersToMetastore("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}, {ID: "node-2", Addr: "localhost:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerAdvertisedLookupUsesPortOnlyPeerWhenClusterAddrHostful(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAdvertisedLookupRejectsDifferentPortOnlyPeerWhenClusterAddrHostful(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsPeerRemovalForDifferentNodeIDEvenWithPortOnlyMatch(t *testing.T) {
	got := configPeersToMetastore("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-x", Addr: ":9101"}, {ID: "node-2", Addr: "localhost:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsAdvertisedLookupForDifferentNodeIDEvenWithPortOnlyMatch(t *testing.T) {
	got := advertisedClusterAddr("node-1", "localhost:9101", []config.ClusterPeer{{ID: "node-x", Addr: ":9101"}})
	if got != "localhost:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsPortOnlyMatchForIPv4HostfulClusterAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("192.168.1.10:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortOnlyMatchForIPv4HostfulClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer("192.168.1.10:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPortOnlyMatchForIPv6HostfulClusterAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("[::1]:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortOnlyMatchForIPv6HostfulClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer("[::1]:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPortOnlyMatchForHostnameClusterAddr(t *testing.T) {
	if !clusterAddrMatchesPeer("narad-0.default.svc:9101", ":9101") {
		t.Fatal("clusterAddrMatchesPeer() = false, want true")
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortOnlyMatchForHostnameClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer("narad-0.default.svc:9101", ":9102") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9101"}})
	if got != "narad-0.default.svc:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}})
	if got != "narad-0.default.svc:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsDifferentPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.default.svc:9101", []config.ClusterPeer{{ID: "node-1", Addr: ":9102"}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsPortOnlyNearMatchForHostfulClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer("narad-0.default.svc:9101", ":19101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsPortOnlyNearMatchForClusterPortAddr(t *testing.T) {
	if clusterAddrMatchesPeer(":9101", "host:19101") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9101 "}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsTrimmedPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9101 "}})
	if got != " narad-0.default.svc:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsTrimmedDifferentPortOnlyAdvertisedLookupForHostfulClusterAddr(t *testing.T) {
	got := advertisedClusterAddr("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9102 "}})
	if got != " narad-0.default.svc:9101 " {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsTrimmedDifferentPortOnlyPeerRemovalForHostfulClusterAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", " narad-0.default.svc:9101 ", []config.ClusterPeer{{ID: "node-1", Addr: " :9102 "}, {ID: "node-2", Addr: "narad-1.default.svc:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsTrimmedNearMatchPortOnlyForHostfulClusterAddr(t *testing.T) {
	if clusterAddrMatchesPeer(" narad-0.default.svc:9101 ", " :19101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerRejectsTrimmedNearMatchHostfulForClusterPortAddr(t *testing.T) {
	if clusterAddrMatchesPeer(" :9101 ", " host:19101 ") {
		t.Fatal("clusterAddrMatchesPeer() = true, want false")
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulPeerRemovalForClusterPortUseCaseWithHostname(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9101"}, {ID: "node-2", Addr: "host:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulAdvertisedLookupForClusterPortUseCaseWithHostname(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9101"}})
	if got != "host:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentPortAdvertisedLookupForClusterPortUseCaseWithHostname(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentPortPeerRemovalForClusterPortUseCaseWithHostname(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "host:9102"}, {ID: "node-2", Addr: "host:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulPeerRemovalForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9101"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulAdvertisedLookupForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9101"}})
	if got != "[::1]:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentPortAdvertisedLookupForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentPortPeerRemovalForClusterPortUseCaseWithIPv6(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9102"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulPeerRemovalForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9101"}, {ID: "node-2", Addr: "10.0.0.2:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsHostfulAdvertisedLookupForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9101"}})
	if got != "10.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentPortAdvertisedLookupForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := advertisedClusterAddr("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9102"}})
	if got != ":9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsHostfulDifferentPortPeerRemovalForClusterPortUseCaseWithIPv4(t *testing.T) {
	got := configPeersToMetastore("node-1", ":9101", []config.ClusterPeer{{ID: "node-1", Addr: "10.0.0.1:9102"}, {ID: "node-2", Addr: "10.0.0.2:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerUsesAdvertisedAddrInServeConfigWhenClusterPortProvided(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != "127.0.0.1:9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerUsesClusterAddrFallbackInServeConfigWhenPeerMissing(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
	if cfg != nil {
		t.Fatalf("loadServeConfig() cfg = %+v, want nil", cfg)
	}
}

func TestClusterAddrMatchesPeerRemovesAdvertisedLocalPeerInServeConfigWhenClusterPortProvided(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	got := configPeersToMetastore(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers)
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerKeepsAllPeersWhenAdvertisedLocalPeerMissingInServeConfig(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-c@127.0.0.1:9457,node-d@127.0.0.1:9458,node-e@127.0.0.1:9459")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
	if cfg != nil {
		t.Fatalf("loadServeConfig() cfg = %+v, want nil", cfg)
	}
}

func TestClusterAddrMatchesPeerAcceptsServeConfigClusterPortWithHostnamePeers(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-0.local:9456,node-c@narad-1.local:9457,node-d@narad-2.local:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != "narad-0.local:9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsServeConfigClusterPortWithIPv6Peers(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::1]:9456,node-c@[::1]:9457,node-d@[::1]:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != "[::1]:9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsServeConfigClusterPortWithPortOnlyPeers(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@:9456,node-c@:9457,node-d@:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != ":9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithWrongLocalPeerPort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9457,node-c@127.0.0.1:9458,node-d@127.0.0.1:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithWrongLocalPeerPortPortOnly(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@:9457,node-c@:9458,node-d@:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithWrongLocalPeerPortHostname(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-0.local:9457,node-c@narad-1.local:9458,node-d@narad-2.local:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithWrongLocalPeerPortIPv6(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::1]:9457,node-c@[::1]:9458,node-d@[::1]:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithDifferentNodeIDButMatchingAddr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@127.0.0.1:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithDifferentNodeIDButMatchingPortOnlyAddr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@:9456,node-c@:9457,node-d@:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithDifferentNodeIDButMatchingHostnameAddr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@narad-0.local:9456,node-c@narad-1.local:9457,node-d@narad-2.local:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigClusterPortWithDifferentNodeIDButMatchingIPv6Addr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@[::1]:9456,node-c@[::1]:9457,node-d@[::1]:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerAcceptsServeConfigExactHostfulAddrWithMatchingPeer(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	cfg, err := loadServeConfig(nil)
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != "127.0.0.1:9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsServeConfigExactHostfulAddrWithPortOnlyPeerFallback(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@:9456,node-c@:9457,node-d@:9458")
	cfg, err := loadServeConfig(nil)
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != "127.0.0.1:9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigExactHostfulAddrWithDifferentHostSamePort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.2:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigExactHostfulAddrWithDifferentHostHostnameSamePort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "narad-0.local:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-1.local:9456,node-c@narad-2.local:9457,node-d@narad-3.local:9458")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigExactHostfulAddrWithDifferentIPv6HostSamePort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "[::1]:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::2]:9456,node-c@[::1]:9457,node-d@[::1]:9458")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigExactHostfulAddrWithDifferentPortSameHost(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9457,node-c@127.0.0.1:9458,node-d@127.0.0.1:9459")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigExactHostfulAddrWithDifferentPortSameHostname(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "narad-0.local:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-0.local:9457,node-c@narad-1.local:9458,node-d@narad-2.local:9459")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerRejectsServeConfigExactHostfulAddrWithDifferentPortSameIPv6Host(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "[::1]:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::1]:9457,node-c@[::1]:9458,node-d@[::1]:9459")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestClusterAddrMatchesPeerAcceptsServeConfigExactHostfulAddrWithMatchingHostnamePeer(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "narad-0.local:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-0.local:9456,node-c@narad-1.local:9457,node-d@narad-2.local:9458")
	cfg, err := loadServeConfig(nil)
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != "narad-0.local:9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerAcceptsServeConfigExactHostfulAddrWithMatchingIPv6Peer(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "[::1]:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::1]:9456,node-c@[::1]:9457,node-d@[::1]:9458")
	cfg, err := loadServeConfig(nil)
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if got := advertisedClusterAddr(cfg.Cluster.NodeID, cfg.Cluster.Addr, cfg.Cluster.Peers); got != "[::1]:9456" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerRemovesLocalPeerForExactHostfulAddrHostname(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-0.local:9101"}, {ID: "node-2", Addr: "narad-1.local:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerRemovesLocalPeerForExactHostfulAddrIPv6(t *testing.T) {
	got := configPeersToMetastore("node-1", "[::1]:9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::1]:9101"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 1 || got[0].ID != "node-2" {
		t.Fatalf("configPeersToMetastore() = %+v", got)
	}
}

func TestClusterAddrMatchesPeerKeepsDifferentHostSamePortForExactHostfulAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-1", Addr: "127.0.0.2:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerKeepsDifferentHostSamePortForExactHostnameAddr(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-1", Addr: "narad-1.local:9101"}, {ID: "node-2", Addr: "narad-1.local:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerKeepsDifferentHostSamePortForExactIPv6Addr(t *testing.T) {
	got := configPeersToMetastore("node-1", "[::1]:9101", []config.ClusterPeer{{ID: "node-1", Addr: "[::2]:9101"}, {ID: "node-2", Addr: "[::1]:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsExactHostfulDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}})
	if got != "127.0.0.1:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerKeepsExactHostfulDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := configPeersToMetastore("node-1", "127.0.0.1:9101", []config.ClusterPeer{{ID: "node-x", Addr: "127.0.0.1:9101"}, {ID: "node-2", Addr: "127.0.0.1:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsExactHostnameDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-x", Addr: "narad-0.local:9101"}})
	if got != "narad-0.local:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerKeepsExactHostnameDifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := configPeersToMetastore("node-1", "narad-0.local:9101", []config.ClusterPeer{{ID: "node-x", Addr: "narad-0.local:9101"}, {ID: "node-2", Addr: "narad-1.local:9102"}})
	if len(got) != 2 {
		t.Fatalf("configPeersToMetastore() len = %d, want 2", len(got))
	}
}

func TestClusterAddrMatchesPeerRejectsExactIPv6DifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
	got := advertisedClusterAddr("node-1", "[::1]:9101", []config.ClusterPeer{{ID: "node-x", Addr: "[::1]:9101"}})
	if got != "[::1]:9101" {
		t.Fatalf("advertisedClusterAddr() = %q", got)
	}
}

func TestClusterAddrMatchesPeerKeepsExactIPv6DifferentNodeIDEvenWhenAddrMatches(t *testing.T) {
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

func TestLoadServeConfigRejectsSharedPeersWithoutLocalVoter(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9101")
	t.Setenv("NARAD_NODE_ID", "node-1")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-2@127.0.0.1:9102,node-3@127.0.0.1:9103,node-4@127.0.0.1:9104")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want error")
	}
}

func TestLoadServeConfigAcceptsSharedThreeVoterListFromEnv(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9101")
	t.Setenv("NARAD_NODE_ID", "node-1")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-1@127.0.0.1:9101,node-2@127.0.0.1:9102,node-3@127.0.0.1:9103")
	cfg, err := loadServeConfig(nil)
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if len(cfg.Cluster.Peers) != 3 {
		t.Fatalf("len(cfg.Cluster.Peers) = %d, want 3", len(cfg.Cluster.Peers))
	}
}

func TestLoadServeConfigAppliesNodeIDFlagToSharedPeers(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9101")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9101,node-c@127.0.0.1:9102,node-d@127.0.0.1:9103")
	cfg, err := loadServeConfig([]string{"--node-id", "node-b"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.Cluster.NodeID != "node-b" {
		t.Fatalf("Cluster.NodeID = %q, want %q", cfg.Cluster.NodeID, "node-b")
	}
}

func TestLoadServeConfigAppliesClusterPortFlagToSharedPeers(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@:9456,node-c@:9457,node-d@:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.Cluster.Addr != ":9456" {
		t.Fatalf("Cluster.Addr = %q, want %q", cfg.Cluster.Addr, ":9456")
	}
}

func TestLoadServeConfigAcceptsClusterPortFlagWithHostfulSharedPeers(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	cfg, err := loadServeConfig([]string{"--cluster-port", "9456"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.Cluster.Addr != ":9456" {
		t.Fatalf("Cluster.Addr = %q, want %q", cfg.Cluster.Addr, ":9456")
	}
}

func TestLoadServeConfigRejectsSharedPeersWhenClusterAddrDoesNotMatch(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9109")
	t.Setenv("NARAD_NODE_ID", "node-1")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-1@127.0.0.1:9101,node-2@127.0.0.1:9102,node-3@127.0.0.1:9103")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want error")
	}
}

func TestLoadServeConfigRejectsSharedPeersMissingLocalAddrWithFlag(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-1")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-1@127.0.0.1:9101,node-2@127.0.0.1:9102,node-3@127.0.0.1:9103")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want error")
	}
}

func TestLoadServeConfigRejectsRemoteOnlyPeerListFromEnv(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9101")
	t.Setenv("NARAD_NODE_ID", "node-1")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-2@127.0.0.1:9102,node-3@127.0.0.1:9103")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want error")
	}
}

func TestLoadServeConfigRejectsMalformedPeersFromEnv(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_PEERS", "node-2,node-3@127.0.0.1:9103")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want error")
	}
}

func TestLoadServeConfigAppliesNodeIDFlag(t *testing.T) {
	cfg, err := loadServeConfig([]string{"--node-id", "node-b"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.Cluster.NodeID != "node-b" {
		t.Fatalf("Cluster.NodeID = %q, want %q", cfg.Cluster.NodeID, "node-b")
	}
}

func TestBuildBrokerRejectsNonStoreMetastore(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeMS := stubMetastore{}

	if _, _, _, _, _, err := buildBroker(cfg, "node-1", fakeMS, schema.NewAlwaysValid(), m, log); err == nil {
		t.Fatal("buildBroker() error = nil, want error")
	}
}

func TestBuildBrokerReturnsLogs(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := metastore.New(metastore.Config{NodeID: "node-1", DataDir: filepath.Join(t.TempDir(), "metastore"), BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()

	br, logs, _, _, _, err := buildBroker(cfg, "node-1", store, schema.NewJSONSchema(), m, log)
	if err != nil {
		t.Fatalf("buildBroker() error = %v", err)
	}
	if br == nil || logs == nil {
		t.Fatalf("buildBroker() = (%v, %v), want non-nil", br, logs)
	}
}

type stubMetastore struct{}

func (stubMetastore) CreateTopic(context.Context, topic.Topic) error { return nil }
func (stubMetastore) UpdateTopic(context.Context, topic.Topic) error { return nil }
func (stubMetastore) DeleteTopic(context.Context, string) error      { return nil }
func (stubMetastore) GetTopic(context.Context, string) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubMetastore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}
func (stubMetastore) PutSchema(context.Context, string, int, []byte) error   { return nil }
func (stubMetastore) GetSchema(context.Context, string, int) ([]byte, error) { return nil, nil }
func (stubMetastore) LeaderAddr() string                                     { return "" }
func (stubMetastore) GetMember(string) (metastore.Member, error) {
	return metastore.Member{}, metastore.ErrNotFound
}
func (stubMetastore) Close() error { return nil }

func TestInitializeConsumerOffsetsRestoresOnlyOwnedPartitions(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !store.IsLeader() {
		t.Fatal("metastore leader election timed out")
	}
	if err := store.CreateTopic(ctx, topic.Topic{
		Name:                      "orders",
		Partitions:                2,
		VisibilityTimeoutMs:       1000,
		MaxInFlightPerPartition:   16,
		MaxAckedAheadPerPartition: 16,
	}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-1"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-2"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}
	for partition, offset := range map[int]int64{0: 3, 1: 9} {
		partitionDir := storage.TopicPartitionDir(dataDir, "orders", partition)
		if err := storage.WriteConsumerOffset(partitionDir, offset); err != nil {
			t.Fatalf("WriteConsumerOffset(%d) error = %v", partition, err)
		}
	}
	inFlight := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 16, MaxAckedAhead: 16}, nil
	}, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if err := initializeConsumerOffsets(ctx, dataDir, store, inFlight, logger, "node-1"); err != nil {
		t.Fatalf("initializeConsumerOffsets() error = %v", err)
	}
	if got := inFlight.Next("orders", 0); got != 4 {
		t.Fatalf("Next(orders,0) = %d, want 4", got)
	}
	if got := inFlight.Next("orders", 1); got != 0 {
		t.Fatalf("Next(orders,1) = %d, want 0", got)
	}
}

func TestInitializeSchemasLoadsPersistedSchemas(t *testing.T) {
	ctx := context.Background()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !store.IsLeader() {
		t.Fatal("metastore leader election timed out")
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	raw := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)
	if err := store.PutSchema(ctx, "orders", 1, raw); err != nil {
		t.Fatalf("PutSchema() error = %v", err)
	}

	registry := schema.NewJSONSchema()
	if err := initializeSchemas(ctx, store, registry); err != nil {
		t.Fatalf("initializeSchemas() error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{"id":"o_123"}`)); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestInitializeSchemasSkipsTopicsWithoutSchemas(t *testing.T) {
	ctx := context.Background()
	store, err := metastore.New(metastore.Config{
		NodeID:        "node-1",
		DataDir:       filepath.Join(t.TempDir(), "metastore"),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !store.IsLeader() {
		t.Fatal("metastore leader election timed out")
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}

	registry := schema.NewJSONSchema()
	if err := initializeSchemas(ctx, store, registry); err != nil {
		t.Fatalf("initializeSchemas() error = %v", err)
	}
	if err := registry.Validate(ctx, "orders", []byte(`{"id":"o_123"}`)); !errors.Is(err, schema.ErrSchemaNotFound) {
		t.Fatalf("Validate() error = %v, want %v", err, schema.ErrSchemaNotFound)
	}
}

func TestBuildMetricsReturnsUsableRegistry(t *testing.T) {
	reg, m := buildMetrics()
	if reg == nil || m == nil {
		t.Fatal("buildMetrics() returned nil values")
	}
	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(metricFamilies) == 0 {
		t.Fatal("Gather() returned no metric families")
	}
}

func TestCloseWithLogDoesNothingOnNilError(t *testing.T) {
	closeWithLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "metastore", func() error { return nil })
}

func TestZstdLevelFromString(t *testing.T) {
	cases := []struct {
		input string
		want  zstd.EncoderLevel
	}{
		{input: "fastest", want: zstd.SpeedFastest},
		{input: "default", want: zstd.SpeedDefault},
		{input: "better", want: zstd.SpeedBetterCompression},
		{input: "best", want: zstd.SpeedBestCompression},
		{input: "", want: zstd.SpeedDefault},
	}

	for _, tc := range cases {
		got, err := zstdLevelFromString(tc.input)
		if err != nil {
			t.Fatalf("zstdLevelFromString(%q) error = %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("zstdLevelFromString(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}

	if _, err := zstdLevelFromString("weird"); err == nil {
		t.Fatal("zstdLevelFromString() error = nil, want error")
	}
}

func TestBuildCodec(t *testing.T) {
	c, err := buildCodec(config.StorageConfig{Codec: "none"})
	if err != nil {
		t.Fatalf("buildCodec(none) error = %v", err)
	}
	if c.Flag() != 0 {
		t.Fatalf("buildCodec(none) flag = %d, want 0", c.Flag())
	}

	c, err = buildCodec(config.StorageConfig{Codec: "zstd", CompressionLevel: "better"})
	if err != nil {
		t.Fatalf("buildCodec(zstd) error = %v", err)
	}
	if c == nil {
		t.Fatal("buildCodec(zstd) returned nil codec")
	}

	if _, err := buildCodec(config.StorageConfig{Codec: "unknown"}); err == nil {
		t.Fatal("buildCodec(unknown) error = nil, want error")
	}
}

func TestStorageOptions(t *testing.T) {
	opts, err := storageOptions(config.StorageConfig{
		Codec:                       "none",
		Fsync:                       config.FsyncBatched,
		FlushBytes:                  128,
		FlushRecords:                16,
		FlushIntervalMs:             250,
		SyncIntervalMs:              750,
		SyncBytes:                   4096,
		HighWatermarkSyncIntervalMs: 3000,
		SegmentBytes:                4096,
		RetentionCheckIntervalMs:    500,
	})
	if err != nil {
		t.Fatalf("storageOptions() error = %v", err)
	}
	if opts.FlushBytes != 128 || opts.FlushRecords != 16 || opts.SegmentBytes != 4096 {
		t.Fatalf("storageOptions() opts = %+v", opts)
	}
	if opts.FlushInterval != 250*time.Millisecond {
		t.Fatalf("FlushInterval = %v, want %v", opts.FlushInterval, 250*time.Millisecond)
	}
	if opts.SyncMode != storage.SyncBatched || opts.SyncInterval != 750*time.Millisecond || opts.SyncBytes != 4096 {
		t.Fatalf("sync options = %+v", opts)
	}
	if opts.HWMSyncInterval != 3*time.Second {
		t.Fatalf("HWMSyncInterval = %v, want 3s", opts.HWMSyncInterval)
	}
	if opts.Retention.CheckInterval != 500*time.Millisecond {
		t.Fatalf("Retention.CheckInterval = %v, want %v", opts.Retention.CheckInterval, 500*time.Millisecond)
	}

	opts, err = storageOptions(config.StorageConfig{
		Codec:                       "none",
		Fsync:                       config.FsyncPerWrite,
		FlushBytes:                  128,
		FlushRecords:                16,
		FlushIntervalMs:             250,
		SyncIntervalMs:              750,
		HighWatermarkSyncIntervalMs: 3000,
		SegmentBytes:                4096,
		RetentionCheckIntervalMs:    500,
	})
	if err != nil {
		t.Fatalf("storageOptions(per_write) error = %v", err)
	}
	if opts.SyncMode != storage.SyncPerWrite {
		t.Fatalf("SyncMode = %q, want %q", opts.SyncMode, storage.SyncPerWrite)
	}
}

func TestIngressWALOptions(t *testing.T) {
	opts := ingressWALOptions(config.StorageConfig{
		IngressWALSyncIntervalMs: 5,
		IngressWALSyncBytes:      4096,
	})

	if opts.SyncInterval != 5*time.Millisecond {
		t.Fatalf("SyncInterval = %v, want 5ms", opts.SyncInterval)
	}
	if opts.SyncBytes != 4096 {
		t.Fatalf("SyncBytes = %d, want 4096", opts.SyncBytes)
	}
}

func TestStorageOptionsReturnsCodecError(t *testing.T) {
	_, err := storageOptions(config.StorageConfig{Codec: "wat"})
	if err == nil {
		t.Fatal("storageOptions() error = nil, want error")
	}
}

func TestCloseWithLogHandlesError(t *testing.T) {
	closeWithLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "broker", func() error {
		return errors.New("close failed")
	})
}

func TestBuildAPIServerPanicsWithoutBroker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("buildAPIServer() did not panic")
		}
	}()

	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	_ = buildAPIServer(context.Background(), cfg, nil, nil, nil, nil, m, reg, log)
}

func TestBuildAPIServerReturnsServer(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	broker := stubBroker{}
	srv := buildAPIServer(context.Background(), cfg, broker, nil, nil, nil, m, reg, log)
	if srv == nil {
		t.Fatal("buildAPIServer() returned nil")
	}
}

type stubBroker struct{}

func (stubBroker) CreateTopic(context.Context, brokertopics.CreateOpts) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) IncreaseTopicPartitions(context.Context, string, int) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) UpdateTopicRetention(context.Context, string, int64) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) UpdateTopicCaps(context.Context, string, int64, int64) (topic.Topic, error) {
	return topic.Topic{}, nil
}

func (stubBroker) UpdateTopicSchema(context.Context, string, []byte) (topic.Topic, error) {
	return topic.Topic{}, nil
}
func (stubBroker) DeleteTopic(context.Context, string) error             { return nil }
func (stubBroker) PurgeTopic(context.Context, string) error              { return nil }
func (stubBroker) GetTopic(context.Context, string) (topic.Topic, error) { return topic.Topic{}, nil }
func (stubBroker) GetTopicDetails(context.Context, string) (topic.Details, error) {
	return topic.Details{}, nil
}

func (stubBroker) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}

func (stubBroker) Produce(context.Context, string, string, []byte, ...int) (int64, int, error) {
	return 0, 0, nil
}

func (stubBroker) AcceptProduce(context.Context, string, string, []byte, ...int) (ingress.AcceptedProduce, error) {
	return ingress.AcceptedProduce{}, nil
}

func (stubBroker) CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error) {
	return 0, nil
}

func (stubBroker) CommitAcceptedProduceBatch(_ context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	return make([]int64, len(records)), nil
}

func (stubBroker) Consume(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return topic.Message{}, false, nil
}
func (stubBroker) Ack(context.Context, string, consumer.Handle) error        { return nil }
func (stubBroker) Snapshot(context.Context) ([]metrics.TopicSnapshot, error) { return nil, nil }
func (stubBroker) Ready(context.Context) error                               { return nil }
func (stubBroker) Close() error                                              { return nil }
