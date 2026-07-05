package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/platform/config"
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

func TestLoadServeConfigUsesAdvertisedAddrWhenClusterPortProvided(t *testing.T) {
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

func TestLoadServeConfigUsesClusterAddrFallbackWhenPeerMissing(t *testing.T) {
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

func TestLoadServeConfigRemovesAdvertisedLocalPeerWhenClusterPortProvided(t *testing.T) {
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

func TestLoadServeConfigKeepsAllPeersWhenAdvertisedLocalPeerMissing(t *testing.T) {
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

func TestLoadServeConfigAcceptsClusterPortWithHostnamePeers(t *testing.T) {
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

func TestLoadServeConfigAcceptsClusterPortWithIPv6Peers(t *testing.T) {
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

func TestLoadServeConfigAcceptsClusterPortWithPortOnlyPeers(t *testing.T) {
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

func TestLoadServeConfigRejectsClusterPortWithWrongLocalPeerPort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9457,node-c@127.0.0.1:9458,node-d@127.0.0.1:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsClusterPortWithWrongLocalPeerPortPortOnly(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@:9457,node-c@:9458,node-d@:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsClusterPortWithWrongLocalPeerPortHostname(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-0.local:9457,node-c@narad-1.local:9458,node-d@narad-2.local:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsClusterPortWithWrongLocalPeerPortIPv6(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::1]:9457,node-c@[::1]:9458,node-d@[::1]:9459")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsClusterPortWithDifferentNodeIDButMatchingAddr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@127.0.0.1:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsClusterPortWithDifferentNodeIDButMatchingPortOnlyAddr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@:9456,node-c@:9457,node-d@:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsClusterPortWithDifferentNodeIDButMatchingHostnameAddr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@narad-0.local:9456,node-c@narad-1.local:9457,node-d@narad-2.local:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsClusterPortWithDifferentNodeIDButMatchingIPv6Addr(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-x@[::1]:9456,node-c@[::1]:9457,node-d@[::1]:9458")
	if _, err := loadServeConfig([]string{"--cluster-port", "9456"}); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigAcceptsExactHostfulAddrWithMatchingPeer(t *testing.T) {
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

func TestLoadServeConfigAcceptsExactHostfulAddrWithPortOnlyPeerFallback(t *testing.T) {
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

func TestLoadServeConfigRejectsExactHostfulAddrWithDifferentHostSamePort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.2:9456,node-c@127.0.0.1:9457,node-d@127.0.0.1:9458")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsExactHostfulAddrWithDifferentHostHostnameSamePort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "narad-0.local:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-1.local:9456,node-c@narad-2.local:9457,node-d@narad-3.local:9458")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsExactHostfulAddrWithDifferentIPv6HostSamePort(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "[::1]:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::2]:9456,node-c@[::1]:9457,node-d@[::1]:9458")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsExactHostfulAddrWithDifferentPortSameHost(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@127.0.0.1:9457,node-c@127.0.0.1:9458,node-d@127.0.0.1:9459")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsExactHostfulAddrWithDifferentPortSameHostname(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "narad-0.local:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@narad-0.local:9457,node-c@narad-1.local:9458,node-d@narad-2.local:9459")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigRejectsExactHostfulAddrWithDifferentPortSameIPv6Host(t *testing.T) {
	t.Setenv("NARAD_NODE_ID", "node-b")
	t.Setenv("NARAD_CLUSTER_ADDR", "[::1]:9456")
	t.Setenv("NARAD_CLUSTER_PEERS", "node-b@[::1]:9457,node-c@[::1]:9458,node-d@[::1]:9459")
	if _, err := loadServeConfig(nil); err == nil {
		t.Fatal("loadServeConfig() error = nil, want validation error")
	}
}

func TestLoadServeConfigAcceptsExactHostfulAddrWithMatchingHostnamePeer(t *testing.T) {
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

func TestLoadServeConfigAcceptsExactHostfulAddrWithMatchingIPv6Peer(t *testing.T) {
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
