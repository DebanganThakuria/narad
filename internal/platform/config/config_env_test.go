package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestInitialMembersEnvParsing(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_INITIAL_MEMBERS", " narad-0, narad-1 ,narad-2,, ")
	cfg := Default()
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	want := []string{"narad-0", "narad-1", "narad-2"}
	if !reflect.DeepEqual(cfg.Cluster.InitialMembers, want) {
		t.Fatalf("InitialMembers = %v, want %v", cfg.Cluster.InitialMembers, want)
	}
}

func TestSecurityFlagEnvParsing(t *testing.T) {
	t.Setenv("NARAD_SECURITY_ALLOW_LEGACY_CLUSTER_AUTH", "true")
	cfg := Default()
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	if !cfg.Security.AllowLegacyClusterAuth {
		t.Fatal("AllowLegacyClusterAuth not applied from env")
	}
	t.Setenv("NARAD_SECURITY_ALLOW_LEGACY_CLUSTER_AUTH", "maybe")
	if err := applyEnv(Default()); err == nil {
		t.Fatal("non-boolean NARAD_SECURITY_ALLOW_LEGACY_CLUSTER_AUTH accepted")
	}
}

func TestHTTPHardeningEnvParsing(t *testing.T) {
	t.Setenv("NARAD_HTTP_MAX_HEADER_BYTES", "8192")
	t.Setenv("NARAD_HTTP_MAX_CONNECTIONS", "10")
	t.Setenv("NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY", "3")
	t.Setenv("NARAD_HTTP_METRICS_ADDR", " :9100 ")
	t.Setenv("NARAD_HTTP_METRICS_UNAUTHENTICATED", "true")
	cfg := Default()
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	if cfg.HTTP.MaxHeaderBytes != 8192 || cfg.HTTP.MaxConnections != 10 || cfg.HTTP.MaxConsumeInFlightPerIdentity != 3 || cfg.HTTP.MetricsAddr != ":9100" || !cfg.HTTP.MetricsUnauthenticated {
		t.Fatalf("http env not applied: %+v", cfg.HTTP)
	}
}

func TestInsecureClusterEnvParsing(t *testing.T) {
	t.Setenv("NARAD_SECURITY_ALLOW_INSECURE_CLUSTER", "true")
	cfg := Default()
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	if !cfg.Security.AllowInsecureCluster {
		t.Fatal("AllowInsecureCluster not applied from env")
	}
}

func TestRaftCompactionEnvParsing(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_RAFT_SNAPSHOT_THRESHOLD", "64")
	t.Setenv("NARAD_CLUSTER_RAFT_SNAPSHOT_INTERVAL", "250ms")
	t.Setenv("NARAD_CLUSTER_RAFT_TRAILING_LOGS", "32")
	cfg := Default()
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	if cfg.Cluster.RaftSnapshotThreshold != 64 || cfg.Cluster.RaftSnapshotInterval.D() != 250*time.Millisecond || cfg.Cluster.RaftTrailingLogs != 32 {
		t.Fatalf("raft compaction env not applied: %+v", cfg.Cluster)
	}
	t.Setenv("NARAD_CLUSTER_RAFT_TRAILING_LOGS", "-1")
	if err := applyEnv(Default()); err == nil {
		t.Fatal("negative NARAD_CLUSTER_RAFT_TRAILING_LOGS accepted")
	}
}

func TestRaftCompactionDefaultsMatchRaft(t *testing.T) {
	cfg := Default()
	if cfg.Cluster.RaftSnapshotThreshold != 8192 || cfg.Cluster.RaftSnapshotInterval.D() != 120*time.Second || cfg.Cluster.RaftTrailingLogs != 10240 {
		t.Fatalf("Default() raft compaction = %d/%s/%d, want hashicorp/raft's 8192/120s/10240",
			cfg.Cluster.RaftSnapshotThreshold, cfg.Cluster.RaftSnapshotInterval, cfg.Cluster.RaftTrailingLogs)
	}
}

func TestRaftCompactionValidation(t *testing.T) {
	cfg := Default()
	cfg.Cluster.RaftSnapshotThreshold = 0
	cfg.Cluster.RaftSnapshotInterval = Duration(time.Millisecond)
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() accepted a zero snapshot threshold and a 1ms interval")
	}
	for _, want := range []string{"cluster.raft_snapshot_threshold must be > 0", "cluster.raft_snapshot_interval (1ms) must be >= 5ms"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate() error = %q, want it to mention %q", err, want)
		}
	}
	// Trailing logs may legitimately be zero (keep no log behind a snapshot).
	cfg = Default()
	cfg.Cluster.RaftTrailingLogs = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() rejected raft_trailing_logs=0: %v", err)
	}
}

func TestRaftCompactionFromConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"cluster":{"raft_snapshot_threshold":64,"raft_snapshot_interval":"50ms","raft_trailing_logs":32}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Cluster.RaftSnapshotThreshold != 64 || cfg.Cluster.RaftSnapshotInterval.D() != 50*time.Millisecond || cfg.Cluster.RaftTrailingLogs != 32 {
		t.Fatalf("config file raft compaction not applied: %+v", cfg.Cluster)
	}
}
