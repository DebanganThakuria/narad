package config

import (
	"testing"
	"time"
)

func TestDefaultReturnsExpectedValues(t *testing.T) {
	cfg := Default()

	if cfg.HTTP.Addr != ":7942" || cfg.Cluster.Addr != ":7943" {
		t.Fatalf("Default() addrs = %+v", cfg)
	}
	if cfg.HTTP.ReadTimeout != Duration(10*time.Second) {
		t.Fatalf("Default() read timeout = %v, want 10s", cfg.HTTP.ReadTimeout)
	}
	if cfg.Storage.Fsync != FsyncBatched || cfg.Storage.Codec != "zstd" || cfg.Storage.CompressionLevel != "best" {
		t.Fatalf("Default() storage = %+v", cfg.Storage)
	}
	if cfg.Topic.DefaultPartitions != 3 || cfg.Topic.MaxPartitions != 1024 || cfg.Topic.DefaultReplicationFactor != 2 {
		t.Fatalf("Default() topic defaults = %+v", cfg.Topic)
	}
}
