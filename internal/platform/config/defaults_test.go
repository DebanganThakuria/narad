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
	if cfg.Storage.Fsync != FsyncBatched || cfg.Storage.Codec != "none" || cfg.Storage.CompressionLevel != "fastest" {
		t.Fatalf("Default() storage = %+v", cfg.Storage)
	}
	if cfg.Storage.SyncIntervalMs != 1000 || cfg.Storage.SyncBytes != 8<<20 || cfg.Storage.HighWatermarkSyncIntervalMs != 5000 {
		t.Fatalf("Default() storage sync = %+v", cfg.Storage)
	}
	if cfg.Storage.IngressWALSyncIntervalMs != 10 || cfg.Storage.IngressWALSyncBytes != 1<<20 || cfg.Storage.IngressWALShards != 1 {
		t.Fatalf("Default() ingress WAL sync = %+v", cfg.Storage)
	}
	if cfg.Topic.DefaultPartitions != 3 || cfg.Topic.MaxPartitions != 108 {
		t.Fatalf("Default() topic defaults = %+v", cfg.Topic)
	}
}
