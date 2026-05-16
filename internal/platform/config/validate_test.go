package config

import (
	"strings"
	"testing"
)

func TestDefaultValidateSucceeds(t *testing.T) {
	cfg := Default()

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestValidateRejectsInvalidFields(t *testing.T) {
	cfg := Default()
	cfg.HTTP.Addr = "   "
	cfg.HTTP.ReadTimeout = 0
	cfg.HTTP.WriteTimeout = 0
	cfg.HTTP.IdleTimeout = 0
	cfg.HTTP.ShutdownGrace = 0
	cfg.HTTP.MaxConsumeWait = -1
	cfg.Cluster.Addr = "   "
	cfg.Storage.DataDir = ""
	cfg.Storage.Fsync = "invalid"
	cfg.Storage.Codec = "gzip"
	cfg.Storage.CompressionLevel = "turbo"
	cfg.Storage.FlushBytes = 0
	cfg.Storage.FlushRecords = 0
	cfg.Storage.FlushIntervalMs = 0
	cfg.Storage.SegmentBytes = 1024
	cfg.Storage.RetentionCheckIntervalMs = 0
	cfg.Topic.DefaultPartitions = 0
	cfg.Topic.MaxPartitions = 0
	cfg.Topic.DefaultReplicationFactor = 0
	cfg.Topic.DefaultRetentionAgeMs = -1
	cfg.Topic.DefaultVisibilityTimeoutMs = -1
	cfg.Log.Level = "verbose"
	cfg.Log.Format = "pretty"

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want aggregated validation error")
	}

	for _, want := range []string{
		"http.addr must not be empty",
		"http.read_timeout must be > 0",
		"cluster.addr must not be empty",
		"storage.fsync \"invalid\" is not one of [per_write, batched]",
		"storage.codec \"gzip\" is not one of [zstd, none]",
		"at least one of storage.flush_bytes or storage.flush_records must be > 0",
		"topic.default_partitions must be >= 3",
		"log.level \"verbose\" is not one of [debug, info, warn, error]",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Validate() error = %q, want substring %q", err.Error(), want)
		}
	}
}

func TestValidateRejectsSameHTTPAndClusterAddr(t *testing.T) {
	cfg := Default()
	cfg.Cluster.Addr = cfg.HTTP.Addr

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "http.addr and cluster.addr must differ") {
		t.Fatalf("Validate() error = %v, want address conflict", err)
	}
}

func TestValidateRejectsDefaultPartitionsAboveMax(t *testing.T) {
	cfg := Default()
	cfg.Topic.DefaultPartitions = 9
	cfg.Topic.MaxPartitions = 8

	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "topic.default_partitions (9) must not exceed topic.max_partitions (8)") {
		t.Fatalf("Validate() error = %v, want partition bounds error", err)
	}
}
