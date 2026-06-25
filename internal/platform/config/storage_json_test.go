package config

import (
	"encoding/json"
	"testing"
)

func TestStorageConfigAllowsCodecAndCompression(t *testing.T) {
	c := StorageConfig{DataDir: "data", Codec: "none", CompressionLevel: "fastest"}
	if err := json.Unmarshal([]byte(`{"codec":"zstd","compression_level":"better"}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.Codec != "zstd" {
		t.Errorf("codec = %q, want zstd", c.Codec)
	}
	if c.CompressionLevel != "better" {
		t.Errorf("compression_level = %q, want better", c.CompressionLevel)
	}
	if c.DataDir != "data" {
		t.Errorf("data_dir = %q, want preserved 'data'", c.DataDir)
	}
}

func TestStorageConfigAllowsDataDir(t *testing.T) {
	var c StorageConfig
	if err := json.Unmarshal([]byte(`{"data_dir":"/var/lib/narad"}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.DataDir != "/var/lib/narad" {
		t.Errorf("data_dir = %q", c.DataDir)
	}
}

func TestStorageConfigOmittedPreservesDefaults(t *testing.T) {
	c := StorageConfig{DataDir: "d", Codec: "none", CompressionLevel: "fastest"}
	if err := json.Unmarshal([]byte(`{"data_dir":"e"}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.Codec != "none" || c.CompressionLevel != "fastest" {
		t.Errorf("omitted fields changed: codec=%q level=%q, want none/fastest", c.Codec, c.CompressionLevel)
	}
}

func TestStorageConfigAllowsIngressWALShards(t *testing.T) {
	c := StorageConfig{IngressWALShards: 1}
	if err := json.Unmarshal([]byte(`{"ingress_wal_shards":8}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.IngressWALShards != 8 {
		t.Errorf("ingress_wal_shards = %d, want 8", c.IngressWALShards)
	}
	// Omitted (0) must not clobber the existing value.
	c2 := StorageConfig{IngressWALShards: 4}
	if err := json.Unmarshal([]byte(`{"data_dir":"d"}`), &c2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c2.IngressWALShards != 4 {
		t.Errorf("ingress_wal_shards = %d, want preserved 4", c2.IngressWALShards)
	}
}

func TestStorageConfigRejectsInternalKeys(t *testing.T) {
	for _, key := range []string{"segment_bytes", "fsync", "flush_bytes", "sync_interval_ms", "retention_check_interval_ms"} {
		var c StorageConfig
		if err := json.Unmarshal([]byte(`{"`+key+`":123}`), &c); err == nil {
			t.Errorf("storage.%s was accepted, want rejected as internal", key)
		}
	}
}
