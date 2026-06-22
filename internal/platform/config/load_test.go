package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadUsesDefaultsWithoutFile(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP.Addr == "" || cfg.Storage.DataDir == "" {
		t.Fatalf("Load() cfg = %+v", cfg)
	}
}

func TestLoadReadsConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"http":{"addr":"127.0.0.1:8111"},"storage":{"data_dir":"custom-data"},"log":{"level":"debug"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:8111" || cfg.Storage.DataDir != "custom-data" || cfg.Log.Level != "debug" {
		t.Fatalf("Load() cfg = %+v", cfg)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	path := filepath.Join("..", "..", "..", "narad.example.json")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q) error = %v", path, err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"unknown":true}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestLoadRejectsInternalStorageFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"storage":{"flush_bytes":123}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if _, err := Load(path); err == nil {
		t.Fatal("Load() error = nil, want error")
	}
}

func TestApplyEnvOverridesFields(t *testing.T) {
	t.Setenv("NARAD_HTTP_ADDR", "127.0.0.1:9001")
	t.Setenv("NARAD_HTTP_PPROF_ADDR", "127.0.0.1:6060")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9002")
	t.Setenv("NARAD_HTTP_READ_TIMEOUT", "2s")
	t.Setenv("NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS", "3600000")
	t.Setenv("NARAD_TOPIC_DEFAULT_PARTITIONS", "6")
	t.Setenv("NARAD_LOG_FORMAT", "json")

	cfg := Default()
	defaultStorage := cfg.Storage
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9001" || cfg.Cluster.Addr != "127.0.0.1:9002" {
		t.Fatalf("applyEnv() cfg = %+v", cfg)
	}
	if cfg.HTTP.PprofAddr != "127.0.0.1:6060" {
		t.Fatalf("PprofAddr = %q, want %q", cfg.HTTP.PprofAddr, "127.0.0.1:6060")
	}
	if cfg.HTTP.ReadTimeout.D() != 2*time.Second {
		t.Fatalf("ReadTimeout = %v, want %v", cfg.HTTP.ReadTimeout.D(), 2*time.Second)
	}
	if cfg.Topic.DefaultRetentionAgeMs != 3600000 || cfg.Topic.DefaultPartitions != 6 || cfg.Log.Format != "json" {
		t.Fatalf("applyEnv() cfg = %+v", cfg)
	}
	if cfg.Storage != defaultStorage {
		t.Fatalf("applyEnv() changed internal storage config: got %+v want %+v", cfg.Storage, defaultStorage)
	}
}

func TestApplyEnvRejectsInvalidNumbers(t *testing.T) {
	t.Setenv("NARAD_TOPIC_DEFAULT_PARTITIONS", "bad")
	cfg := Default()
	if err := applyEnv(cfg); err == nil {
		t.Fatal("applyEnv() error = nil, want error")
	}
}

func TestApplyEnvIgnoresInternalStorageKnobs(t *testing.T) {
	t.Setenv("NARAD_STORAGE_FLUSH_BYTES", "bad")
	t.Setenv("NARAD_INGRESS_WAL_SHARDS", "999")

	cfg := Default()
	defaultStorage := cfg.Storage
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	if cfg.Storage != defaultStorage {
		t.Fatalf("storage changed: got %+v want %+v", cfg.Storage, defaultStorage)
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("NARAD_HTTP_MAX_CONSUME_WAIT", "1500ms")
	var d Duration
	if err := envDuration("NARAD_HTTP_MAX_CONSUME_WAIT", &d); err != nil {
		t.Fatalf("envDuration() error = %v", err)
	}
	if d.D() != 1500*time.Millisecond {
		t.Fatalf("envDuration() duration = %v, want %v", d.D(), 1500*time.Millisecond)
	}
}

func TestDurationJSONRoundTrip(t *testing.T) {
	original := Duration(3 * time.Second)
	b, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var decoded Duration
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if decoded != original {
		t.Fatalf("decoded = %v, want %v", decoded, original)
	}
}

func TestDurationUnmarshalSupportsNumericNanoseconds(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`1000000000`), &d); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if d.D() != time.Second {
		t.Fatalf("duration = %v, want %v", d.D(), time.Second)
	}
}

func TestDurationUnmarshalRejectsInvalidString(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"soon"`), &d); err == nil {
		t.Fatal("Unmarshal() error = nil, want error")
	}
}
