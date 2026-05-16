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
	if err := os.WriteFile(path, []byte(`{"http":{"addr":"127.0.0.1:8111"},"log":{"level":"debug"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:8111" || cfg.Log.Level != "debug" {
		t.Fatalf("Load() cfg = %+v", cfg)
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

func TestApplyEnvOverridesFields(t *testing.T) {
	t.Setenv("NARAD_HTTP_ADDR", "127.0.0.1:9001")
	t.Setenv("NARAD_CLUSTER_ADDR", "127.0.0.1:9002")
	t.Setenv("NARAD_HTTP_READ_TIMEOUT", "2s")
	t.Setenv("NARAD_STORAGE_FLUSH_BYTES", "123")
	t.Setenv("NARAD_LOG_FORMAT", "json")
	t.Setenv("NARAD_WORKER_ENABLED", "true")

	cfg := Default()
	if err := applyEnv(cfg); err != nil {
		t.Fatalf("applyEnv() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9001" || cfg.Cluster.Addr != "127.0.0.1:9002" {
		t.Fatalf("applyEnv() cfg = %+v", cfg)
	}
	if cfg.HTTP.ReadTimeout.D() != 2*time.Second {
		t.Fatalf("ReadTimeout = %v, want %v", cfg.HTTP.ReadTimeout.D(), 2*time.Second)
	}
	if cfg.Storage.FlushBytes != 123 || cfg.Log.Format != "json" || !cfg.Worker.Enabled {
		t.Fatalf("applyEnv() cfg = %+v", cfg)
	}
}

func TestApplyEnvRejectsInvalidNumbers(t *testing.T) {
	t.Setenv("NARAD_STORAGE_FLUSH_BYTES", "bad")
	cfg := Default()
	if err := applyEnv(cfg); err == nil {
		t.Fatal("applyEnv() error = nil, want error")
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
