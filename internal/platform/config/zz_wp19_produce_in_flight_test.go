package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The per-identity produce cap mirrors the consume one: a JSON key, an
// env var, and a >= 0 check, but it defaults to 0 (off), which is how
// the server behaved before it was configurable.
func TestZZWP19MaxProduceInFlightPerIdentity(t *testing.T) {
	if got := Default().HTTP.MaxProduceInFlightPerIdentity; got != 0 {
		t.Fatalf("default = %d, want 0 (off)", got)
	}
	if err := Default().Validate(); err != nil {
		t.Fatalf("Validate() with the default = %v", err)
	}

	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"http":{"max_produce_in_flight_per_identity":64}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP.MaxProduceInFlightPerIdentity != 64 {
		t.Fatalf("file value not applied: %d", cfg.HTTP.MaxProduceInFlightPerIdentity)
	}

	t.Setenv("NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY", "7")
	if cfg, err = Load(path); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.HTTP.MaxProduceInFlightPerIdentity != 7 {
		t.Fatalf("env did not override the file: %d", cfg.HTTP.MaxProduceInFlightPerIdentity)
	}
	if cfg.HTTP.MaxConsumeInFlightPerIdentity != 1024 {
		t.Fatalf("the produce cap moved the consume cap: %d", cfg.HTTP.MaxConsumeInFlightPerIdentity)
	}

	t.Setenv("NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY", "lots")
	if err := applyEnv(Default()); err == nil {
		t.Fatal("non-integer NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY accepted")
	}

	cfg = Default()
	cfg.HTTP.MaxProduceInFlightPerIdentity = -1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "max_produce_in_flight") {
		t.Fatalf("negative produce cap: %v", err)
	}
}
