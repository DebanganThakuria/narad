package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRemotesDefaults(t *testing.T) {
	cfg := Default()
	if !slices.Equal(cfg.Remotes.AllowedPorts, []int{443}) || cfg.Remotes.MaxHeldBytes != 256<<20 || cfg.Remotes.Configured() {
		t.Fatalf("remotes defaults = %+v", cfg.Remotes)
	}
	if cfg.HTTP.MaxBatchBodyBytesInFlight != 256<<20 {
		t.Fatalf("http.max_batch_body_bytes_in_flight default = %d", cfg.HTTP.MaxBatchBodyBytesInFlight)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults do not validate: %v", err)
	}
}

func TestRemotesEnv(t *testing.T) {
	t.Setenv("NARAD_REMOTES_ALLOWED_HOSTS", "*.internal.example, b.example")
	t.Setenv("NARAD_REMOTES_ALLOWED_PORTS", "443,8443")
	t.Setenv("NARAD_REMOTES_ALLOW_ADDRESSES", "127.0.0.0/8")
	t.Setenv("NARAD_REMOTES_MAX_HELD_BYTES", "1048576")
	t.Setenv("NARAD_REMOTES_API_HOP_ENCRYPTED", "true")
	t.Setenv("NARAD_CLUSTER_SECRET_PREVIOUS", "previous-secret-value")
	t.Setenv("NARAD_HTTP_MAX_BATCH_BODY_BYTES_IN_FLIGHT", "0")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.Remotes
	if !slices.Equal(r.AllowedHosts, []string{"*.internal.example", "b.example"}) || !slices.Equal(r.AllowedPorts, []int{443, 8443}) ||
		!slices.Equal(r.AllowAddresses, []string{"127.0.0.0/8"}) || r.MaxHeldBytes != 1<<20 || !r.APIHopEncrypted {
		t.Fatalf("remotes from env = %+v", r)
	}
	if cfg.Security.ClusterSecretPrevious != "previous-secret-value" || cfg.HTTP.MaxBatchBodyBytesInFlight != 0 {
		t.Fatalf("security/http from env = %q %d", cfg.Security.ClusterSecretPrevious, cfg.HTTP.MaxBatchBodyBytesInFlight)
	}
	if !r.Configured() {
		t.Fatal("remotes from env not reported configured")
	}
	t.Setenv("NARAD_REMOTES_ALLOWED_PORTS", "443,x")
	if _, err := Load(""); err == nil {
		t.Fatal("a non-numeric port loaded")
	}
}

func TestRemotesValidation(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"block without security", func(c *Config) { c.Security.Enabled = false; c.Remotes.APIHopEncrypted = true }, "require security.enabled"},
		{"no ports", func(c *Config) { c.Remotes.AllowedPorts = nil }, "allowed_ports must name"},
		{"bad port", func(c *Config) { c.Remotes.AllowedPorts = []int{0} }, "allowed_ports entries"},
		{"bad cidr", func(c *Config) { c.Remotes.AllowAddresses = []string{"127.0.0.1"} }, "allow_addresses"},
		{"empty host", func(c *Config) { c.Remotes.AllowedHosts = []string{""} }, "allowed_hosts entries"},
		{"negative held", func(c *Config) { c.Remotes.MaxHeldBytes = -1 }, "max_held_bytes"},
		{"negative budget", func(c *Config) { c.HTTP.MaxBatchBodyBytesInFlight = -1 }, "max_batch_body_bytes_in_flight"},
	}
	for _, c := range cases {
		cfg := Default()
		c.edit(cfg)
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: Validate = %v, want %q", c.name, err, c.want)
		}
	}
	// Security off with the default remotes block is fine.
	cfg := Default()
	cfg.Security.Enabled = false
	if err := cfg.Validate(); err != nil {
		t.Fatalf("security off without remotes settings: %v", err)
	}
}

// The previous cluster secret, like the current one, is never read from
// the config file.
func TestClusterSecretPreviousIsNotFileConfigurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "narad.json")
	if err := os.WriteFile(path, []byte(`{"security":{"cluster_secret_previous":"x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("cluster_secret_previous loaded from a config file")
	}
}
