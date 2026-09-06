package config

import (
	"reflect"
	"testing"
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
