package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// The authenticator's queue gauge is live in the binary: building the
// authenticator registers it on the process registry.
func TestBuildAuthenticatorRegistersTheVerifyQueueGauge(t *testing.T) {
	cfg := config.Default()
	cfg.Security.Enabled = true
	reg := prometheus.NewRegistry()
	store := openLeaderStore(t, t.TempDir())

	if auth := buildAuthenticator(cfg, store, reg, slog.New(slog.NewTextHandler(io.Discard, nil))); auth == nil {
		t.Fatal("buildAuthenticator returned nil with security enabled")
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == "narad_auth_verify_queued" {
			return
		}
	}
	t.Fatal("narad_auth_verify_queued is not registered")
}
