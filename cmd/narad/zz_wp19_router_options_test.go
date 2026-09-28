package main

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// Both per-identity caps reach the API router from the environment. The
// produce one used to stop at the router: nothing set it, so it was off
// whatever the operator configured.
func TestZZWP19APIRouterOptionsCarryInFlightCaps(t *testing.T) {
	t.Setenv("NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY", "32")
	t.Setenv("NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY", "16")
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	opts := apiRouterOptions(cfg.HTTP)
	if opts.ProduceInFlightPerIdentity != 32 || opts.ConsumeInFlightPerIdentity != 16 {
		t.Fatalf("router options = %+v, want produce 32 and consume 16", opts)
	}

	defaults := apiRouterOptions(config.Default().HTTP)
	if defaults.ProduceInFlightPerIdentity != 0 || defaults.ConsumeInFlightPerIdentity != 1024 {
		t.Fatalf("default router options = %+v, want produce off and consume 1024", defaults)
	}
	if !defaults.MetricsOnAPI || !defaults.MetricsRequireAuth {
		t.Fatalf("default router options = %+v, want /metrics on the API port behind credentials", defaults)
	}
	cfg.HTTP.MetricsAddr, cfg.HTTP.MetricsUnauthenticated = ":9100", true
	if opts := apiRouterOptions(cfg.HTTP); opts.MetricsOnAPI || opts.MetricsRequireAuth {
		t.Fatalf("router options with a metrics listener = %+v", opts)
	}
}
