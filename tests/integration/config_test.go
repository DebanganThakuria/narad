package main

import (
	"strings"
	"testing"
	"time"
)

// The driver is a package main with no other tests; these pin the CLI
// contract the wrapper scripts (scripts/local-cluster-e2e.sh,
// scripts/local-cluster-chaos.sh, make cluster-load) rely on.

func TestParseConfigDefaultsParse(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig(nil): %v", err)
	}
	if cfg.mode != modeLoad {
		t.Fatalf("default mode = %q, want %q", cfg.mode, modeLoad)
	}
	if len(cfg.nodes) != 3 {
		t.Fatalf("default nodes = %v, want 3 entries", cfg.nodes)
	}
	if cfg.timeout != 2*time.Minute {
		t.Fatalf("default timeout = %s, want 2m", cfg.timeout)
	}
	if !strings.HasPrefix(cfg.runID, "lc-") {
		t.Fatalf("run id %q should be generated with the lc- prefix", cfg.runID)
	}
}

func TestParseConfigExplicitValues(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--mode", "chaos",
		"--nodes", "127.0.0.1:18081/, http://127.0.0.1:18082,,",
		"--timeout", "30s",
		"--run-id", "fixed",
		"--cleanup=false",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.mode != modeChaos {
		t.Fatalf("mode = %q, want chaos", cfg.mode)
	}
	want := []string{"http://127.0.0.1:18081", "http://127.0.0.1:18082"}
	if len(cfg.nodes) != len(want) {
		t.Fatalf("nodes = %v, want %v", cfg.nodes, want)
	}
	for i := range want {
		if cfg.nodes[i] != want[i] {
			t.Fatalf("nodes[%d] = %q, want %q", i, cfg.nodes[i], want[i])
		}
	}
	if cfg.timeout != 30*time.Second || cfg.runID != "fixed" || cfg.cleanup {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestValidMode(t *testing.T) {
	for _, mode := range []string{modeLoad, modeChaos, modeSteady, modeXnode, modeEdge} {
		if !validMode(mode) {
			t.Errorf("validMode(%q) = false, want true", mode)
		}
	}
	for _, mode := range []string{"soak", "", "LOAD", "load "} {
		if validMode(mode) {
			t.Errorf("validMode(%q) = true, want false", mode)
		}
	}
}

func TestParseConfigRejectsSoakMode(t *testing.T) {
	_, err := parseConfig([]string{"--mode", "soak"})
	if err == nil {
		t.Fatal("expected --mode soak to be rejected")
	}
	if !strings.Contains(err.Error(), `invalid --mode "soak"`) {
		t.Fatalf("error = %q, want invalid --mode", err)
	}
}

func TestParseConfigRejectsNonPositiveTimeoutInEveryMode(t *testing.T) {
	// Before the soak removal a zero timeout was legal in exactly one
	// mode; now it is never legal, whatever the mode.
	for _, mode := range []string{modeLoad, modeChaos, modeSteady, modeXnode, modeEdge} {
		for _, timeout := range []string{"0", "0s", "-1s"} {
			_, err := parseConfig([]string{"--mode", mode, "--timeout", timeout})
			if err == nil {
				t.Errorf("mode=%s timeout=%s: expected an error", mode, timeout)
				continue
			}
			if err.Error() != "--timeout must be > 0" {
				t.Errorf("mode=%s timeout=%s: error = %q", mode, timeout, err)
			}
		}
	}
}

func TestParseConfigRemovedSoakFlagsAreUnknown(t *testing.T) {
	for _, args := range [][]string{
		{"--metrics-addr", ":9300"},
		{"--rate-scale", "0.2"},
		{"--soak-profiles", "firehose"},
	} {
		_, err := parseConfig(args)
		if err == nil {
			t.Errorf("%v: expected a flag parse error", args)
			continue
		}
		if !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Errorf("%v: error = %q, want unknown-flag error", args, err)
		}
	}
}

func TestParseConfigOtherValidationStillApplies(t *testing.T) {
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"--nodes", " , "}, "at least one node is required"},
		{[]string{"--topics", "0"}, "--topics must be > 0"},
		{[]string{"--messages", "0"}, "--messages must be > 0"},
		{[]string{"--partitions", "2"}, "--partitions must be >= 3"},
		{[]string{"--produce-concurrency", "0"}, "concurrency values must be > 0"},
		{[]string{"--produce-rate", "-1"}, "--produce-rate must be >= 0"},
		{[]string{"--assignment-timeout", "0"}, "--assignment-timeout must be > 0"},
		{[]string{"--visibility-timeout", "0"}, "--visibility-timeout must be > 0"},
	}
	for _, tc := range cases {
		_, err := parseConfig(tc.args)
		if err == nil || err.Error() != tc.want {
			t.Errorf("%v: error = %v, want %q", tc.args, err, tc.want)
		}
	}
}
