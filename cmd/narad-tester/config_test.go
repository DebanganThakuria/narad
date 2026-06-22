package main

import "testing"

func TestParseConfigRateRampFlags(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--messages-per-second", "50",
		"--max-messages-per-second", "500",
		"--rate-ramp-step", "10",
		"--rate-ramp-interval", "10m",
		"--dispatch-interval", "2ms",
		"--consume-wait", "0s",
		"--max-consumed-markers", "5000",
		"--max-outstanding-messages", "7000",
		"--max-messages", "100000",
		"--drain-timeout", "30s",
	})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.MessagesPerSecond != 50 {
		t.Fatalf("messages per second = %d, want 50", cfg.MessagesPerSecond)
	}
	if cfg.MaxMessagesPerSecond != 500 {
		t.Fatalf("max messages per second = %d, want 500", cfg.MaxMessagesPerSecond)
	}
	if cfg.RateRampStep != 10 {
		t.Fatalf("rate ramp step = %d, want 10", cfg.RateRampStep)
	}
	if cfg.RateRampInterval.String() != "10m0s" {
		t.Fatalf("rate ramp interval = %s, want 10m0s", cfg.RateRampInterval)
	}
	if cfg.DispatchInterval.String() != "2ms" {
		t.Fatalf("dispatch interval = %s, want 2ms", cfg.DispatchInterval)
	}
	if cfg.ConsumeWait != 0 {
		t.Fatalf("consume wait = %s, want 0s", cfg.ConsumeWait)
	}
	if cfg.MaxConsumedMarkers != 5000 {
		t.Fatalf("max consumed markers = %d, want 5000", cfg.MaxConsumedMarkers)
	}
	if cfg.MaxOutstandingMessages != 7000 {
		t.Fatalf("max outstanding messages = %d, want 7000", cfg.MaxOutstandingMessages)
	}
	if cfg.MaxMessages != 100000 {
		t.Fatalf("max messages = %d, want 100000", cfg.MaxMessages)
	}
	if cfg.DrainTimeout.String() != "30s" {
		t.Fatalf("drain timeout = %s, want 30s", cfg.DrainTimeout)
	}
}

func TestParseConfigRejectsRateCapBelowInitialRate(t *testing.T) {
	t.Parallel()

	_, err := parseConfig([]string{
		"--messages-per-second", "100",
		"--max-messages-per-second", "50",
	})
	if err == nil {
		t.Fatal("parse config succeeded, want error")
	}
}

func TestParseConfigAllowsProducerOnlyMode(t *testing.T) {
	t.Parallel()

	cfg, err := parseConfig([]string{
		"--consumer-concurrency", "0",
	})
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if cfg.ConsumerConcurrency != 0 {
		t.Fatalf("consumer concurrency = %d, want 0", cfg.ConsumerConcurrency)
	}
}

func TestParseConfigAcceptsDeprecatedLedgerFlags(t *testing.T) {
	t.Parallel()

	if _, err := parseConfig([]string{
		"--ledger-path", "tmp/old.db",
		"--pending-ttl", "1m",
	}); err != nil {
		t.Fatalf("parse config with deprecated ledger flags: %v", err)
	}
}
