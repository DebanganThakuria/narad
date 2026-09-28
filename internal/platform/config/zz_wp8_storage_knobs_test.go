package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestWP8ConsumerOffsetCommitInterval pins the committer's own knob: it
// defaults to a 1s durability interval (the committer writes to the
// page cache every 100ms and syncs each partition once an interval),
// an operator may set it in the config file (100 restores the older
// power-loss window), and values that would spin the loop or stretch
// the redelivery window past a minute are rejected.
func TestWP8ConsumerOffsetCommitInterval(t *testing.T) {
	cfg := Default()
	if got := cfg.Storage.ConsumerOffsetCommitIntervalMs; got != 1000 {
		t.Fatalf("default consumer_offset_commit_interval_ms = %d, want 1000", got)
	}

	c := Default().Storage
	if err := json.Unmarshal([]byte(`{"consumer_offset_commit_interval_ms":500}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.ConsumerOffsetCommitIntervalMs != 500 {
		t.Fatalf("consumer_offset_commit_interval_ms = %d, want 500", c.ConsumerOffsetCommitIntervalMs)
	}
	if c.FlushIntervalMs != 100 {
		t.Fatalf("flush_interval_ms = %d, want the default 100 untouched", c.FlushIntervalMs)
	}
	c = Default().Storage
	if err := json.Unmarshal([]byte(`{"data_dir":"e"}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if c.ConsumerOffsetCommitIntervalMs != 1000 {
		t.Fatalf("omitted consumer_offset_commit_interval_ms changed to %d, want the default kept", c.ConsumerOffsetCommitIntervalMs)
	}

	for _, bad := range []int{0, -1, 9, 60_001} {
		cfg := Default()
		cfg.Storage.ConsumerOffsetCommitIntervalMs = bad
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "storage.consumer_offset_commit_interval_ms") {
			t.Fatalf("Validate() with consumer_offset_commit_interval_ms=%d error = %v, want a rejection naming the key", bad, err)
		}
	}
	for _, ok := range []int{10, 100, 1000, 60_000} {
		cfg := Default()
		cfg.Storage.ConsumerOffsetCommitIntervalMs = ok
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() with consumer_offset_commit_interval_ms=%d error = %v, want nil", ok, err)
		}
	}
}

// TestWP8IngressWALPrealloc pins the segment-preparation opt-in: off
// by default, settable from the config file, and an omitted key keeps
// the default.
func TestWP8IngressWALPrealloc(t *testing.T) {
	if Default().Storage.IngressWALPrealloc {
		t.Fatal("ingress_wal_prealloc defaults to true, want false (opt-in)")
	}
	c := Default().Storage
	if err := json.Unmarshal([]byte(`{"ingress_wal_prealloc":true}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !c.IngressWALPrealloc {
		t.Fatal("ingress_wal_prealloc = false after setting it true in the file")
	}
	if err := json.Unmarshal([]byte(`{"data_dir":"e"}`), &c); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !c.IngressWALPrealloc {
		t.Fatal("an omitted ingress_wal_prealloc reset the value")
	}
	if err := json.Unmarshal([]byte(`{"ingress_wal_prealloc":"yes"}`), &c); err == nil {
		t.Fatal("a non-boolean ingress_wal_prealloc was accepted")
	}
}

// TestWP8HighWatermarkSyncIntervalIsIgnored pins the deprecation: the
// setting no longer does anything, so no value fails validation.
func TestWP8HighWatermarkSyncIntervalIsIgnored(t *testing.T) {
	for _, v := range []int{-1, 0, 5000} {
		cfg := Default()
		cfg.Storage.HighWatermarkSyncIntervalMs = v
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() with high_watermark_sync_interval_ms=%d error = %v, want nil (deprecated, ignored)", v, err)
		}
	}
}
