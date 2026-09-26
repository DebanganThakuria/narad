package config

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestWP8ConsumerOffsetCommitInterval pins the committer's own knob: it
// defaults to the 100 ms cadence it used to borrow from
// storage.flush_interval_ms, an operator may set it in the config file,
// and values that would spin the loop or stretch the crash redelivery
// window past a minute are rejected.
func TestWP8ConsumerOffsetCommitInterval(t *testing.T) {
	cfg := Default()
	if got := cfg.Storage.ConsumerOffsetCommitIntervalMs; got != 100 {
		t.Fatalf("default consumer_offset_commit_interval_ms = %d, want 100", got)
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
	if c.ConsumerOffsetCommitIntervalMs != 100 {
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
