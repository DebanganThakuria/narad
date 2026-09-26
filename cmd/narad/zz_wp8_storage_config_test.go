package main

import (
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/config"
)

// TestWP8ConsumerOffsetCommitIntervalIsItsOwnKnob pins the wiring: the
// offset committer runs on storage.consumer_offset_commit_interval_ms,
// not on the storage flush interval it used to share.
func TestWP8ConsumerOffsetCommitIntervalIsItsOwnKnob(t *testing.T) {
	sc := config.Default().Storage
	sc.FlushIntervalMs = 20
	sc.ConsumerOffsetCommitIntervalMs = 250
	if got := consumerOffsetCommitInterval(sc); got != 250*time.Millisecond {
		t.Fatalf("consumerOffsetCommitInterval = %v, want 250ms (flush interval 20ms must not leak in)", got)
	}
	if got := consumerOffsetCommitInterval(config.Default().Storage); got != 100*time.Millisecond {
		t.Fatalf("default consumerOffsetCommitInterval = %v, want 100ms", got)
	}
}
