package metrics

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func writeBytes(dir, name string, size int) error {
	return os.WriteFile(filepath.Join(dir, name), bytes.Repeat([]byte("x"), size), 0o600)
}

// TestPollerDataDirScanIntervalIsConfigurable verifies the data-dir
// walk is rate-limited by the field (defaulting to the historical 30s),
// and that a zero interval walks on every tick.
func TestPollerDataDirScanIntervalIsConfigurable(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	dataDir := t.TempDir()
	writeFile := func(name string, size int) {
		t.Helper()
		if err := writeBytes(dataDir, name, size); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	writeFile("a.log", 11)

	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger(), dataDir)
	if p.DataDirScanInterval != defaultDataDirScanInterval {
		t.Fatalf("default DataDirScanInterval = %v, want %v", p.DataDirScanInterval, defaultDataDirScanInterval)
	}
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_data_dir_size_bytes", nil); got != 11 {
		t.Fatalf("data_dir_size_bytes = %v, want 11", got)
	}

	// Within the interval the gauge is not refreshed.
	writeFile("b.log", 5)
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_data_dir_size_bytes", nil); got != 11 {
		t.Fatalf("data_dir_size_bytes rescanned inside the interval: %v", got)
	}

	// A zero interval walks on every tick.
	p.DataDirScanInterval = 0
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_data_dir_size_bytes", nil); got != 16 {
		t.Fatalf("data_dir_size_bytes with zero interval = %v, want 16", got)
	}
}

// After a partition moves to another node, the old owner must stop
// exporting its last values for it, or a cluster-wide sum double
// counts and a stale lag looks like a stuck consumer. The topic itself
// lives on, so whole-topic pruning does not cover this.
func TestPollerClearsGaugesForPartitionsThatMovedAway(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	provider := &mutableSnapshotProvider{snaps: []TopicSnapshot{{
		Topic: "orders",
		Partitions: []PartitionSnapshot{
			{Partition: 0, HighWatermark: 10, SizeBytes: 5, InFlightSize: 1},
			{Partition: 1, HighWatermark: 20, SizeBytes: 7, InFlightSize: 2},
		},
	}}}
	p := NewPoller(m, provider, discardLogger())
	p.tick(context.Background())
	for _, partition := range []string{"0", "1"} {
		if !hasSeries(t, reg, "narad_consumer_lag_messages", "partition", partition) {
			t.Fatalf("first tick: lag series for partition %s missing", partition)
		}
	}

	// Partition 1 moves to another node; the topic stays.
	provider.snaps[0].Partitions = provider.snaps[0].Partitions[:1]
	p.tick(context.Background())
	for _, name := range []string{
		"narad_consumer_lag_messages", "narad_inflight_size", "narad_acked_ahead_size",
		"narad_oldest_unconsumed_message_age_seconds", "narad_partition_size_bytes",
		"narad_segments", "narad_consumer_dropped_messages",
	} {
		if hasSeries(t, reg, name, "partition", "1") {
			t.Errorf("%s{partition=1} still exported after the partition moved away", name)
		}
	}
	if got := readGauge(t, reg, "narad_consumer_lag_messages", map[string]string{"topic": "orders", "partition": "0"}); got != 10 {
		t.Fatalf("partition 0 lag = %v, want 10 (must survive its sibling's departure)", got)
	}

	// It comes back (moved here again): reported again.
	provider.snaps[0].Partitions = append(provider.snaps[0].Partitions, PartitionSnapshot{Partition: 1, HighWatermark: 3})
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_consumer_lag_messages", map[string]string{"topic": "orders", "partition": "1"}); got != 3 {
		t.Fatalf("returned partition lag = %v, want 3", got)
	}
}

// Lag is measured from the committed high watermark, not the log's
// next offset, which also counts buffered and hidden-tail records.
func TestPollerLagUsesHighWatermarkNotLogEnd(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, fakeSnapshotProvider{[]TopicSnapshot{{
		Topic:      "orders",
		Partitions: []PartitionSnapshot{{Partition: 0, LogEndOffset: 150, HighWatermark: 100, CommittedOffset: 40}},
	}}}, discardLogger())
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_consumer_lag_messages", map[string]string{"topic": "orders", "partition": "0"}); got != 60 {
		t.Fatalf("consumer_lag_messages = %v, want 60 (HWM 100 - committed 40), not 110 from the log end", got)
	}
}

// open_partition_logs was only written by the eviction sweep: with
// eviction disabled it stayed at 0 forever. The poller now sets it on
// every tick from the wired counter.
func TestPollerSetsOpenPartitionLogsEveryTick(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	open := 3
	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger())
	p.SetOpenLogCounter(func() int { return open })
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_open_partition_logs", nil); got != 3 {
		t.Fatalf("open_partition_logs = %v, want 3", got)
	}
	open = 1
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_open_partition_logs", nil); got != 1 {
		t.Fatalf("open_partition_logs after change = %v, want 1", got)
	}
}
