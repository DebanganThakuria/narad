package metrics

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

// TestPollerSetsReaperRestartsEveryTick pins the wiring of
// narad_reaper_restarts: with no counter wired the gauge is left
// alone, and once wired it is copied from the source on every tick so a
// replacement loop started between polls shows up on the next scrape.
func TestPollerSetsReaperRestartsEveryTick(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger())
	m.ReaperRestarts.Set(7)
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_reaper_restarts", nil); got != 7 {
		t.Fatalf("reaper_restarts without a wired counter = %v, want 7 untouched", got)
	}
	restarts := int64(0)
	p.SetReaperRestartCounter(func() int64 { return restarts })
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_reaper_restarts", nil); got != 0 {
		t.Fatalf("reaper_restarts = %v, want 0", got)
	}
	restarts = 2
	p.tick(context.Background())
	if got := readGauge(t, reg, "narad_reaper_restarts", nil); got != 2 {
		t.Fatalf("reaper_restarts after a restart = %v, want 2", got)
	}
}

// The vital signs (WAL health and backlog, open logs, reaper restarts,
// free space) must keep following their sources while the broker
// Snapshot is blocked: that is when a stuck disk or a wedged partition
// is happening, and when an operator reads them. The inventory loop
// stays at its start stamp, so its frozen-poller alert can fire.
func TestPollerVitalsRefreshWhileSnapshotBlocks(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	release := make(chan struct{})
	p := NewPoller(m, blockingSnapshotProvider{release: release}, discardLogger(), t.TempDir())
	p.interval = 10 * time.Millisecond
	var healthy atomic.Bool
	healthy.Store(true)
	var open, free atomic.Int64
	open.Store(2)
	free.Store(4096)
	p.SetIngressWALHealth(healthy.Load)
	p.SetOpenLogCounter(func() int { return int(open.Load()) })
	p.statfs = func(string) (uint64, error) { return uint64(free.Load()), nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		close(release)
		<-done
	})

	inventory := map[string]string{"loop": "inventory"}
	vitals := map[string]string{"loop": "vitals"}
	inventoryStart := waitGauge(t, reg, "narad_poller_last_success_timestamp_seconds", inventory, func(v float64) bool { return v > 0 })
	vitalsStart := waitGauge(t, reg, "narad_poller_last_success_timestamp_seconds", vitals, func(v float64) bool { return v > 0 })

	healthy.Store(false)
	open.Store(9)
	free.Store(8192)
	waitGauge(t, reg, "narad_ingress_wal_failed", nil, func(v float64) bool { return v == 1 })
	waitGauge(t, reg, "narad_open_partition_logs", nil, func(v float64) bool { return v == 9 })
	waitGauge(t, reg, "narad_data_dir_available_bytes", nil, func(v float64) bool { return v == 8192 })
	waitGauge(t, reg, "narad_poller_last_success_timestamp_seconds", vitals, func(v float64) bool { return v > vitalsStart })
	if got := readGauge(t, reg, "narad_poller_last_success_timestamp_seconds", inventory); got != inventoryStart {
		t.Fatalf("inventory last success moved from %v to %v while its Snapshot is blocked", inventoryStart, got)
	}
}

// A failing Snapshot used to return before the open-log count, the
// reaper restarts and the free space were read, so they froze exactly
// while the node was in trouble.
func TestPollerVitalsRefreshWhenSnapshotFails(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, failingSnapshotProvider{}, discardLogger(), t.TempDir())
	open, restarts := 3, int64(1)
	p.SetOpenLogCounter(func() int { return open })
	p.SetReaperRestartCounter(func() int64 { return restarts })

	p.tick(context.Background())
	open, restarts = 7, 4
	p.tick(context.Background())

	if got := readGauge(t, reg, "narad_open_partition_logs", nil); got != 7 {
		t.Errorf("narad_open_partition_logs with Snapshot failing = %v, want 7", got)
	}
	if got := readGauge(t, reg, "narad_reaper_restarts", nil); got != 4 {
		t.Errorf("narad_reaper_restarts with Snapshot failing = %v, want 4", got)
	}
	if got := readGauge(t, reg, "narad_data_dir_available_bytes", nil); got <= 0 {
		t.Errorf("narad_data_dir_available_bytes with Snapshot failing = %v, want the volume's free space", got)
	}
}

// A statfs that never returns (a dead network mount, a wedged volume)
// must not hold up the other vital signs, must not start a second read
// while the first is stuck, and must keep the vitals loop's success
// stamp from moving so the frozen-poller alert tells the truth.
func TestPollerHungStatfsDoesNotFreezeOtherVitals(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger(), t.TempDir())
	p.vitalsDeadline = 50 * time.Millisecond
	unblock := make(chan struct{})
	var calls atomic.Int32
	p.statfs = func(string) (uint64, error) {
		calls.Add(1)
		<-unblock
		return 4096, nil
	}
	backlog := uint64(7)
	p.SetIngressDispatchBacklog(func() uint64 { return backlog })
	vitals := map[string]string{"loop": "vitals"}

	start := time.Now()
	p.vitalsTick(context.Background())
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a vitals pass took %v behind a hung statfs, want about the %v read deadline", took, p.vitalsDeadline)
	}
	if got := readGauge(t, reg, "narad_ingress_dispatch_backlog_records", nil); got != 7 {
		t.Fatalf("backlog behind a hung statfs = %v, want 7", got)
	}
	if hasGauge(t, reg, "narad_poller_last_success_timestamp_seconds", vitals) {
		t.Fatal("the vitals pass was stamped a success while statfs had not answered")
	}
	if got, _ := readCounter(t, reg, "narad_errors_total", map[string]string{"component": "metrics", "kind": "data_dir_available_timeout"}); got != 1 {
		t.Fatalf("statfs timeouts counted = %v, want 1", got)
	}

	backlog = 8
	p.vitalsTick(context.Background())
	if got := readGauge(t, reg, "narad_ingress_dispatch_backlog_records", nil); got != 8 {
		t.Fatalf("backlog on the second pass = %v, want 8", got)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("statfs started %d times while the first call hung, want 1", n)
	}

	close(unblock)
	deadline := time.Now().Add(5 * time.Second)
	for !hasGauge(t, reg, "narad_poller_last_success_timestamp_seconds", vitals) {
		if time.Now().After(deadline) {
			t.Fatal("the vitals loop never recorded a complete pass after statfs answered")
		}
		p.vitalsTick(context.Background())
	}
	if got := readGauge(t, reg, "narad_data_dir_available_bytes", nil); got != 4096 {
		t.Fatalf("available bytes after statfs answered = %v, want 4096", got)
	}
}

// A vital-sign source that panics is contained to its own read: the
// other gauges are still set, the panic is counted, and the pass is not
// a success.
func TestPollerRecoversAPanickingVitalSource(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	p := NewPoller(m, fakeSnapshotProvider{}, discardLogger())
	p.SetOpenLogCounter(func() int { panic("open log table torn") })
	p.SetIngressDispatchBacklog(func() uint64 { return 3 })

	p.tick(context.Background())

	if got := readGauge(t, reg, "narad_ingress_dispatch_backlog_records", nil); got != 3 {
		t.Fatalf("backlog next to a panicking source = %v, want 3", got)
	}
	if got, _ := readCounter(t, reg, "narad_errors_total", map[string]string{"component": "metrics", "kind": "open_logs_panic"}); got != 1 {
		t.Fatalf("panics counted = %v, want 1", got)
	}
	if hasGauge(t, reg, "narad_poller_last_success_timestamp_seconds", map[string]string{"loop": "vitals"}) {
		t.Fatal("the vitals pass was stamped a success although a source panicked")
	}
	if !hasGauge(t, reg, "narad_poller_last_success_timestamp_seconds", map[string]string{"loop": "inventory"}) {
		t.Fatal("the inventory pass was not stamped although its Snapshot answered")
	}
}

type blockingSnapshotProvider struct{ release chan struct{} }

func (b blockingSnapshotProvider) Snapshot(ctx context.Context) ([]TopicSnapshot, error) {
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return nil, ctx.Err()
}

type failingSnapshotProvider struct{}

func (failingSnapshotProvider) Snapshot(context.Context) ([]TopicSnapshot, error) {
	return nil, errors.New("list topics: metastore read failed")
}

// hasGauge reports whether a gauge series with these labels exists.
func hasGauge(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) bool {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, met := range mf.GetMetric() {
			if labelsMatch(met.GetLabel(), labels) && met.GetGauge() != nil {
				return true
			}
		}
	}
	return false
}

// waitGauge waits up to 5 s for the gauge to satisfy ok and returns it.
func waitGauge(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string, ok func(float64) bool) float64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if hasGauge(t, reg, name, labels) {
			if v := readGauge(t, reg, name, labels); ok(v) {
				return v
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s%v never reached the wanted value within 5s", name, labels)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
