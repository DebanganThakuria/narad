package metastore

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// gathered returns every sample of the named family, keyed by its label
// values joined with ",", "" for a series without labels.
func gathered(t *testing.T, g prometheus.Gatherer, name string) map[string]float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]float64{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			var labels []string
			for _, lp := range m.GetLabel() {
				labels = append(labels, lp.GetValue())
			}
			out[strings.Join(labels, ",")] = sampleValue(m)
		}
	}
	return out
}

func sampleValue(m *dto.Metric) float64 {
	switch {
	case m.GetGauge() != nil:
		return m.GetGauge().GetValue()
	case m.GetCounter() != nil:
		return m.GetCounter().GetValue()
	default:
		return 0
	}
}

// one returns the value of a series without labels and fails the test
// when it is missing.
func one(t *testing.T, g prometheus.Gatherer, name string) float64 {
	t.Helper()
	v, ok := gathered(t, g, name)[""]
	if !ok {
		t.Fatalf("%s is not exported", name)
	}
	return v
}

var metastoreSeries = []string{
	"narad_metastore_fsm_bytes",
	"narad_metastore_applied_index",
	"narad_metastore_apply_errors_total",
	"narad_metastore_apply_stalled",
	"narad_metastore_apply_stopped",
	"narad_metastore_snapshot_bytes",
	"narad_metastore_snapshot_duration_seconds",
	"narad_metastore_snapshot_failures_total",
}

var raftSeries = []string{
	"narad_raft_state",
	"narad_raft_term",
	"narad_raft_last_log_index",
	"narad_raft_commit_index",
	"narad_raft_applied_index",
	"narad_raft_fsm_pending",
	"narad_raft_has_leader",
	"narad_raft_last_contact_seconds",
	"narad_raft_voters",
	"narad_raft_nonvoters",
}

func TestMetastoreMetricsAreExported(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := singleNodeConfig(t)
	cfg.Registerer = reg
	h := openStore(t, cfg)
	if err := h.s.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}

	for _, name := range metastoreSeries {
		if len(gathered(t, reg, name)) == 0 {
			t.Errorf("%s is not exported", name)
		}
	}
	if v := one(t, reg, "narad_metastore_fsm_bytes"); v <= 0 {
		t.Fatalf("narad_metastore_fsm_bytes = %v, want the size of fsm.db", v)
	}
	if v := one(t, reg, "narad_metastore_applied_index"); v < 1 || v != float64(h.s.AppliedIndex()) {
		t.Fatalf("narad_metastore_applied_index = %v, want AppliedIndex %d", v, h.s.AppliedIndex())
	}
	if v := one(t, reg, "narad_metastore_snapshot_bytes"); v != 0 {
		t.Fatalf("narad_metastore_snapshot_bytes = %v before any snapshot, want 0", v)
	}
	if err := h.s.r.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if v := one(t, reg, "narad_metastore_snapshot_bytes"); v <= 0 {
		t.Fatalf("narad_metastore_snapshot_bytes = %v after a snapshot, want its size", v)
	}
	if v := one(t, reg, "narad_metastore_snapshot_duration_seconds"); v <= 0 {
		t.Fatalf("narad_metastore_snapshot_duration_seconds = %v after a snapshot, want > 0", v)
	}
	for _, name := range []string{"narad_metastore_apply_stalled", "narad_metastore_apply_stopped", "narad_metastore_snapshot_failures_total"} {
		if v := one(t, reg, name); v != 0 {
			t.Fatalf("%s = %v on a healthy store, want 0", name, v)
		}
	}
	kinds := gathered(t, reg, "narad_metastore_apply_errors_total")
	for _, kind := range []string{"storage", "unknown_entry_type", "undecodable", "newer_database"} {
		if v, ok := kinds[kind]; !ok || v != 0 {
			t.Fatalf("narad_metastore_apply_errors_total = %v, want every kind present at 0", kinds)
		}
	}

	// An entry type this build does not know stops the FSM.
	f := h.s.r.Apply(unknownEntry(), 5*time.Second)
	if err := f.Error(); err != nil {
		t.Fatalf("propose: %v", err)
	}
	if err, _ := f.Response().(error); !errors.Is(err, ErrStoppedApplying) {
		t.Fatalf("unknown entry answered %v, want ErrStoppedApplying", err)
	}
	if v := gathered(t, reg, "narad_metastore_apply_errors_total")["unknown_entry_type"]; v != 1 {
		t.Fatalf("narad_metastore_apply_errors_total{kind=unknown_entry_type} = %v, want 1", v)
	}
	if v := one(t, reg, "narad_metastore_apply_stopped"); v != 1 {
		t.Fatalf("narad_metastore_apply_stopped = %v after the FSM stopped, want 1", v)
	}
	waitUntil(t, 10*time.Second, "narad_raft_state to report the shutdown", func() bool {
		return gathered(t, reg, "narad_raft_state")["shutdown"] == 1
	})
}

func TestRaftMetricsReportStateTermIndexesAndLeader(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := singleNodeConfig(t)
	cfg.Registerer = reg
	h := openStore(t, cfg)
	if err := h.s.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}

	for _, name := range raftSeries {
		if len(gathered(t, reg, name)) == 0 {
			t.Errorf("%s is not exported", name)
		}
	}
	state := gathered(t, reg, "narad_raft_state")
	if len(state) != 4 || state["leader"] != 1 || state["follower"] != 0 || state["candidate"] != 0 || state["shutdown"] != 0 {
		t.Fatalf("narad_raft_state = %v, want leader=1 and the other three 0", state)
	}
	for _, name := range []string{"narad_raft_term", "narad_raft_last_log_index", "narad_raft_commit_index", "narad_raft_applied_index"} {
		if v := one(t, reg, name); v < 1 {
			t.Fatalf("%s = %v, want >= 1", name, v)
		}
	}
	if last, commit := one(t, reg, "narad_raft_last_log_index"), one(t, reg, "narad_raft_commit_index"); last < commit {
		t.Fatalf("narad_raft_last_log_index %v < narad_raft_commit_index %v", last, commit)
	}
	if v := one(t, reg, "narad_raft_fsm_pending"); v < 0 {
		t.Fatalf("narad_raft_fsm_pending = %v", v)
	}
	for name, want := range map[string]float64{
		"narad_raft_has_leader":           1,
		"narad_raft_last_contact_seconds": 0,
		"narad_raft_voters":               1,
		"narad_raft_nonvoters":            0,
	} {
		if v := one(t, reg, name); v != want {
			t.Fatalf("%s = %v on a single-node leader, want %v", name, v, want)
		}
	}
}

// A node that has never heard from a leader (a joiner nobody has added
// yet) reports no leader, and a last contact as old as the store.
func TestRaftMetricsOnANodeThatNeverHeardALeader(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := singleNodeConfig(t)
	cfg.JoinOnly = true
	cfg.Registerer = reg
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	time.Sleep(200 * time.Millisecond)

	if v := gathered(t, reg, "narad_raft_state")["follower"]; v != 1 {
		t.Fatalf("narad_raft_state{state=follower} = %v, want 1", v)
	}
	if v := one(t, reg, "narad_raft_has_leader"); v != 0 {
		t.Fatalf("narad_raft_has_leader = %v, want 0", v)
	}
	if v := one(t, reg, "narad_raft_last_contact_seconds"); v < 0.2 {
		t.Fatalf("narad_raft_last_contact_seconds = %v, want at least the 0.2 s since the store opened", v)
	}
	if v := one(t, reg, "narad_raft_voters"); v != 0 {
		t.Fatalf("narad_raft_voters = %v with no configuration, want 0", v)
	}
}

// Without a Registerer the store exports nothing, and in particular
// does not fall back to Prometheus's default registry.
func TestMetricsAreOffWithoutARegisterer(t *testing.T) {
	h := openStore(t, singleNodeConfig(t))
	if err := h.s.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	for _, name := range append(append([]string{}, metastoreSeries...), raftSeries...) {
		if got := gathered(t, prometheus.DefaultGatherer, name); len(got) != 0 {
			t.Fatalf("%s landed in the default registry: %v", name, got)
		}
	}
}

// A second store in one process (an embedded restart, tests) cannot
// register the same series again: it logs that and opens all the same.
// Close takes a store's series off the registry, so the next store can
// register its own.
func TestSecondStoreOnOneRegistryIsNotFatal(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	first := singleNodeConfig(t)
	first.Registerer = reg
	h1 := openStore(t, first)

	logs := &lockedBuffer{}
	second := singleNodeConfig(t)
	second.NodeID = "solo-2"
	second.Registerer = reg
	second.Log = slog.New(slog.NewTextHandler(logs, nil))
	h2 := openStore(t, second)
	logs.mu.Lock()
	logged := logs.buf.String()
	logs.mu.Unlock()
	if !strings.Contains(logged, "level=ERROR") || !strings.Contains(logged, "metrics") {
		t.Fatalf("the second store did not log at error that its metrics are missing; its log:\n%s", logged)
	}
	if v := one(t, reg, "narad_metastore_applied_index"); v != float64(h1.s.AppliedIndex()) {
		t.Fatalf("narad_metastore_applied_index = %v, want the first store's %d", v, h1.s.AppliedIndex())
	}

	h1.close(t)
	h2.close(t)
	if got := gathered(t, reg, "narad_raft_state"); len(got) != 0 {
		t.Fatalf("closed stores still export narad_raft_state: %v", got)
	}
	third := singleNodeConfig(t)
	third.Registerer = reg
	openStore(t, third)
	if v := gathered(t, reg, "narad_raft_state")["leader"]; v != 1 {
		t.Fatalf("a store opened after the others closed does not export its state (narad_raft_state{state=leader} = %v)", v)
	}
}
