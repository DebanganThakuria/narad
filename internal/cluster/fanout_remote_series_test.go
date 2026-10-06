package cluster

// A remote cursor that stops while its link is still live (its parent
// partition moved to another owner) drops the per-partition series it
// exported, so a stalled state never pages from a node that no longer
// runs the partition.

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

type remoteSeries struct {
	partition, state string
	value            float64
}

// gatherRemoteSeries reads one of the per-partition remote link gauges.
func gatherRemoteSeries(t *testing.T, rg *remoteRig, name string) []remoteSeries {
	t.Helper()
	reg := prometheus.NewRegistry()
	rl := rg.src.metrics.RemoteLink
	reg.MustRegister(rl.State, rl.LagSeconds, rl.RetentionHeadroomSeconds)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var out []remoteSeries
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			s := remoteSeries{value: m.GetGauge().GetValue()}
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "partition":
					s.partition = lp.GetValue()
				case "state":
					s.state = lp.GetValue()
				}
			}
			out = append(out, s)
		}
	}
	return out
}

func TestMovedPartitionDropsItsRemoteSeries(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 20, 3, 0), 15*time.Second)

	rg.target.faults.set("down")
	rg.src.produce(t, 0, 20, 3, 100)
	rg.waitState(t, 0, topic.RemoteStateUnavailable, 15*time.Second)
	rigWait(t, "the unavailable series", 10*time.Second, func() bool {
		for _, s := range gatherRemoteSeries(t, rg, "narad_fanout_remote_state") {
			if s.state == topic.RemoteStateUnavailable && s.value == 1 {
				return true
			}
		}
		return false
	})

	ctx := context.Background()
	if err := rg.src.store.RegisterMember(ctx, metastore.Member{ID: "node-other", Addr: "127.0.0.1:2", Status: metastore.MemberAlive, Build: "narad test", EntryTypes: metastore.MaxEntryType, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := rg.src.store.AssignPartition(ctx, "orders", 0, "node-other"); err != nil {
		t.Fatal(err)
	}
	rigWait(t, "this node's cursor to stop", 20*time.Second, func() bool {
		return rg.src.runner.remoteCursorFor("orders", 0, "orders-to-b") == nil
	})
	for _, name := range []string{"narad_fanout_remote_state", "narad_fanout_remote_lag_seconds", "narad_fanout_remote_retention_headroom_seconds"} {
		for _, s := range gatherRemoteSeries(t, rg, name) {
			if s.partition == "0" {
				t.Fatalf("partition 0 moved away and its cursor stopped, yet this node still exports %s %+v", name, s)
			}
		}
	}
}

// gatherRemoteGauge reads a per-remote gauge or counter of one remote:
// its value and whether the series exists.
func gatherRemoteGauge(t *testing.T, c prometheus.Collector, name, remoteName string) (float64, bool) {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(c)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "remote" && lp.GetValue() == remoteName {
					if m.GetGauge() != nil {
						return m.GetGauge().GetValue(), true
					}
					return m.GetCounter().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// narad_remote_gate_backoff_seconds follows the remote's gate: above 0
// while the gate backs off, 0 once the remote answers again.
func TestRemoteGateBackoffIsExported(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 10, 3, 0), 15*time.Second)
	gauge := rg.src.metrics.RemoteLink.GateBackoffSeconds
	rg.target.faults.set("down")
	rg.src.produce(t, 0, 10, 3, 100)
	rigWait(t, "a gate backoff above 0", 20*time.Second, func() bool {
		v, ok := gatherRemoteGauge(t, gauge, "narad_remote_gate_backoff_seconds", "b")
		return ok && v > 0
	})
	rg.target.faults.set("")
	rigWait(t, "the gate backoff back at 0", 30*time.Second, func() bool {
		v, ok := gatherRemoteGauge(t, gauge, "narad_remote_gate_backoff_seconds", "b")
		return ok && v == 0
	})
}

// A remote that disappears from the registry takes its per-remote series
// and this node's sender state for it along.
func TestDeletedRemoteDropsItsSeriesAndSenderState(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 10, 3, 0), 15*time.Second)
	rl := rg.src.metrics.RemoteLink
	if _, ok := gatherRemoteGauge(t, rl.ChunkBytesLimit, "narad_remote_chunk_bytes_limit", "b"); !ok {
		t.Fatal("precondition: no chunk_bytes_limit series for b")
	}
	// The link goes first (a detach), then the remote.
	ctx := context.Background()
	if err := rg.src.store.DetachChild(ctx, "orders", "orders-to-b"); err != nil {
		t.Fatal(err)
	}
	rigWait(t, "the cursor to stop", 20*time.Second, func() bool {
		return rg.src.runner.remoteCursorFor("orders", 0, "orders-to-b") == nil
	})
	rg.src.lookup.Delete("b")
	rigWait(t, "b's series and state to go", 10*time.Second, func() bool {
		_, chunk := gatherRemoteGauge(t, rl.ChunkBytesLimit, "narad_remote_chunk_bytes_limit", "b")
		_, wire := gatherRemoteGauge(t, rl.WireBytesTotal, "narad_remote_wire_bytes_total", "b")
		return !chunk && !wire && rg.src.runner.sender().remoteStateIfAny("b") == nil
	})
}

// The link's last-success gauge carries the newest success among this
// node's cursors of the link: a quiet partition re-publishing an
// hour-old success never pulls it back while another partition ships.
// It goes once the node's last cursor of the link stops, so a node the
// link's partitions moved off exports no frozen timestamp.
func TestRemoteLastSuccessIsTheNewestOfTheNodesCursors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	r := &FanoutRunner{metrics: m, logger: rigLogger()}
	key := fanoutCursorKey{parent: "orders", child: "orders-to-b", epoch: "e1", remote: true}
	quietKey, busyKey := key, key
	quietKey.partition, busyKey.partition = 4, 3
	quiet, forgetQuiet := r.registerRemoteCursor(quietKey)
	busy, forgetBusy := r.registerRemoteCursor(busyKey)
	now := time.Now()
	quiet.succeeded(now.Add(-time.Hour))
	busy.succeeded(now)
	r.publishRemoteState(busy)
	r.publishRemoteState(quiet)
	gauge := m.RemoteLink.LastSuccessTimestampSeconds.WithLabelValues("orders", "orders-to-b")
	if got, want := testutil.ToFloat64(gauge), float64(now.UnixMilli())/1000; got != want {
		t.Fatalf("last success = %v after the quiet partition published, want the busy one's %v", got, want)
	}
	forgetBusy()
	if n := testutil.CollectAndCount(m.RemoteLink.LastSuccessTimestampSeconds); n != 1 {
		t.Fatalf("%d last-success series with a cursor of the link still running, want 1", n)
	}
	forgetQuiet()
	if n := testutil.CollectAndCount(m.RemoteLink.LastSuccessTimestampSeconds); n != 0 {
		t.Fatalf("%d last-success series after the node's last cursor of the link stopped, want 0", n)
	}
}
