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

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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
