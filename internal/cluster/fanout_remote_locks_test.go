package cluster

// Lock discipline of a remote cursor's state: the listing never waits on
// the target, and concurrent publishers leave exactly one state series.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// The children listing reads every running remote cursor's snapshot (in
// process for local owners, through OpFanoutCursors for peers, whose RPC
// gives up after 5 s). A target check in flight against a slow target
// must not hold that snapshot: the listing is where a stalled link says
// why.
func TestRemoteChildListingDoesNotWaitOnATargetCheck(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 20, 3, 0), 15*time.Second)
	rg.waitState(t, 0, topic.RemoteStateRunning, 5*time.Second)

	cur := rg.src.runner.remoteCursorFor("orders", 0, "orders-to-b")
	if cur == nil {
		t.Fatal("no remote cursor")
	}
	rg.target.faults.slowDelay = 3 * time.Second
	rg.target.faults.set("slow")
	defer rg.target.faults.set("")
	before := rg.target.faults.listings.Load()
	cur.forceTargetCheck()
	rg.src.produce(t, 0, 5, 3, 100)
	rigWait(t, "the target check to reach the target", 10*time.Second, func() bool {
		return rg.target.faults.listings.Load() > before
	})
	time.Sleep(100 * time.Millisecond)

	stats, err := rg.src.broker.FanoutCursorStats(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	out := rg.src.runner.OverlayRemoteCursorStats("orders", stats)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the cursor-stats overlay took %s behind a target check in flight", took)
	}
	if len(out) == 0 || out[0].State == "" {
		t.Fatalf("overlay = %+v, want the cursor's state", out)
	}
	// The single-flight still holds: asking for a check while one is in
	// flight returns at once.
	start = time.Now()
	cur.forceTargetCheck()
	if took := time.Since(start); took > 100*time.Millisecond {
		t.Fatalf("forceTargetCheck waited %s on the check in flight", took)
	}
}

// Lanes, the lag refresher and the commit publish the cursor's state
// concurrently. Once the cursor settles, exactly one state series is 1.
func TestRemoteCursorStatePublishLeavesOneSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	r := &FanoutRunner{metrics: m, logger: rigLogger()}
	key := fanoutCursorKey{parent: "orders", partition: 0, child: "orders-to-b", remote: true}
	states := []string{topic.RemoteStateUnavailable, topic.RemoteStateAuthFailed, topic.RemoteStateForbidden, ""}
	for round := range 300 {
		cur := newRemoteCursor(key)
		var wg sync.WaitGroup
		for g := range 4 {
			wg.Go(func() {
				for i := range 4 {
					cur.setLane(g, states[(g+i+round)%len(states)])
					r.publishRemoteState(cur)
				}
			})
		}
		wg.Wait()
		for g := range 4 {
			cur.setLane(g, "")
		}
		r.publishRemoteState(cur)
		fams, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		var live []string
		for _, f := range fams {
			if f.GetName() != "narad_fanout_remote_state" {
				continue
			}
			for _, s := range f.GetMetric() {
				if s.GetGauge().GetValue() != 1 {
					continue
				}
				for _, lp := range s.GetLabel() {
					if lp.GetName() == "state" {
						live = append(live, lp.GetValue())
					}
				}
			}
		}
		if len(live) != 1 || live[0] != topic.RemoteStateRunning {
			t.Fatalf("round %d: state series at 1 = %v, want only running", round, live)
		}
		m.RemoteLink.PruneLink("orders", "orders-to-b")
	}
}
