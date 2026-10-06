package cluster

// A quiet remote link keeps its state honest without a record to send:
// the runtime target check runs on its own schedule, and the lookup's
// state and a failed check show in the listing.

import (
	"context"
	"testing"
	"time"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote/sink"
)

func TestIdleRemoteLinkKeepsItsTargetCheckAndSaysWhy(t *testing.T) {
	limits := domremote.DefaultLimits()
	limits.CheckIntervalMs = 1000
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{partitions: 1}, limits: limits})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 5, 2, 0), 15*time.Second)
	first := rg.waitState(t, 0, topic.RemoteStateRunning, 10*time.Second)

	// Idle and healthy: target_verified_at keeps moving.
	rigWait(t, "a fresh target check on the idle link", 10*time.Second, func() bool {
		snap, ok := rg.src.cursorState(0)
		return ok && snap.state == topic.RemoteStateRunning && snap.verifiedMs > first.verifiedMs
	})

	// The remote deleted (with force): the idle link says so at once.
	rg.src.lookup.Delete("b")
	rg.waitState(t, 0, topic.RemoteStateRemoteMissing, 10*time.Second)
	rg.src.lookup.Set(rg.entry)
	rg.waitState(t, 0, topic.RemoteStateRunning, 10*time.Second)

	// The target topic deleted on B: target_missing, still idle.
	ctx := context.Background()
	if err := rg.target.broker.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("delete the target topic: %v", err)
	}
	rg.waitState(t, 0, topic.RemoteStateTargetMissing, 10*time.Second)

	// Recreated: a new ID, so the link stops in target_replaced before
	// anything is sent to it.
	rg.target.createTopic(t, "orders", 3)
	rg.waitState(t, 0, topic.RemoteStateTargetReplaced, 10*time.Second)
	rg.src.produce(t, 0, 3, 2, 100)
	time.Sleep(2 * time.Second)
	if got := rg.target.records(t, "orders"); len(got) != 0 {
		t.Fatalf("%d records reached the recreated target without accept_target", len(got))
	}
	if snap, _ := rg.src.cursorState(0); snap.state != topic.RemoteStateTargetReplaced {
		t.Fatalf("state %s with records waiting, want target_replaced", snap.state)
	}
}

// Quiet cursors' target checks are requests to the remote like chunks:
// they go through its gate. A wrong password then costs the target one
// failed login per node per gate backoff, not one per cursor per check
// interval, and every quiet cursor still says why the link is stuck.
func TestIdleRemoteChecksGoThroughTheGate(t *testing.T) {
	limits := domremote.DefaultLimits()
	limits.CheckIntervalMs = 1000
	const partitions = 8
	rg := newRemoteRig(t, remoteRigOpts{
		rigSourceOpts: rigSourceOpts{partitions: partitions},
		password:      "a-wrong-password-0123456789", limits: limits,
	})
	rg.src.start()
	defer rg.src.stop()
	for p := range partitions {
		rg.waitState(t, p, topic.RemoteStateAuthFailed, 15*time.Second)
	}
	before := rg.target.faults.listings.Load()
	time.Sleep(5 * time.Second)
	if n := rg.target.faults.listings.Load() - before; n > 1 {
		t.Fatalf("%d credentialed target checks in 5s from %d quiet cursors with a wrong password; want at most one per gate backoff", n, partitions)
	}
	for p := range partitions {
		if snap, _ := rg.src.cursorState(p); snap.state != topic.RemoteStateAuthFailed {
			t.Fatalf("partition %d shows %s, want auth_failed", p, snap.state)
		}
	}
}

// The runtime target check is one per link per node, not one per
// cursor: a link's cursors share its schedule and its verdict, so a
// steady interval and a gate that closed and reopened each cost the
// target one children listing from this node, however many partitions
// the link has. A failed check that was not the gate's probe never
// counts toward closing the gate.
func TestRemoteLinkTargetCheckRunsOncePerNodeNotPerCursor(t *testing.T) {
	limits := domremote.DefaultLimits()
	limits.CheckIntervalMs = 2000
	const partitions = 8
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{partitions: partitions}, limits: limits})
	rg.src.start()
	defer rg.src.stop()
	for p := range partitions {
		rg.waitState(t, p, topic.RemoteStateRunning, 15*time.Second)
	}
	before := rg.target.faults.listings.Load()
	time.Sleep(5 * time.Second)
	// One check every 1.6 to 2.4 s: at most four in 5 s.
	if n := rg.target.faults.listings.Load() - before; n > 4 {
		t.Fatalf("%d target checks in 5s from %d cursors of one link at a 2 s interval; want one per interval", n, partitions)
	}

	rs := rg.src.runner.sender().remoteState("b")
	unavailable := sink.Verdict{Action: sink.ActRetry, State: topic.RemoteStateUnavailable, Class: topic.RemoteStateUnavailable}
	before = rg.target.faults.listings.Load()
	for range sink.GateTripAfter {
		rs.gate.Failed(unavailable, false)
	}
	rigWait(t, "the gate to reopen", 10*time.Second, func() bool { return !rs.gate.Closed() })
	time.Sleep(500 * time.Millisecond)
	// The reopen's check, and at most one interval check beside it.
	if n := rg.target.faults.listings.Load() - before; n > 2 {
		t.Fatalf("%d target checks after the gate reopened, from %d cursors of one link; want one", n, partitions)
	}
	for p := range partitions {
		rg.waitState(t, p, topic.RemoteStateRunning, 5*time.Second)
	}
}
