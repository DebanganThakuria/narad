package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// A link's target check is single-flight, but a cursor that needs no
// fresh verdict (the interval check is merely due) must not wait for a
// check in flight against a slow target: it reads the last verdict and
// sends. A cursor that does need a fresh one waits for it only as long
// as its context lives.
func TestRemoteLinkLanesDoNotWaitOnAnIntervalCheckInFlight(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{partitions: 1}})
	rg.src.start()
	defer rg.src.stop()
	rg.waitDelivered(t, rg.src.produce(t, 0, 5, 2, 0), 15*time.Second)
	rg.waitState(t, 0, topic.RemoteStateRunning, 10*time.Second)

	ctx := context.Background()
	cur := rg.src.runner.remoteCursorFor("orders", 0, "orders-to-b")
	if cur == nil {
		t.Fatal("no remote cursor")
	}
	s := rg.src.runner.sender()
	e, rs, state := s.entry("b")
	if state != "" {
		t.Fatalf("lookup state %q", state)
	}
	stub, err := rg.src.store.GetTopic(ctx, "orders-to-b")
	if err != nil {
		t.Fatal(err)
	}
	// A verdict for this entry, gate epoch and target is published.
	cur.forceTargetCheck()
	if ok, _, _ := s.checkTarget(ctx, ctx, cur, e, rs, stub); !ok {
		t.Fatal("the first check stopped sending")
	}

	// The interval check is due and the target answers slowly.
	rg.target.faults.slowDelay = 3 * time.Second
	rg.target.faults.set("slow")
	defer rg.target.faults.set("")
	expireInterval := func() {
		cur.check.mu.Lock()
		cur.check.nextCheck = time.Time{}
		cur.check.mu.Unlock()
	}
	expireInterval()
	before := rg.target.faults.listings.Load()
	inflight := make(chan struct{})
	go func() {
		defer close(inflight)
		s.checkTarget(ctx, ctx, cur, e, rs, stub)
	}()
	rigWait(t, "the interval check to reach the target", 10*time.Second, func() bool {
		return rg.target.faults.listings.Load() > before
	})
	expireInterval()

	start := time.Now()
	ok, _, ran := s.checkTarget(ctx, ctx, cur, e, rs, stub)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a lane waited %s on an interval check in flight", took)
	}
	if !ok || ran {
		t.Fatalf("checkTarget = (ok %v, ran %v), want the published verdict (true, false)", ok, ran)
	}

	// A forced check waits for a fresh verdict, but no longer than its
	// own context.
	cur.forceTargetCheck()
	wctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start = time.Now()
	if ok, _, _ := s.checkTarget(ctx, wctx, cur, e, rs, stub); ok {
		t.Fatal("a forced check whose wait ended reported sending may go on")
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a forced check waited %s past its context", took)
	}
	<-inflight
}
