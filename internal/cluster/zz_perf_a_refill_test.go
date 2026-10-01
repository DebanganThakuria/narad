package cluster

import (
	"context"
	"slices"
	"testing"
	"time"
)

// A hot destination's commit takes its whole queue, so its next batch
// used to be read from the WAL only once the commit had landed (the
// rescan it asks for then), a read's worth of dead time per commit. A
// commit that leaves its destination's queue short with records still
// skipped now asks for a rescan at once, and Run wakes for it at once,
// so the next batch is queued, full, while the commit runs. Run's steps
// are driven one at a time on a settable clock, with the backstop rescan
// not due.
func TestPerfARefillOverlapsCommit(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	seedProduceDispatchTopicPartitions(t, store, "node-self", 1)
	m := zzWP6Manager(t)
	sink := newZZPerfAGateSink()
	d := NewProduceDispatcher(m, store, "node-self", sink, nil, nil, ProduceDispatcherConfig{})
	clock := &zzWP6Clock{now: time.Unix(1_700_000_000, 0)}
	d.now = clock.Now
	if err := d.loadCursor(); err != nil {
		t.Fatal(err)
	}
	st := d.state
	capacity := d.perDestCap(st)
	parts := zzPerfARepeat(0, 4*capacity)
	zzPerfAAccept(t, m, 0, parts)

	// The first queue goes out; when it lands, the rescan it asks for
	// fills the next one, which goes out in the same round. That rescan
	// also sets lastRescan, so the backstop is a second away from here.
	d.step(ctx, st)
	sink.waitCalls(t, 0)
	sink.release <- struct{}{}
	zzShipSlowMerge(t, d)
	d.step(ctx, st)
	sink.waitCalls(t, 0)
	dest := st.dests[dispatchDestKey{topic: "orders", partition: 0}]
	if dest == nil || !dest.inflight || len(dest.queue) != 0 || dest.skipped == 0 {
		t.Fatalf("want the second batch in flight with an empty queue and records still skipped, got %+v", dest)
	}

	// The commit that just started asked for a rescan, and Run would not
	// sleep before it.
	if !st.rescanDue {
		t.Fatal("no rescan due after a commit left its destination's queue empty with records still skipped")
	}
	if wake := d.nextWake(st); wake != 0 {
		t.Fatalf("nextWake = %v with a rescan due, want 0", wake)
	}

	// The next round refills the queue while the commit is still in
	// flight, so a full batch is ready the moment it lands.
	d.step(ctx, st)
	if !dest.inflight || len(dest.queue) != capacity {
		t.Fatalf("while the commit runs: inflight=%v queue=%d, want the next batch queued in full (%d)", dest.inflight, len(dest.queue), capacity)
	}
	if st.rescanDue {
		t.Fatal("a rescan is still due after the round that refilled the queue: Run would spin")
	}
	sink.release <- struct{}{}
	zzShipSlowMerge(t, d)
	d.step(ctx, st)
	sink.waitCalls(t, 0)
	for job := range st.jobs {
		if len(job.records) != capacity {
			t.Fatalf("the batch sent once the commit landed carried %d records, want a full queue (%d)", len(job.records), capacity)
		}
	}

	// Everything drains, exactly once and in order.
	close(sink.open)
	zzPerfAStepUntilDone(t, d, clock)
	if got := sink.committed()[0]; !slices.Equal(got, zzPerfAWant(parts)[0]) {
		t.Fatalf("committed %d records, want %d exactly once in order (first mismatch near %v)",
			len(got), len(parts), zzPerfAFirstDiff(got, zzPerfAWant(parts)[0]))
	}
}
