package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/remote/sink"
)

// laneRecords are n records at offsets from base, as a slab carries them.
func laneRecords(base, n int) []topic.KeyedRecord {
	recs := make([]topic.KeyedRecord, n)
	for i := range recs {
		recs[i] = topic.KeyedRecord{Offset: int64(base + i), Key: fmt.Sprintf("k%d", i%4), Payload: fmt.Appendf(nil, `{"seq":%d}`, base+i), CommittedAtUnixMs: time.Now().UnixMilli()}
	}
	return recs
}

// laneRig is a remote rig with a running, idle cursor of partition 0,
// whose sender a test drives with commit directly.
func laneRig(t *testing.T, o remoteRigOpts) (*remoteRig, *remoteCursor) {
	t.Helper()
	rg := newRemoteRig(t, o)
	rg.src.start()
	t.Cleanup(rg.src.stop)
	var cur *remoteCursor
	rigWait(t, "the remote cursor", 10*time.Second, func() bool {
		cur = rg.src.runner.remoteCursorFor("orders", 0, "orders-to-b")
		return cur != nil
	})
	return rg, cur
}

type commitResult struct {
	remaining []topic.KeyedRecord
	reread    bool
}

func (rg *remoteRig) commitAsync(ctx context.Context, cur *remoteCursor, recs []topic.KeyedRecord) <-chan commitResult {
	version := rg.src.store.TopicVersion("orders-to-b")
	stub, _ := rg.src.store.GetTopic(context.Background(), "orders-to-b")
	out := make(chan commitResult, 1)
	go func() {
		remaining, reread := rg.src.runner.sender().commit(ctx, cur.key, stub, version, recs)
		out <- commitResult{remaining, reread}
	}()
	return out
}

// A pause that lands while a lane holds its records (the target is
// down) ends the slab: the records leave the held budget for the whole
// pause instead of starving every other link on the node, with no
// re-read counted and no error logged for a planned pause.
func TestRemoteChildPauseMidSlabHoldsNothing(t *testing.T) {
	rg, cur := laneRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 30 * time.Second}})
	rg.target.faults.set("down")
	held := rg.src.runner.sender().held
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := rg.commitAsync(ctx, cur, laneRecords(1000, 20))
	rigWait(t, "the lane to hold its records", 10*time.Second, func() bool { return held.Used() > 0 })
	rg.setState(t, metastore.RemoteChildStateOp{Pause: &metastore.RemotePauseState{Paused: true, Reason: "maintenance"}})
	select {
	case res := <-done:
		if !res.reread || len(res.remaining) != 0 {
			t.Fatalf("commit after a pause = %d remaining, reread %v; want the slab ended for a re-read", len(res.remaining), res.reread)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the slab kept holding its records across the pause")
	}
	if n := held.Used(); n != 0 {
		t.Fatalf("held bytes after the pause = %d, want 0", n)
	}
	if v := counterValue(rg.src.metrics.RemoteLink.RereadsTotal.WithLabelValues("orders", "orders-to-b")); v != 0 {
		t.Fatalf("a pause counted %v re-reads", v)
	}
}

// Lanes that wait on a closed gate hold their records in the budget
// first, so an outage does not pin parent log frames outside
// max_held_bytes and held_bytes shows what the node keeps.
func TestRemoteChildLanesWaitingOnTheGateHoldTheirRecords(t *testing.T) {
	rg, cur := laneRig(t, remoteRigOpts{lanes: 4})
	rs := rg.src.runner.sender().remoteState("b")
	// Closed for the 30 s auth ceiling: no probe is due during the test.
	rs.gate.Failed(sink.Verdict{Action: sink.ActGate, State: topic.RemoteStateAuthFailed, Class: topic.RemoteStateAuthFailed}, false)
	held := rg.src.runner.sender().held
	recs := laneRecords(1000, 40)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := rg.commitAsync(ctx, cur, recs)
	want := sink.HeldSize(recs)
	rigWait(t, "every waiting lane to hold", 5*time.Second, func() bool { return held.Used() == want })
	cancel()
	<-done
	if n := held.Used(); n != 0 {
		t.Fatalf("held bytes after the slab ended = %d, want 0", n)
	}
}

// A lane parked on the gate when a pause is applied sends nothing once
// the probe falls due: the pause ends the slab.
func TestRemoteChildLaneParkedOnTheGateSendsNothingAfterAPause(t *testing.T) {
	rg, cur := laneRig(t, remoteRigOpts{rigSourceOpts: rigSourceOpts{stallRetry: 30 * time.Second}})
	rs := rg.src.runner.sender().remoteState("b")
	rs.gate.Failed(sink.Verdict{Action: sink.ActGate, State: topic.RemoteStateThrottled, Class: topic.RemoteStateThrottled, RetryAfter: 1500 * time.Millisecond}, false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := rg.commitAsync(ctx, cur, laneRecords(1000, 5))
	time.Sleep(200 * time.Millisecond)
	rg.setState(t, metastore.RemoteChildStateOp{Pause: &metastore.RemotePauseState{Paused: true, Reason: "maintenance"}})
	// Past the probe's due time.
	time.Sleep(2500 * time.Millisecond)
	rg.target.awaitDispatched(t, 2*time.Second)
	if got := rg.target.records(t, "orders"); len(got) != 0 {
		t.Fatalf("the target got %d records after the pause", len(got))
	}
	cancel()
	<-done
}
