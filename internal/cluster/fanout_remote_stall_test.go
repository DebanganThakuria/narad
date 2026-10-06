package cluster

// A stalled link retries after each stall interval however the slab got
// there: held, or re-read because the held budget had no room.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/remote/sink"
)

// With no held budget a stalled lane cannot keep its records, so its
// slab is read again after the stall wait; the pass after that must
// still send its one retry, or nothing ever clears the stall.
func TestRemoteChildStallClearsWithoutAHeldBudget(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{grant: []string{"other"},
		rigSourceOpts: rigSourceOpts{heldBudget: -1, stallRetry: 300 * time.Millisecond}})
	rg.src.start()
	defer rg.src.stop()
	want := rg.src.produce(t, 0, 20, 3, 0)
	rg.waitState(t, 0, topic.RemoteStateForbidden, 15*time.Second)
	if err := rg.target.store.SetUserGrants(context.Background(), rigReplUser,
		[]userGrant{{Action: "produce", Patterns: []string{"orders"}}}, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	rg.waitDelivered(t, want, 10*time.Second)
	rg.waitState(t, 0, topic.RemoteStateRunning, 10*time.Second)
}

// A resume is a change to the link, as a change during a stall wait is:
// the stalled link retries at once instead of waiting a whole stall
// interval again.
func TestRemoteChildStalledLinkRetriesAtOnceAfterAResume(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{grant: []string{"other"},
		rigSourceOpts: rigSourceOpts{stallRetry: 8 * time.Second}})
	rg.src.start()
	defer rg.src.stop()
	want := rg.src.produce(t, 0, 20, 3, 0)
	rg.waitState(t, 0, topic.RemoteStateForbidden, 15*time.Second)
	if err := rg.target.store.SetUserGrants(context.Background(), rigReplUser,
		[]userGrant{{Action: "produce", Patterns: []string{"orders"}}}, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	rg.setState(t, metastore.RemoteChildStateOp{Pause: &metastore.RemotePauseState{Paused: true, Reason: "fixing the grant"}})
	rigWait(t, "the cursor to see the pause", 10*time.Second, func() bool {
		c := rg.src.runner.remoteCursorFor("orders", 0, "orders-to-b")
		if c == nil {
			return false
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.paused
	})
	rg.setState(t, metastore.RemoteChildStateOp{Pause: &metastore.RemotePauseState{Paused: false}})
	rg.waitDelivered(t, want, 4*time.Second)
}

// A lane that shipped everything gives its held records back at once:
// a sibling blocked on a refused record must not keep the whole slab's
// bytes in the node's held budget until an admin skips it.
func TestRemoteChildFinishedLaneReleasesItsHeldRecords(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{lanes: 2, rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	schema := []byte(`{"type":"object","required":["seq"],"properties":{"seq":{"type":"integer"}}}`)
	if _, err := rg.target.broker.UpdateTopicSchema(context.Background(), "orders", schema, 0); err != nil {
		t.Fatal(err)
	}
	const badKey = "bad"
	var otherLane []string
	for i := range 32 {
		key := fmt.Sprintf("key-%d", i)
		if sink.LaneOf(topic.KeyedRecord{Key: key}, 2) != sink.LaneOf(topic.KeyedRecord{Key: badKey}, 2) {
			otherLane = append(otherLane, key)
		}
	}
	if len(otherLane) == 0 {
		t.Fatal("every key maps to the bad record's lane")
	}
	// Every record is in the parent before the cursor starts, so one
	// slab carries them all.
	var finished []topic.KeyedRecord
	for i, key := range otherLane {
		payload := fmt.Appendf(nil, `{"seq":%d}`, i)
		rg.src.producePayload(t, 0, key, payload)
		finished = append(finished, topic.KeyedRecord{Key: key, Payload: payload})
	}
	rg.src.producePayload(t, 0, badKey, []byte(`{"not_seq":true}`))
	rg.target.faults.set("down")
	rg.src.start()
	defer rg.src.stop()
	rg.waitState(t, 0, topic.RemoteStateUnavailable, 15*time.Second)
	rigWait(t, "the lanes holding their records", 10*time.Second, func() bool {
		return rg.src.runner.remote.held.Used() > 0
	})
	time.Sleep(time.Second)
	both := rg.src.runner.remote.held.Used()

	rg.target.faults.set("")
	rg.waitDelivered(t, finished, 60*time.Second)
	rg.waitState(t, 0, topic.RemoteStateRejectedRecord, 15*time.Second)
	rigWait(t, "the finished lane's held bytes released", 5*time.Second, func() bool {
		return rg.src.runner.remote.held.Used() < both
	})
}
