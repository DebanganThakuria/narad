package cluster

// A stalled link retries after each stall interval however the slab got
// there: held, or re-read because the held budget had no room.

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
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
