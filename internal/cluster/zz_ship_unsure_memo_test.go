package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// An unconfirmed incarnation check is remembered for failureBackoff from
// when the check came back, not from when it started: a leader check
// slower than the backoff used to leave a memo that had already run
// out, so every destination holding a record of that incarnation asked
// the leader again, each check up to leaderConfirmRPCTimeout on the
// single dispatcher loop.
func TestIncarnationUnsureMemoSurvivesSlowLeader(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	zzWP6RegisterMembers(t, store)
	const parts = 4
	owners := make([]string, parts)
	for p := range owners {
		owners[p] = "node-self"
	}
	zzWP6SeedIncarnation(t, store, "incarnation-1", owners...)
	manager := newDispatchIngressManager(t)
	// One record per partition, so each is the first of its destination
	// and none is skipped for an earlier one.
	for p := range parts {
		if _, err := manager.AcceptProduceWithTopicID(ctx, "orders", "incarnation-1", "k", p, fmt.Appendf(nil, `{"p":%d}`, p)); err != nil {
			t.Fatal(err)
		}
	}
	// The local replica moves on to a recreated topic of the same name.
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	zzWP6SeedIncarnation(t, store, "incarnation-2", owners...)

	clock := &zzWP6Clock{now: time.Unix(1_700_000_000, 0)}
	var checks atomic.Int32
	peer := fakePeerClient{getTopicFn: func(context.Context, string, string) (nodewire.Response, error) {
		// A leader check that fails slowly: it takes twice the backoff.
		checks.Add(1)
		clock.Advance(2 * defaultProduceDispatchFailureBackoff)
		return nodewire.Response{}, errors.New("leader rpc timed out")
	}}
	// This node is not the leader (node-self is), so the check goes to
	// the leader over the peer client.
	d := NewProduceDispatcher(manager, store, "node-other", &fakeProduceCommitter{}, peer, nil, ProduceDispatcherConfig{})
	d.now = clock.Now

	if _, err := d.DispatchAvailable(ctx); err != nil {
		t.Fatalf("DispatchAvailable: %v", err)
	}
	if n := checks.Load(); n != 1 {
		t.Fatalf("%d leader checks for one unconfirmed incarnation in one pass, want 1", n)
	}
	if st := d.state; st.skipped != parts || st.nextSeq >= manager.DurableProduceNext() {
		t.Fatalf("skipped %d, checkpoint %d of %d; want all %d records left in the WAL, unconfirmed", st.skipped, st.nextSeq, manager.DurableProduceNext(), parts)
	}
	// Once the memo runs out the incarnation is asked about again.
	clock.Advance(defaultProduceDispatchFailureBackoff)
	if _, err := d.DispatchAvailable(ctx); err != nil {
		t.Fatalf("DispatchAvailable: %v", err)
	}
	if n := checks.Load(); n != 2 {
		t.Fatalf("%d leader checks after the memo ran out, want 2", n)
	}
}
