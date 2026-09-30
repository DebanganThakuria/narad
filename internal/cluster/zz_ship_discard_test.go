package cluster

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// A commit that fails because its topic was deleted meanwhile discards
// its records once the delete is confirmed, instead of retrying them
// against an owner that will never take them; the checkpoint moves past
// them.
func TestFailedCommitOfDeletedTopicIsDiscarded(t *testing.T) {
	d, peer, _, accept := zzShipSlowSetup(t, 1) // partition 0 here, 1 on node-remote
	ctx := context.Background()
	st := d.state
	accept(1, 0, 2)
	d.step(ctx, st)
	zzShipSlowWaitCall(t, peer, 1)

	if err := d.store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	peer.release <- errors.New("topic not found")
	zzShipSlowMerge(t, d)
	d.step(ctx, st)

	if got := peer.committed(1); len(got) != 0 {
		t.Fatalf("partition 1 committed %v", got)
	}
	select {
	case part := <-peer.calls:
		t.Fatalf("a discarded record was retried on partition %d", part)
	default:
	}
	p1 := st.dests[dispatchDestKey{topic: "orders", partition: 1}]
	if st.held != 0 || st.skipped != 0 || (p1 != nil && p1.failing()) {
		t.Fatalf("held %d skipped %d dest %+v, want nothing left and nothing failing", st.held, st.skipped, p1)
	}
	if next := d.ingress.DurableProduceNext(); st.nextSeq != next {
		t.Fatalf("checkpoint %d, want the durable frontier %d past the discarded records", st.nextSeq, next)
	}
}

// A failed batch mixing a record of a replaced incarnation with one that
// carries no incarnation (accepted by an older release) discards only
// the replaced one; the other is retried, and commits once the owner
// takes it.
func TestFailedCommitDiscardsOnlyReplacedIncarnation(t *testing.T) {
	d, peer, clock, _ := zzShipSlowSetup(t, 1)
	ctx := context.Background()
	st := d.state
	if _, err := d.ingress.AcceptProduce(ctx, "orders", "k", 1, []byte(`{"i":0}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := d.ingress.AcceptProduceWithTopicID(ctx, "orders", "incarnation-1", "k", 1, []byte(`{"i":1}`)); err != nil {
		t.Fatal(err)
	}
	d.step(ctx, st)
	zzShipSlowWaitCall(t, peer, 1)
	p1 := st.dests[dispatchDestKey{topic: "orders", partition: 1}]
	if p1 == nil || !p1.inflight || p1.held != 2 {
		t.Fatalf("want both records in one commit in flight, got %+v", p1)
	}

	// The topic is deleted and recreated as incarnation-2 while the batch
	// is in flight, and the batch fails.
	if err := d.store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := d.store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "incarnation-2", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	for p, owner := range []string{"node-self", "node-remote"} {
		if err := d.store.AssignPartition(ctx, "orders", p, owner); err != nil {
			t.Fatal(err)
		}
	}
	peer.release <- errors.New("owner refused")
	zzShipSlowMerge(t, d)
	if !p1.failing() || p1.held != 1 {
		t.Fatalf("after the failure: failing %v held %d, want the one kept record held as the probe", p1.failing(), p1.held)
	}

	clock.Advance(d.failureBackoff)
	d.step(ctx, st) // the retry
	zzShipSlowWaitCall(t, peer, 1)
	peer.release <- nil
	zzShipSlowMerge(t, d)
	d.step(ctx, st)

	if got := peer.committed(1); !slices.Equal(got, []int{0}) {
		t.Fatalf("partition 1 committed %v, want only the record without an incarnation", got)
	}
	if st.held != 0 || st.skipped != 0 {
		t.Fatalf("held %d skipped %d, want nothing left", st.held, st.skipped)
	}
	if next := d.ingress.DurableProduceNext(); st.nextSeq != next {
		t.Fatalf("checkpoint %d, want the durable frontier %d", st.nextSeq, next)
	}
}
