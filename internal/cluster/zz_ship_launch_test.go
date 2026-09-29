package cluster

import (
	"context"
	"slices"
	"testing"
)

// zzShipQueueBehindInflight puts {"i":0} in flight to remote partition 1
// and queues {"i":1}..{"i":3} behind it, so they are held when whatever
// the test changes next happens.
func zzShipQueueBehindInflight(t *testing.T, d *ProduceDispatcher, peer *zzShipSlowPeer, accept func(int, int, int)) {
	t.Helper()
	ctx := context.Background()
	st := d.state
	accept(1, 0, 1)
	d.step(ctx, st)
	zzShipSlowWaitCall(t, peer, 1)
	accept(1, 1, 4)
	d.step(ctx, st)
	p1 := st.dests[dispatchDestKey{topic: "orders", partition: 1}]
	if p1 == nil || len(p1.queue) != 3 || !p1.inflight {
		t.Fatalf("want 3 records queued behind the in-flight commit, got %+v", p1)
	}
}

func zzShipLocalPayloads(t *testing.T, d *ProduceDispatcher) (payloads []string, partitions []int) {
	t.Helper()
	for _, r := range d.committer.(*fakeProduceCommitter).committed() {
		payloads = append(payloads, string(r.Payload))
		partitions = append(partitions, r.TargetPartition)
	}
	return payloads, partitions
}

// Records already held for a destination whose owner dies before their
// commit leaves are rerouted at launch to a live sibling, in order. The
// reroute tests change membership before the first read, where place
// does it; this is the launch-time path (unresolvedAtLaunch).
func TestHeldRecordsRerouteWhenTheirOwnerDiesBeforeLaunch(t *testing.T) {
	d, peer, _, accept := zzShipSlowSetup(t, 1) // partition 0 here, 1 on node-remote
	ctx := context.Background()
	st := d.state
	zzShipQueueBehindInflight(t, d, peer, accept)

	if err := d.store.MarkMemberDead(ctx, "node-remote"); err != nil {
		t.Fatal(err)
	}
	peer.release <- nil // the in-flight commit lands; the queue goes next
	zzShipSlowMerge(t, d)
	d.step(ctx, st) // launch: partition 1 no longer resolves
	zzShipSlowMerge(t, d)
	d.step(ctx, st)

	if got := peer.committed(1); !slices.Equal(got, []int{0}) {
		t.Fatalf("partition 1 got %v, want only the record in flight before its owner died", got)
	}
	payloads, partitions := zzShipLocalPayloads(t, d)
	want := []string{`{"i":1}`, `{"i":2}`, `{"i":3}`}
	if !slices.Equal(payloads, want) || !slices.Equal(partitions, []int{0, 0, 0}) {
		t.Fatalf("local partitions %v got %v, want %v rerouted to partition 0 in order", partitions, payloads, want)
	}
	if st.held != 0 || st.skipped != 0 {
		t.Fatalf("held %d skipped %d, want nothing left", st.held, st.skipped)
	}
	if next := d.ingress.DurableProduceNext(); st.nextSeq != next {
		t.Fatalf("checkpoint %d, want the durable frontier %d", st.nextSeq, next)
	}
}

// Records already held for a destination whose topic is deleted before
// their commit leaves are discarded at launch once the delete is
// confirmed, and the checkpoint moves past them.
func TestHeldRecordsDiscardedWhenTheirTopicIsDeletedBeforeLaunch(t *testing.T) {
	d, peer, _, accept := zzShipSlowSetup(t, 1)
	ctx := context.Background()
	st := d.state
	zzShipQueueBehindInflight(t, d, peer, accept)

	if err := d.store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	peer.release <- nil
	zzShipSlowMerge(t, d)
	d.step(ctx, st) // launch: the topic no longer resolves, and is confirmed gone

	if got := peer.committed(1); !slices.Equal(got, []int{0}) {
		t.Fatalf("partition 1 got %v, want only the record in flight before the delete", got)
	}
	if payloads, _ := zzShipLocalPayloads(t, d); len(payloads) != 0 {
		t.Fatalf("records of a deleted topic were committed locally: %v", payloads)
	}
	select {
	case part := <-peer.calls:
		t.Fatalf("a commit went to partition %d after the delete", part)
	default:
	}
	if st.held != 0 || st.skipped != 0 || st.outstanding != 0 {
		t.Fatalf("held %d skipped %d outstanding %d, want nothing left", st.held, st.skipped, st.outstanding)
	}
	if next := d.ingress.DurableProduceNext(); st.nextSeq != next {
		t.Fatalf("checkpoint %d, want the durable frontier %d past the discarded records", st.nextSeq, next)
	}
}
