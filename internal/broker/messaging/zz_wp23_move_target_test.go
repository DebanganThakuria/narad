package messaging

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// M7: a node that is only the target of a move creates no reservation
// shard for the partition before the flip. The move runner resets the
// partition's consumer state before it installs the copy (G2), so that
// no shard can write its frontier into the copy through the offset
// committer; that is only enough if nothing creates a shard between that
// reset and the flip. Only ReserveNext creates a shard, and only
// tryQueueRead calls it, over the partitions localProbePartitions
// resolves: the pinned one after an ownership check, or the owned ones.
// This drives every consume-side entry point of a node that owns
// orders/1 and is the move target of orders/0, then flips the move and
// checks that the same calls now create orders/0's shard.
func TestZZWP23MoveTargetCreatesNoShardBeforeFlip(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 30_000}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"node-self", "node-other"} {
		if err := store.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ".example:7942", Status: metastore.MemberAlive}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-other"); err != nil {
		t.Fatal(err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetAssignmentTarget(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatal(err)
	}
	e := newClusterTestEngine(t, store, fixedPartitionManager{picked: 1})
	if _, _, err := e.Produce(ctx, "orders", "", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("produce to the owned partition: %v", err)
	}

	notOwner := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, ErrNotPartitionOwner) {
			t.Fatalf("%s on the move target: err %v, want %v", what, err, ErrNotPartitionOwner)
		}
	}
	// Pinned to the partition moving here, in every form.
	_, _, err := e.Consume(ctx, "orders", ConsumeOpts{Partition: new(0)})
	notOwner("pinned consume", err)
	_, _, err = e.Consume(ctx, "orders", ConsumeOpts{Partition: new(0), Wait: 50 * time.Millisecond})
	notOwner("pinned long-poll consume", err)
	_, _, err = e.Consume(ctx, "orders", ConsumeOpts{Partition: new(0), Offset: new(int64(0))})
	notOwner("replay consume", err)
	_, _, err = e.ConsumeBatch(ctx, "orders", ConsumeOpts{Partition: new(0)}, 4, nil)
	notOwner("pinned batch consume", err)

	// Queue-style: served from the owned partition only.
	msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{})
	if err != nil || !found || msg.Partition != 1 {
		t.Fatalf("queue consume = partition %d found %v err %v, want the owned partition 1", msg.Partition, found, err)
	}
	if _, _, w, err := e.ConsumeProbe(ctx, "orders", ConsumeOpts{}); err != nil || w == nil {
		t.Fatalf("probe = waiter %v err %v, want a waiter (nothing left)", w, err)
	}
	if got, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 4, nil); err != nil || len(got) != 0 {
		t.Fatalf("batch consume = %d messages err %v, want none", len(got), err)
	}
	// A parked queue consumer the dispatcher serves: its pump resolves
	// the owned partitions afresh.
	served := make(chan topic.Message, 1)
	go func() {
		msg, found, _ := e.Consume(ctx, "orders", ConsumeOpts{Wait: 5 * time.Second})
		if found {
			served <- msg
		}
		close(served)
	}()
	time.Sleep(50 * time.Millisecond)
	if _, _, err := e.Produce(ctx, "orders", "", []byte(`{"id":2}`)); err != nil {
		t.Fatalf("produce to the owned partition: %v", err)
	}
	if msg, ok := <-served; !ok || msg.Partition != 1 {
		t.Fatalf("parked consume = %+v (served %v), want a record of partition 1", msg, ok)
	}

	// Handles forged for the partition moving here.
	forged := consumer.Handle{Partition: 0, Offset: 0, Nonce: 1}
	if err := e.Ack(ctx, "orders", forged); err == nil {
		t.Fatal("ack of a handle for the target partition succeeded")
	}
	if err := e.ExtendAck(ctx, "orders", forged); err == nil {
		t.Fatal("extend of a handle for the target partition succeeded")
	}
	if err := e.Nack(ctx, "orders", forged); err == nil {
		t.Fatal("nack of a handle for the target partition succeeded")
	}

	if _, _, _, ok := e.offsets.Reservable("orders", 0); ok {
		t.Fatal("the move target created a reservation shard for the partition before the flip")
	}
	if _, ok := e.offsets.CommittedOffset("orders", 0); ok {
		t.Fatal("the move target holds a committed frontier for the partition before the flip")
	}
	if _, err := os.Stat(storage.TopicPartitionDir(e.logs.DataDir(), "orders", 0)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the move target opened the partition's directory before the flip (stat err %v)", err)
	}
	// The same calls did create the owned partition's shard: the checks
	// above can see one.
	if _, _, _, ok := e.offsets.Reservable("orders", 1); !ok {
		t.Fatal("no shard for the owned partition: the probe cannot see shard creation")
	}

	// The flip: from here the partition is this node's, and a consume
	// creates its shard from the installed files.
	if err := store.CompleteMove(ctx, "orders", 0, "node-other", "node-self"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Consume(ctx, "orders", ConsumeOpts{Partition: new(0)}); err != nil {
		t.Fatalf("pinned consume after the flip: %v", err)
	}
	if _, _, _, ok := e.offsets.Reservable("orders", 0); !ok {
		t.Fatal("no shard for the partition after the flip and a consume")
	}
}
