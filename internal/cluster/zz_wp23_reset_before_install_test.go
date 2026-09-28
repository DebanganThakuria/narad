package cluster

// A destination that owned a partition earlier can still hold that
// ownership's reservation shard when the partition moves back. The
// shard's acks persist through the consumer offset committer, which
// writes by path into whatever directory the partition's path names;
// once the move has installed its copy there, an ack of the old shard
// primes the copy with the old frontier, and the new owner skips every
// record between the copy's frontier and the old one. The move runner
// resets the partition's consumer state before the install as well as
// after it, so no shard exists for the partition while the copy is
// being installed.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP23DestNode is the broker half of a move's destination: a real
// consumer.InFlight wired to a real offset committer the way cmd/narad
// wires them, resetting and installing the way *messaging.Engine does.
type zzWP23DestNode struct {
	dataDir   string
	offsets   *consumer.InFlight
	committer *runtime.ConsumerOffsetCommitter

	mu    sync.Mutex
	calls []string
	// duringInstall runs right after the install's swap, before the
	// runner's next step.
	duringInstall func()
}

func newZZWP23DestNode(t *testing.T, dataDir string) *zzWP23DestNode {
	t.Helper()
	committer := runtime.NewConsumerOffsetCommitter(dataDir, time.Second, nil)
	t.Cleanup(func() { _ = committer.Close() })
	offsets := zzWP23WireOffsets(dataDir, committer)
	return &zzWP23DestNode{dataDir: dataDir, offsets: offsets, committer: committer}
}

// zzWP23WireOffsets builds an InFlight over dataDir persisted through
// committer, as cmd/narad's serve wiring does.
func zzWP23WireOffsets(dataDir string, committer *runtime.ConsumerOffsetCommitter) *consumer.InFlight {
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 64, MaxAckedAhead: 64}, nil
	}, committer.Commit)
	offsets.SetCommittedRecovery(func(topicName string, p int) (int64, bool) {
		committed, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, topicName, p))
		return committed, ok && err == nil
	})
	offsets.SetAheadRecovery(func(topicName string, p int) (int64, []int64, bool) {
		rec, ok, err := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, topicName, p))
		return rec.Committed, rec.Offsets, ok && err == nil
	})
	committer.SetAheadSource(offsets.AheadSnapshot)
	offsets.SetDropNotifier(committer.Forget)
	return offsets
}

func (d *zzWP23DestNode) ReclaimMovedPartition(context.Context, string, int) error { return nil }

func (d *zzWP23DestNode) ResetPartitionConsumerState(topicName string, partition int) {
	d.mu.Lock()
	d.calls = append(d.calls, "reset")
	d.mu.Unlock()
	d.offsets.DropPartition(topicName, partition)
}

func (d *zzWP23DestNode) InstallPartitionDir(_ string, _ int, swap func() error) error {
	d.mu.Lock()
	d.calls = append(d.calls, "install")
	d.mu.Unlock()
	if err := swap(); err != nil {
		return err
	}
	if d.duringInstall != nil {
		d.duringInstall()
	}
	return nil
}

// zzWP23RecoveredFrontier is the frontier the broker recovers from a
// partition directory: the larger of its two consumer state files.
func zzWP23RecoveredFrontier(t *testing.T, dir string) int64 {
	t.Helper()
	frontier := int64(-1)
	if off, ok, err := storage.ReadConsumerOffset(dir); err != nil {
		t.Fatalf("read consumer.offset: %v", err)
	} else if ok {
		frontier = off
	}
	if rec, ok, err := storage.ReadConsumerAhead(dir); err != nil {
		t.Fatalf("read consumer.ahead: %v", err)
	} else if ok {
		frontier = max(frontier, rec.Committed)
	}
	return frontier
}

// The destination owned orders/0 before and still holds that
// ownership's shard, recovered at 19 with offset 20 reserved, which the
// committer has never primed. The partition moves back with a copy at
// frontier 5. The old shard's first ack lands right after the install:
// both its own commit and a late Commit of the kind an ack issues after
// releasing the shard's lock, then a committer tick. The installed copy
// must recover its own 5.
func TestZZWP23MoveResetsConsumerStateBeforeInstall(t *testing.T) {
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 10)
	dataDir := t.TempDir()
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := storage.WriteConsumerOffset(dir, 19); err != nil {
		t.Fatal(err)
	}
	dest := newZZWP23DestNode(t, dataDir)
	res, err := dest.offsets.ReserveNext(context.Background(), "orders", 0, time.Minute, 30)
	if err != nil || !res.Reserved || res.Offset != 20 {
		t.Fatalf("setup: the old shard reserved %+v (err %v), want offset 20", res, err)
	}
	var ackErr error
	dest.duringInstall = func() {
		ackErr = dest.offsets.CommitHandle("orders", 0, res.Offset, res.Nonce)
		dest.committer.Commit("orders", 0, res.Offset)
		// Close runs a tick: prime, window write, writeout, level.
		_ = dest.committer.Close()
	}

	store := &fakeMoveStore{
		assignment: metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:     metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
	}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: hwm, committed: 5, hasCommitted: true}}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, dest, nil, nil, MoveConfig{})
	r.Reconcile(context.Background())
	r.wg.Wait()

	if got := store.completeArgs; len(got) != 3 || got[2] != "narad-dst" {
		t.Fatalf("the move did not flip: %v", got)
	}
	if got := zzWP23RecoveredFrontier(t, dir); got != 5 {
		t.Fatalf("installed copy recovers %d, want the source's 5 (the old shard's ack: %v)", got, ackErr)
	}
	dest.mu.Lock()
	calls := append([]string(nil), dest.calls...)
	dest.mu.Unlock()
	if len(calls) != 3 || calls[0] != "reset" || calls[1] != "install" || calls[2] != "reset" {
		t.Fatalf("destination calls %v, want [reset install reset]", calls)
	}
	if ackErr == nil {
		t.Fatal("the old shard's ack was accepted after the install: its shard outlived the reset before install")
	}
}
