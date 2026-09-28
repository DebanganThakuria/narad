package runtime

// A deleted topic's directory found by an open of its recreated
// successor is quarantined, and the deleted incarnation is retired: the
// retired hook drops its consumer shards, and their drop notifier
// Forgets them in the offset committer. Until then a shard of the
// deleted incarnation is live, and a commit of its is persisted by path
// into whatever directory the partition's path names. These tests pin
// that the hook runs before the successor's partition directory exists,
// so no such commit can land in it.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP23WireInFlight wires a consumer.InFlight to committer the way
// cmd/narad/serve_wiring.go does: acks commit through it, shards recover
// from the partition directory's two files, the committer snapshots the
// shards, and a dropped shard is forgotten.
func zzWP23WireInFlight(dataDir string, committer *ConsumerOffsetCommitter) *consumer.InFlight {
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1 << 10, MaxAckedAhead: 1 << 10}, nil
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

// An old-incarnation shard the committer never primed (recovered at 19,
// no ack since the process started) takes an ack, and the committer
// ticks, at the moment the open retires the old incarnation, just
// before the hook drops the shard. The recreated topic's directory must
// not exist yet, and must recover -1 once the open made it. It did not
// when the hook ran after storage.NewLog: the tick primed the new
// directory by path and wrote 20 into it.
func TestZZWP23IncarnationOpenRetiresBeforeItsDirectory(t *testing.T) {
	for _, via := range []string{"get-open", "get-closed", "cold-walk"} {
		t.Run(via, func(t *testing.T) {
			ctx := context.Background()
			store := newIncarnationStore(t)
			dataDir := t.TempDir()
			logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
			defer logs.CloseAll()

			if err := store.CreateTopic(ctx, topic.Topic{Name: "t", ID: "1111111111111111", Partitions: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := logs.Get("t", 0); err != nil {
				t.Fatal(err)
			}
			if via != "get-open" {
				if err := logs.CloseTopic("t"); err != nil {
					t.Fatal(err)
				}
			}

			c := zzWP23ManualCommitter(dataDir)
			defer func() { _ = c.Close() }()
			shards := &zzWP23Shards{}
			c.SetAheadSource(shards.source)
			old := newZZWP23Shard(19)
			shards.set(0, old)

			if err := store.DeleteTopic(ctx, "t"); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateTopic(ctx, topic.Topic{Name: "t", ID: "2222222222222222", Partitions: 1}); err != nil {
				t.Fatal(err)
			}

			newDir := storage.TopicPartitionDir(dataDir, "t", 0)
			calls := 0
			logs.SetTopicRetiredHook(func(name string) {
				calls++
				if _, err := os.Stat(newDir); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("retired hook runs with %s present (stat err %v): the old shard's commits can land in it", newDir, err)
				}
				old.ack(20)
				c.Commit("t", 0, old.state().frontier)
				if err := c.flush(); err != nil {
					t.Logf("tick in the retire: %v", err)
				}
				// dropTopicState -> InFlight.DropTopic: the shard goes, then
				// the drop notifier's Forget.
				shards.set(0, nil)
				c.Forget("t", 0)
			})

			switch via {
			case "cold-walk":
				if _, err := logs.sweepColdPartition("t", 0); err != nil {
					t.Fatal(err)
				}
			default:
				if _, err := logs.Get("t", 0); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 1 {
				t.Fatalf("retired hook ran %d times, want once", calls)
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if rec := zzWP23RecoverNew(t, newDir); rec.frontier != -1 {
				t.Fatalf("LOSS: the recreated topic recovers frontier %d, the old incarnation's, want -1: its records 0..%d are never delivered",
					rec.frontier, rec.frontier)
			}
		})
	}
}

// The same with the production pieces: a real consumer.InFlight whose
// old-incarnation shard handed out 0..19, wired to the committer as the
// serve wiring does, and a retired hook that drops the topic's shards
// the way topics.Manager's does. The window is the open's own: the
// SetOpened hook runs after storage.NewLog made the new directory. In
// tick-in-window the old shard's acks are pending and a tick runs
// there; in real-loop the acks arrive there and the committer's own
// loop ticks. Either way the recreated topic must deliver its offset 0.
func TestZZWP23IncarnationOpenKeepsTheOldFrontierOut(t *testing.T) {
	for _, mode := range []string{"tick-in-window", "real-loop"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store := newIncarnationStore(t)
			dataDir := t.TempDir()
			logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
			defer logs.CloseAll()

			var committer *ConsumerOffsetCommitter
			if mode == "tick-in-window" {
				committer = zzWP23ManualCommitter(dataDir)
			} else {
				committer = NewConsumerOffsetCommitter(dataDir, 10*time.Millisecond, nil)
			}
			defer func() { _ = committer.Close() }()
			offsets := zzWP23WireInFlight(dataDir, committer)
			logs.SetTopicRetiredHook(func(name string) { offsets.DropTopic(name) })

			if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "1111111111111111", Partitions: 1}); err != nil {
				t.Fatal(err)
			}
			l, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			appendOld(t, l, 30, "old-incarnation")
			var handles []consumer.ReserveResult
			for range 20 {
				res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l.HighWatermark())
				if err != nil || !res.Reserved {
					t.Fatalf("reserve: %+v %v", res, err)
				}
				handles = append(handles, res)
			}
			// Acks of the old incarnation's records. Once its shard is
			// dropped they are stale, and refusing them is correct.
			ackAll := func() {
				for _, h := range handles {
					_ = offsets.CommitHandle("orders", 0, h.Offset, h.Nonce)
				}
			}
			if mode == "tick-in-window" {
				ackAll()
			}

			// Deleted and recreated while the log is open; the purge has
			// not reached this node.
			if err := store.DeleteTopic(ctx, "orders"); err != nil {
				t.Fatal(err)
			}
			if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "2222222222222222", Partitions: 1}); err != nil {
				t.Fatal(err)
			}

			opened := false
			logs.SetOpened(func(string, int, *storage.Log) {
				opened = true
				if mode == "tick-in-window" {
					_ = committer.flush()
					return
				}
				ackAll()
				// Several of the loop's ticks.
				time.Sleep(60 * time.Millisecond)
			})
			l2, err := logs.Get("orders", 0)
			logs.SetOpened(nil)
			if err != nil {
				t.Fatal(err)
			}
			if !opened || l2.HighWatermark() != 0 {
				t.Fatalf("setup: opened %v, the recreated topic's hwm %d", opened, l2.HighWatermark())
			}
			appendOld(t, l2, 10, "new-incarnation")
			if mode == "tick-in-window" {
				_ = committer.flush()
			} else {
				time.Sleep(30 * time.Millisecond)
			}

			newDir := storage.TopicPartitionDir(dataDir, "orders", 0)
			frontier := int64(-1)
			if off, ok, _ := storage.ReadConsumerOffset(newDir); ok {
				frontier = off
			}
			if rec, ok, _ := storage.ReadConsumerAhead(newDir); ok {
				frontier = max(frontier, rec.Committed)
			}
			res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l2.HighWatermark())
			if err != nil {
				t.Fatal(err)
			}
			if !res.Reserved || res.Offset != 0 {
				t.Fatalf("LOSS: the recreated topic's first reserve is %+v, want offset 0; its directory holds frontier %d",
					res, frontier)
			}
			if frontier != -1 {
				t.Fatalf("the recreated topic's directory holds frontier %d it never acked", frontier)
			}
		})
	}
}
