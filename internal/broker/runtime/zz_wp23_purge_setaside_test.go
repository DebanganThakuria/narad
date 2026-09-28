package runtime

// A purge's first retire drops the purged incarnation's shards, but
// nothing stops a consume that still holds the purged incarnation's
// (closed) log from making the partition's shard again: tryQueueRead
// resolves the log before it reserves. Before the purge set the topic
// directory aside, that shard (S1) was made from consumer.offset and
// consumer.ahead still at the partition path, recovered the purged
// frontier, and stayed live until the second retire. A commit reaching
// the committer after the first retire (an ack of the dropped shard
// whose onCommit ran after the drop, or the committer's own requeue of
// a forgotten snapshot) made the next tick snapshot S1 and prime the
// partition by path. A tick inside the removal, after os.RemoveAll's
// unlinks and before its rmdir, created consumer.ahead and
// consumer.offset there, the rmdir failed with ENOTEMPTY, and the
// recreated topic adopted the unmarked leftover and skipped its first
// records.
//
// Seams, each standing for a schedule only:
//   - the ack's onCommit is parked after the shard lock is released and
//     before committer.Commit (the ack goroutine descheduled), or the
//     AheadSource wrapper parks a tick after its snapshot;
//   - logs.removeAll is zzWP23RemoveAllWithTick (os.RemoveAll's syscall
//     order) with, before its unlinks, consumer A's ReserveNext on the
//     log it resolved before the purge.
//
// Everything else is production code: a Raft metastore that caps
// resolve through, runtime.Logs (PurgeTopic, Get with adoption),
// consumer.InFlight with the create fence, and the committer wired as
// serve_wiring.go wires it, with a retired hook that calls DropTopic.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func TestZZWP23PurgeMadeAgainShardLeavesNoLeftover(t *testing.T) {
	for _, via := range []string{"purge-after-recreate", "purge-replica-lag", "CONTROL-no-consume-after-retire"} {
		t.Run(via, func(t *testing.T) { zzWP23PurgeMadeAgain(t, via) })
	}
}

func zzWP23PurgeMadeAgain(t *testing.T, via string) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	partDir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceOldID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}

	// An earlier process: the old incarnation acked 0..19 and stopped
	// cleanly.
	{
		logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
		committer := zzWP23ManualCommitter(dataDir)
		offsets := zzWP23FenceWire(dataDir, store, committer, nil, false)
		l, err := logs.Get("orders", 0)
		if err != nil {
			t.Fatal(err)
		}
		appendOld(t, l, 30, "old-incarnation")
		for range 20 {
			res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l.HighWatermark())
			if err != nil || !res.Reserved {
				t.Fatalf("reserve: %+v %v", res, err)
			}
			if err := offsets.CommitHandle("orders", 0, res.Offset, res.Nonce); err != nil {
				t.Fatal(err)
			}
		}
		if err := committer.Close(); err != nil {
			t.Fatal(err)
		}
		logs.CloseAll()
		if got := zzWP23FencePersisted(partDir); got != 19 {
			t.Fatalf("setup: persisted %d, want 19", got)
		}
	}

	// This process, wired as serve_wiring.go wires it, except that the
	// ack's onCommit can be parked (the ack goroutine descheduled after
	// it released the shard lock).
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()
	committer := zzWP23ManualCommitter(dataDir)
	defer func() { _ = committer.Close() }()
	var parkCommit atomic.Bool
	parked := make(chan struct{})
	release := make(chan struct{})
	offsets := consumer.NewInFlight(func(ctx context.Context, name string) (consumer.Caps, error) {
		if _, err := store.GetTopic(ctx, name); err != nil {
			return consumer.Caps{}, err
		}
		return consumer.Caps{MaxInFlight: 1 << 10, MaxAckedAhead: 1 << 10}, nil
	}, func(name string, p int, off int64) {
		if parkCommit.CompareAndSwap(true, false) {
			close(parked)
			<-release
		}
		committer.Commit(name, p, off)
	})
	offsets.SetCommittedRecovery(func(name string, p int) (int64, bool) {
		committed, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, name, p))
		return committed, ok && err == nil
	})
	offsets.SetAheadRecovery(func(name string, p int) (int64, []int64, bool) {
		rec, ok, err := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, name, p))
		return rec.Committed, rec.Offsets, ok && err == nil
	})
	committer.SetAheadSource(offsets.AheadSnapshot)
	offsets.SetDropNotifier(committer.Forget)
	var hookRuns atomic.Int32
	logs.SetTopicRetiredHook(func(name string) {
		hookRuns.Add(1)
		offsets.DropTopic(name)
	})

	// Consumers of the old incarnation: A resolved the log (the consume
	// scan's Get or GetMany) and goes on to reserve on it; C holds
	// offset 20 and acks it while the topic is being deleted.
	oldLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark())
	if err != nil || !c.Reserved || c.Offset != 20 {
		t.Fatalf("setup: C's reserve %+v %v", c, err)
	}

	switch via {
	case "purge-after-recreate", "CONTROL-no-consume-after-retire":
		// The leader deleted and recreated the name before this node
		// runs the old incarnation's purge.
		if err := store.DeleteTopic(ctx, "orders"); err != nil {
			t.Fatal(err)
		}
		if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceNewID, Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	case "purge-replica-lag":
		// The purge RPC arrives before this node's replica applied the
		// delete: the old record still resolves caps.
	}

	// C's ack: the frontier moves to 20 and its onCommit parks.
	parkCommit.Store(true)
	acked := make(chan error, 1)
	go func() { acked <- offsets.CommitHandle("orders", 0, c.Offset, c.Nonce) }()
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: C's onCommit never ran")
	}

	var aRes consumer.ReserveResult
	var aErr, aRead error
	ticked := false
	inner := zzWP23RemoveAllWithTick(t, filepath.Base(partDir), func() {
		ticked = true
		if err := committer.flush(); err != nil {
			t.Logf("tick inside the removal: %v", err)
		}
	})
	logs.removeAll = func(dir string) error {
		if hookRuns.Load() != 1 {
			t.Errorf("setup: removal started after %d retires, want 1", hookRuns.Load())
		}
		// After the first retire dropped the old shard: C's onCommit
		// lands, and A reserves on the old log it holds.
		close(release)
		if err := <-acked; err != nil {
			t.Errorf("setup: C's ack: %v", err)
		}
		if via != "CONTROL-no-consume-after-retire" {
			aRes, aErr = offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark())
			if aErr == nil && aRes.Reserved {
				if _, _, _, aRead = oldLog.ReadKeyedShared(aRes.Offset); aRead != nil {
					_ = offsets.ReleaseHandle("orders", 0, aRes.Offset, aRes.Nonce)
				}
			}
		}
		return inner(dir)
	}

	purged, purgeErr := logs.PurgeTopic("orders", zzWP23FenceOldID)
	t.Logf("purge: purged %v err %v; retired hook ran %d times; tick in the removal %v", purged, purgeErr, hookRuns.Load(), ticked)
	if purgeErr != nil {
		t.Errorf("purge: %v", purgeErr)
	}
	zzWP23NoTopicDirsLeft(t, dataDir)
	t.Logf("consumer A (old log, after the first retire): reserve %+v err %v, read err %v", aRes, aErr, aRead)
	if entries, err := os.ReadDir(partDir); err == nil {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Logf("left at the partition path after the purge: %v, recovering frontier %d", names, zzWP23FencePersisted(partDir))
	}

	if via == "purge-replica-lag" {
		if err := store.DeleteTopic(ctx, "orders"); err != nil {
			t.Fatal(err)
		}
		if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceNewID, Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}

	// The recreated topic opens on this node.
	newLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if hwm := newLog.HighWatermark(); hwm != 0 {
		t.Fatalf("setup: the recreated topic's log starts at hwm %d, want 0", hwm)
	}
	zzWP23FenceAppend(t, newLog, 30)
	var got []int64
	for {
		r, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, newLog.HighWatermark())
		if err != nil {
			t.Fatal(err)
		}
		if !r.Reserved {
			break
		}
		if err := offsets.CommitHandle("orders", 0, r.Offset, r.Nonce); err != nil {
			t.Fatal(err)
		}
		got = append(got, r.Offset)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	persisted := zzWP23FencePersisted(partDir)
	if len(got) != 30 || got[0] != 0 {
		first := int64(-1)
		if len(got) > 0 {
			first = got[0]
		}
		t.Errorf("LOSS: the recreated topic delivered %d of its 30 records, from offset %d; its directory holds frontier %d (records 0..%d never delivered)",
			len(got), first, persisted, first-1)
	} else {
		t.Logf("the recreated topic delivered its 30 records from 0; persisted %d", persisted)
	}
}

// The same leftover with no ack in flight across the retire: the
// commit that primes the partition during the removal is the
// committer's own requeue. A tick drained the old shard's commit and
// took its snapshot; the first retire's Forget landed before the tick's
// prime, which re-queued the commit ("the snapshot may be of the dropped
// shard: take it again"). The next tick's snapshot is of the shard
// consumer A made again from the old files. The one seam besides the
// removal model is the AheadSource wrapper parking the first tick after
// its snapshot (the committer goroutine descheduled between snapshots
// and prime).
func TestZZWP23PurgeRequeuedCommitLeavesNoLeftover(t *testing.T) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	partDir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceOldID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	{
		logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
		committer := zzWP23ManualCommitter(dataDir)
		offsets := zzWP23FenceWire(dataDir, store, committer, nil, false)
		l, err := logs.Get("orders", 0)
		if err != nil {
			t.Fatal(err)
		}
		appendOld(t, l, 30, "old-incarnation")
		for range 20 {
			res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l.HighWatermark())
			if err != nil || !res.Reserved {
				t.Fatalf("reserve: %+v %v", res, err)
			}
			if err := offsets.CommitHandle("orders", 0, res.Offset, res.Nonce); err != nil {
				t.Fatal(err)
			}
		}
		if err := committer.Close(); err != nil {
			t.Fatal(err)
		}
		logs.CloseAll()
	}

	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()
	committer := zzWP23ManualCommitter(dataDir)
	defer func() { _ = committer.Close() }()
	offsets := zzWP23FenceWire(dataDir, store, committer, nil, false)
	var parkSnap atomic.Bool
	snapped := make(chan struct{})
	cont := make(chan struct{})
	committer.SetAheadSource(func(name string, p int) (int64, []int64, uint64, bool) {
		committed, offs, version, ok := offsets.AheadSnapshot(name, p)
		if parkSnap.CompareAndSwap(true, false) {
			close(snapped)
			<-cont
		}
		return committed, offs, version, ok
	})
	var hookRuns atomic.Int32
	logs.SetTopicRetiredHook(func(name string) {
		hookRuns.Add(1)
		offsets.DropTopic(name)
	})

	oldLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark())
	if err != nil || !c.Reserved || c.Offset != 20 {
		t.Fatalf("setup: C's reserve %+v %v", c, err)
	}
	// C acks just before the delete: a commit is pending for the tick.
	if err := offsets.CommitHandle("orders", 0, c.Offset, c.Nonce); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceNewID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}

	// Tick 1 takes the old shard's snapshot and is descheduled.
	parkSnap.Store(true)
	t1 := make(chan error, 1)
	go func() { t1 <- committer.flush() }()
	select {
	case <-snapped:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: tick 1 never took a snapshot")
	}

	var aRes consumer.ReserveResult
	var aErr, aRead error
	ticked := false
	inner := zzWP23RemoveAllWithTick(t, filepath.Base(partDir), func() {
		ticked = true
		if err := committer.flush(); err != nil {
			t.Logf("tick 2 inside the removal: %v", err)
		}
	})
	logs.removeAll = func(dir string) error {
		// The first retire ran (its Forget is recorded); tick 1 resumes,
		// finds the partition forgotten and re-queues the commit.
		close(cont)
		if err := <-t1; err != nil {
			t.Logf("tick 1: %v", err)
		}
		aRes, aErr = offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark())
		if aErr == nil && aRes.Reserved {
			if _, _, _, aRead = oldLog.ReadKeyedShared(aRes.Offset); aRead != nil {
				_ = offsets.ReleaseHandle("orders", 0, aRes.Offset, aRes.Nonce)
			}
		}
		return inner(dir)
	}
	purged, purgeErr := logs.PurgeTopic("orders", zzWP23FenceOldID)
	t.Logf("purge: purged %v err %v; retired hook ran %d times; tick 2 in the removal %v", purged, purgeErr, hookRuns.Load(), ticked)
	if purgeErr != nil {
		t.Errorf("purge: %v", purgeErr)
	}
	zzWP23NoTopicDirsLeft(t, dataDir)
	t.Logf("consumer A (old log, after the first retire): reserve %+v err %v, read err %v", aRes, aErr, aRead)
	if entries, err := os.ReadDir(partDir); err == nil {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Logf("left at the partition path after the purge: %v, recovering frontier %d", names, zzWP23FencePersisted(partDir))
	}

	newLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if hwm := newLog.HighWatermark(); hwm != 0 {
		t.Fatalf("setup: the recreated topic's log starts at hwm %d, want 0", hwm)
	}
	zzWP23FenceAppend(t, newLog, 30)
	var got []int64
	for {
		r, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, newLog.HighWatermark())
		if err != nil {
			t.Fatal(err)
		}
		if !r.Reserved {
			break
		}
		if err := offsets.CommitHandle("orders", 0, r.Offset, r.Nonce); err != nil {
			t.Fatal(err)
		}
		got = append(got, r.Offset)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	persisted := zzWP23FencePersisted(partDir)
	if len(got) != 30 || got[0] != 0 {
		first := int64(-1)
		if len(got) > 0 {
			first = got[0]
		}
		t.Errorf("LOSS: the recreated topic delivered %d of its 30 records, from offset %d; its directory holds frontier %d (records 0..%d never delivered)",
			len(got), first, persisted, first-1)
	} else {
		t.Logf("the recreated topic delivered its 30 records from 0; persisted %d", persisted)
	}
}

// The quarantine counterpart: the retire runs after the rename and
// before the successor's directory exists, so a shard made again after
// it (consumer A still holding the old log) recovers nothing, and a
// late commit of the dropped shard followed by ticks before and after
// the successor's NewLog persists only -1 there.
func TestZZWP23QuarantineMadeAgainAfterRetire(t *testing.T) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	partDir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceOldID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	{
		logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
		committer := zzWP23ManualCommitter(dataDir)
		offsets := zzWP23FenceWire(dataDir, store, committer, nil, false)
		l, err := logs.Get("orders", 0)
		if err != nil {
			t.Fatal(err)
		}
		appendOld(t, l, 30, "old-incarnation")
		for range 20 {
			res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l.HighWatermark())
			if err != nil || !res.Reserved {
				t.Fatalf("reserve: %+v %v", res, err)
			}
			if err := offsets.CommitHandle("orders", 0, res.Offset, res.Nonce); err != nil {
				t.Fatal(err)
			}
		}
		if err := committer.Close(); err != nil {
			t.Fatal(err)
		}
		logs.CloseAll()
	}
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()
	committer := zzWP23ManualCommitter(dataDir)
	defer func() { _ = committer.Close() }()
	var parkCommit atomic.Bool
	parked := make(chan struct{})
	release := make(chan struct{})
	offsets := consumer.NewInFlight(func(ctx context.Context, name string) (consumer.Caps, error) {
		if _, err := store.GetTopic(ctx, name); err != nil {
			return consumer.Caps{}, err
		}
		return consumer.Caps{MaxInFlight: 1 << 10, MaxAckedAhead: 1 << 10}, nil
	}, func(name string, p int, off int64) {
		if parkCommit.CompareAndSwap(true, false) {
			close(parked)
			<-release
		}
		committer.Commit(name, p, off)
	})
	offsets.SetCommittedRecovery(func(name string, p int) (int64, bool) {
		committed, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, name, p))
		return committed, ok && err == nil
	})
	offsets.SetAheadRecovery(func(name string, p int) (int64, []int64, bool) {
		rec, ok, err := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, name, p))
		return rec.Committed, rec.Offsets, ok && err == nil
	})
	committer.SetAheadSource(offsets.AheadSnapshot)
	offsets.SetDropNotifier(committer.Forget)
	oldLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	c, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark())
	if err != nil || !c.Reserved {
		t.Fatalf("setup: %+v %v", c, err)
	}
	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceNewID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	parkCommit.Store(true)
	acked := make(chan error, 1)
	go func() { acked <- offsets.CommitHandle("orders", 0, c.Offset, c.Nonce) }()
	<-parked
	var aRes consumer.ReserveResult
	logs.SetTopicRetiredHook(func(name string) {
		offsets.DropTopic(name)
		close(release)
		if err := <-acked; err != nil {
			t.Errorf("C's ack: %v", err)
		}
		aRes, _ = offsets.ReserveNext(ctx, "orders", 0, time.Minute, oldLog.HighWatermark())
		if aRes.Reserved {
			_ = offsets.ReleaseHandle("orders", 0, aRes.Offset, aRes.Nonce)
		}
		if err := committer.flush(); err != nil {
			t.Logf("tick inside the retire: %v", err)
		}
	})
	newLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := committer.flush(); err != nil {
		t.Logf("tick after the open: %v", err)
	}
	t.Logf("A after the retire: %+v; new directory recovers %d", aRes, zzWP23FencePersisted(partDir))
	zzWP23FenceAppend(t, newLog, 30)
	var got []int64
	for {
		r, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, newLog.HighWatermark())
		if err != nil {
			t.Fatal(err)
		}
		if !r.Reserved {
			break
		}
		if err := offsets.CommitHandle("orders", 0, r.Offset, r.Nonce); err != nil {
			t.Fatal(err)
		}
		got = append(got, r.Offset)
	}
	if len(got) != 30 || got[0] != 0 {
		t.Errorf("LOSS: the recreated topic delivered %d records from %v", len(got), got)
	}
}

// zzWP23NoTopicDirsLeft fails when anything is left under dataDir/topics
// after a purge of the only topic: the partition path, or a directory
// the purge set aside.
func zzWP23NoTopicDirsLeft(t *testing.T, dataDir string) {
	t.Helper()
	entries, err := os.ReadDir(storage.TopicDir(dataDir, ""))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("left under topics/ after the purge: %s", e.Name())
	}
}

// A purge that cannot finish its removal leaves the set-aside copy, never
// topics/<name>, and names it so the retry or the orphan sweeps reclaim
// it: topics/<name>.stale-<id> for an ID-bearing purge, the marker's ID
// for a legacy purge of a marked directory (classified as a quarantine),
// and a random purge- suffix for a legacy purge of an unmarked one.
func TestZZWP23PurgeSetAsideNames(t *testing.T) {
	for _, tc := range []struct {
		name, recordID, purgeID, wantPrefix string
		quarantined                         bool
	}{
		{"id-bearing", "0000000000000009", "0000000000000009", "orders.stale-0000000000000009", true},
		{"legacy-marked", "0000000000000009", "", "orders.stale-0000000000000009", true},
		{"legacy-unmarked", "", "", "orders.stale-purge-", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := newRuntimeFakeMetastore()
			dataDir := t.TempDir()
			logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
			defer logs.CloseAll()
			ms.topics["orders"] = topic.Topic{Name: "orders", ID: tc.recordID, Partitions: 1}
			l, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			appendOld(t, l, 3, "purged")

			var removing string
			logs.removeAll = func(dir string) error {
				removing = dir
				return errors.New("removal interrupted")
			}
			if purged, err := logs.PurgeTopic("orders", tc.purgeID); !purged || err == nil {
				t.Fatalf("purge = (%v, %v), want (true, the removal's error)", purged, err)
			}
			if _, err := os.Stat(storage.TopicDir(dataDir, "orders")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("topics/orders after the purge: stat err %v, want not-exist", err)
			}
			base := filepath.Base(removing)
			if !strings.HasPrefix(base, tc.wantPrefix) {
				t.Fatalf("the purge removed %s, want a set-aside named %s*", base, tc.wantPrefix)
			}
			c, err := classifyTopicDir(storage.TopicDir(dataDir, ""), base)
			if err != nil {
				t.Fatal(err)
			}
			if c.Quarantined != tc.quarantined {
				t.Fatalf("the leftover classifies as %+v, want quarantined %v", c, tc.quarantined)
			}

			// The name is recreated: its open never sees the leftover.
			logs.removeAll = os.RemoveAll
			ms.topics["orders"] = topic.Topic{Name: "orders", ID: "000000000000000a", Partitions: 1}
			l2, err := logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			if hwm := l2.HighWatermark(); hwm != 0 {
				t.Fatalf("the recreated topic opens at hwm %d, want 0", hwm)
			}
			if tc.purgeID != "" {
				// The retried purge reclaims the leftover.
				if _, err := logs.PurgeTopic("orders", tc.purgeID); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(removing); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("the retried purge left %s (stat err %v)", base, err)
				}
			}
		})
	}
}
