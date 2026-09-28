package runtime

// A consume makes a partition's first shard from the files at the
// partition's path: consumer.InFlight reads consumer.offset and
// consumer.ahead, then stores the shard under the topic NAME. These
// tests park that create between its reads and its store while the
// topic's incarnation is retired under it, by an open's quarantine or
// by a purge, and require that the recreated topic is served from its
// own state and persists only that.
//
// Before the create fence the retire's drop found no shard and
// returned, the create stored one after it with the deleted
// incarnation's frontier, and no later retire ran for the name: the
// recreated topic's consumers were served from frontier 19, its records
// 0..19 were never delivered, and the committer's first prime wrote
// the stale frontier into the new incarnation's directory.
//
// The only seam is the park in the recovery, which stands for the
// consume goroutine being descheduled before its store. Everything else
// is production code: a Raft metastore that caps resolve through,
// runtime.Logs, consumer.InFlight and the committer wired as
// cmd/narad/serve_wiring.go wires them, and a retired hook that calls
// InFlight.DropTopic as topics.Manager.dropTopicState does.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

const (
	zzWP23FenceOldID = "1111111111111111"
	zzWP23FenceNewID = "2222222222222222"
)

// zzWP23FenceGate parks the first recovery read made while it is armed,
// after the read, until release is closed.
type zzWP23FenceGate struct {
	armed   atomic.Bool
	read    chan struct{}
	release chan struct{}
}

func (g *zzWP23FenceGate) park() {
	if g != nil && g.armed.CompareAndSwap(true, false) {
		close(g.read)
		<-g.release
	}
}

// zzWP23FenceWire wires an InFlight to committer as serve_wiring.go
// does, with caps resolved through the metastore record (a shard is
// made only for a topic the replica holds). gate parks the committed
// read when parkAhead is false and the acked-ahead read, the last read
// before the store, when it is true.
func zzWP23FenceWire(dataDir string, store *metastore.Store, committer *ConsumerOffsetCommitter, gate *zzWP23FenceGate, parkAhead bool) *consumer.InFlight {
	offsets := consumer.NewInFlight(func(ctx context.Context, name string) (consumer.Caps, error) {
		if _, err := store.GetTopic(ctx, name); err != nil {
			return consumer.Caps{}, err
		}
		return consumer.Caps{MaxInFlight: 1 << 10, MaxAckedAhead: 1 << 10}, nil
	}, committer.Commit)
	offsets.SetCommittedRecovery(func(topicName string, p int) (int64, bool) {
		committed, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, topicName, p))
		if !parkAhead {
			gate.park()
		}
		return committed, ok && err == nil
	})
	offsets.SetAheadRecovery(func(topicName string, p int) (int64, []int64, bool) {
		rec, ok, err := storage.ReadConsumerAhead(storage.TopicPartitionDir(dataDir, topicName, p))
		if parkAhead {
			gate.park()
		}
		return rec.Committed, rec.Offsets, ok && err == nil
	})
	committer.SetAheadSource(offsets.AheadSnapshot)
	offsets.SetDropNotifier(committer.Forget)
	return offsets
}

// zzWP23FencePersisted is the frontier a recovery of dir starts from:
// the larger of the two files' frontiers.
func zzWP23FencePersisted(dir string) int64 {
	frontier := int64(-1)
	if off, ok, err := storage.ReadConsumerOffset(dir); err == nil && ok {
		frontier = off
	}
	if rec, ok, err := storage.ReadConsumerAhead(dir); err == nil && ok {
		frontier = max(frontier, rec.Committed)
	}
	return frontier
}

// zzWP23FenceAppend appends n records after what l holds and commits
// them.
func zzWP23FenceAppend(t *testing.T, l *storage.Log, n int) {
	t.Helper()
	first := l.NextOffset()
	for range n {
		if _, err := l.Append(storage.EncodeKeyedRecord("k", 1, []byte("new-incarnation"))); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.CommitDurable(first, first+int64(n)-1); err != nil {
		t.Fatalf("CommitDurable: %v", err)
	}
}

// The retires:
//
//   - quarantine-open: the topic was deleted and recreated and the purge
//     never reached this node; consumer B's Get of the recreated topic
//     quarantines the directory and runs the retired hook once.
//   - purge: consumer A resolved caps while the old record existed; the
//     delete and the purge run under it (the hook runs before and after
//     the removal), and the name is recreated afterwards.
//   - purge-after-recreate: the metastore already holds the recreated
//     record when the purge of the old incarnation runs; its directory
//     still carries the old ID, so the purge removes it.
func TestZZWP23ShardCreateStraddlingRetire(t *testing.T) {
	for _, via := range []string{"quarantine-open", "purge", "purge-after-recreate"} {
		for _, park := range []string{"after-offset-read", "after-ahead-read"} {
			t.Run(via+"/"+park, func(t *testing.T) {
				t.Parallel()
				zzWP23FenceStraddle(t, via, park == "after-ahead-read")
			})
		}
	}
}

func zzWP23FenceStraddle(t *testing.T, via string, parkAhead bool) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	partDir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceOldID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}

	// An earlier process: the old incarnation's consumers acked 0..19,
	// and a clean stop persisted the frontier.
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
			t.Fatalf("setup: the old incarnation persisted frontier %d, want 19", got)
		}
	}

	// This process. No shard exists for the partition yet.
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()
	committer := zzWP23ManualCommitter(dataDir)
	defer func() { _ = committer.Close() }()
	gate := &zzWP23FenceGate{read: make(chan struct{}), release: make(chan struct{})}
	offsets := zzWP23FenceWire(dataDir, store, committer, gate, parkAhead)
	var hookRuns atomic.Int32
	hookEntered := make(chan struct{})
	logs.SetTopicRetiredHook(func(name string) {
		if hookRuns.Add(1) == 1 {
			close(hookEntered)
		}
		offsets.DropTopic(name)
	})

	// Consumer A resolves the old incarnation's log (the consume scan's
	// Get) and goes on to reserve on it.
	oldLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if via != "purge" {
		if err := store.DeleteTopic(ctx, "orders"); err != nil {
			t.Fatal(err)
		}
		if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceNewID, Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	gate.armed.Store(true)
	type outcome struct {
		res     consumer.ReserveResult
		err     error
		readErr error
	}
	done := make(chan outcome, 1)
	tail := oldLog.HighWatermark()
	go func() {
		res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, tail)
		o := outcome{res: res, err: err}
		if err == nil && res.Reserved {
			// consume.go reads the record; the read on the closed old log
			// fails, and resolveUnreadable gives the reservation back.
			if _, _, _, o.readErr = oldLog.ReadKeyedShared(res.Offset); o.readErr != nil {
				_ = offsets.ReleaseHandle("orders", 0, res.Offset, res.Nonce)
			}
		}
		done <- o
	}()
	select {
	case <-gate.read:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: consumer A never reached its recovery read")
	}

	// The retire runs on its own goroutine while A is parked between its
	// read and its store: with the fence its drop waits for A's store.
	var newLog *storage.Log
	retired := make(chan error, 1)
	go func() {
		switch via {
		case "quarantine-open":
			l, err := logs.Get("orders", 0)
			newLog = l
			retired <- err
		case "purge", "purge-after-recreate":
			if via == "purge" {
				if err := store.DeleteTopic(ctx, "orders"); err != nil {
					retired <- err
					return
				}
			}
			purged, err := logs.PurgeTopic("orders", zzWP23FenceOldID)
			if err == nil && !purged {
				err = errors.New("not purged")
			}
			retired <- err
		}
	}()
	select {
	case <-hookEntered:
	case err := <-retired:
		t.Fatalf("setup: the retire finished without running the retired hook: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("setup: the retire never ran the retired hook")
	}
	// Without the fence the whole retire finishes here. With it the
	// retire waits in its first drop until A stores; A is released after
	// a moment either way.
	var retireErr error
	waited := false
	select {
	case retireErr = <-retired:
	case <-time.After(time.Second):
		waited = true
	}
	close(gate.release)
	a := <-done
	if waited {
		retireErr = <-retired
		t.Logf("the retire waited for consumer A's shard create")
	}
	if retireErr != nil {
		t.Fatal(retireErr)
	}
	t.Logf("consumer A (old incarnation's log): reserve %+v err %v, read err %v; retired hook ran %d times",
		a.res, a.err, a.readErr, hookRuns.Load())
	if via != "quarantine-open" {
		if via == "purge" {
			if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23FenceNewID, Partitions: 1}); err != nil {
				t.Fatal(err)
			}
		}
		if newLog, err = logs.Get("orders", 0); err != nil {
			t.Fatal(err)
		}
	}
	if hwm := newLog.HighWatermark(); hwm != 0 {
		t.Fatalf("setup: the recreated topic's log starts at hwm %d, want 0", hwm)
	}

	// The recreated topic's first records.
	zzWP23FenceAppend(t, newLog, 10)
	if committed, ok := offsets.CommittedOffset("orders", 0); ok {
		t.Errorf("a shard of the retired incarnation (frontier %d) outlived the retire", committed)
	}
	res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, newLog.HighWatermark())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Reserved || res.Offset != 0 {
		t.Errorf("LOSS: the recreated topic's first reserve is %+v, want offset 0", res)
	} else if err := offsets.ReleaseHandle("orders", 0, res.Offset, res.Nonce); err != nil {
		t.Fatal(err)
	}

	// Its consumers ack what they are handed, and a clean stop persists.
	zzWP23FenceAppend(t, newLog, 20)
	var acked []int64
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
		acked = append(acked, r.Offset)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	persisted := zzWP23FencePersisted(partDir)
	if len(acked) != 30 || acked[0] != 0 {
		first := int64(-1)
		if len(acked) > 0 {
			first = acked[0]
		}
		t.Errorf("LOSS persisted: the recreated topic's consumers acked %d offsets from %d and its directory holds frontier %d; want 30 from 0",
			len(acked), first, persisted)
	} else if persisted != 29 {
		t.Errorf("the recreated topic's directory holds frontier %d after acking 0..29, want 29", persisted)
	}
}
