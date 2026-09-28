package cluster

// A move acts on the destination's partition PATH after it checked the
// incarnation (finishMove's EnsureTopicIncarnation): the install's swap
// (RemoveAll, then Rename) and the rollback after a rejected flip. The
// flip is rejected exactly when the topic was deleted under the move. If
// the name was recreated and this node served the successor's partition
// in between (its open quarantines the old topic directory and makes a
// new partition directory), a swap or rollback that re-checks nothing
// removes the SUCCESSOR's partition directory: its acked records and its
// consumer state. The swap and the rollback now run under the topic's
// guard with the partition's log closed, and act only while the topic
// marker still names the move's incarnation; the rollback also only
// while the path still names the directory the install put there.
//
// The seams are fakeMoveStore's completeHook (during-flip) and the
// first ResetPartitionConsumerState (before-install), which run the
// delete, the recreate and the successor's first produce. The
// destination's directory handling is production code: a real
// runtime.Logs over a real Raft metastore installs through
// ReplacePartitionDir and prepares the incarnation through
// EnsureTopicIncarnation, as *messaging.Engine does.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

const (
	zzWP23RollbackOldID = "1111111111111111"
	zzWP23RollbackNewID = "2222222222222222"
)

type zzWP23RollbackDest struct {
	logs    *runtime.Logs
	resets  int
	onReset func(n int)
}

func (d *zzWP23RollbackDest) ReclaimMovedPartition(context.Context, string, int) error { return nil }
func (d *zzWP23RollbackDest) ResetPartitionConsumerState(string, int) {
	d.resets++
	if d.onReset != nil {
		d.onReset(d.resets)
	}
}

func (d *zzWP23RollbackDest) InstallPartitionDir(topicName string, partition int, swap func() error) error {
	return d.logs.ReplacePartitionDir(topicName, partition, swap)
}

func (d *zzWP23RollbackDest) EnsureTopicIncarnation(topicName, id string) error {
	return d.logs.EnsureTopicIncarnation(topicName, id)
}

func TestZZWP23MoveNeverRemovesSuccessorDir(t *testing.T) {
	for _, when := range []string{"during-flip", "before-install", "CONTROL-rejected"} {
		t.Run(when, func(t *testing.T) { zzWP23Rollback(t, when) })
	}
}

func zzWP23Rollback(t *testing.T, when string) {
	ctx := context.Background()
	real := newTestStore(t)
	if err := real.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23RollbackOldID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	hwm, _ := buildSourcePartition(t, src, 10)
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, real, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	dest := &zzWP23RollbackDest{logs: logs}

	var successorHWM int64
	var hookErr error
	store := &fakeMoveStore{
		assignment:  metastore.Assignment{Topic: "orders", Partition: 0, OwnerID: "narad-src", TargetID: "narad-dst"},
		member:      metastore.Member{ID: "narad-src", Addr: "srcaddr", Status: metastore.MemberAlive},
		topics:      []topic.Topic{{Name: "orders", ID: zzWP23RollbackOldID, Partitions: 1}},
		completeErr: errors.New("flip rejected: the topic was deleted"),
	}
	moveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	recreate := func() {
		// The topic is deleted and recreated, and the successor's
		// partition is produced to on this node. The runner's replica
		// sees the recreated record too; one attempt only.
		defer cancel()
		store.topics = []topic.Topic{{Name: "orders", ID: zzWP23RollbackNewID, Partitions: 1}}
		if hookErr = real.DeleteTopic(ctx, "orders"); hookErr != nil {
			return
		}
		if hookErr = real.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23RollbackNewID, Partitions: 1}); hookErr != nil {
			return
		}
		l, err := logs.Get("orders", 0)
		if err != nil {
			hookErr = err
			return
		}
		for range 7 {
			if _, err := l.Append(storage.EncodeKeyedRecord("k", 1, []byte("successor"))); err != nil {
				hookErr = err
				return
			}
		}
		if hookErr = l.CommitDurable(0, 6); hookErr != nil {
			return
		}
		successorHWM = l.HighWatermark()
	}
	switch when {
	case "during-flip":
		store.completeHook = recreate
	case "CONTROL-rejected":
		// The flip is rejected with the name left alone: the rollback
		// must still remove the installed copy.
		store.completeHook = func() {}
	case "before-install":
		// After finishMove's EnsureTopicIncarnation, before its install
		// (the reset right before it).
		store.completeHook = func() {}
		dest.onReset = func(n int) {
			if n == 1 {
				recreate()
			}
		}
	}
	peer := movePeerFake{dirFetcher: dirFetcher{dir: src, hwm: hwm, committed: 5, hasCommitted: true}, incarnation: zzWP23RollbackOldID}
	r := NewMoveRunner(store, "narad-dst", dataDir, peer, dest, nil, nil, MoveConfig{})
	store.completeHook = func(inner func()) func() {
		return func() {
			inner()
			cancel() // one attempt: stop the worker's retry
		}
	}(store.completeHook)
	r.Reconcile(moveCtx)
	r.wg.Wait()
	if when == "CONTROL-rejected" {
		if len(store.completeArgs) == 0 {
			t.Fatal("setup: the flip was never proposed")
		}
		dir := storage.TopicPartitionDir(dataDir, "orders", 0)
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the rejected flip's install survives the rollback (stat err %v)", err)
		}
		if id, marked, _ := storage.ReadTopicIncarnation(storage.TopicDir(dataDir, "orders")); !marked || id != zzWP23RollbackOldID {
			t.Fatalf("topic marker %q (marked %v) after the rollback, want %s", id, marked, zzWP23RollbackOldID)
		}
		return
	}
	if hookErr != nil {
		t.Fatalf("setup: the successor's produce failed: %v", hookErr)
	}
	if successorHWM != 7 {
		t.Fatalf("setup: the successor committed hwm %d, want 7", successorHWM)
	}
	t.Logf("flip args %v", store.completeArgs)

	// The successor's acked records must still be on this node.
	if err := logs.CloseAll(); err != nil {
		t.Logf("close: %v", err)
	}
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	id, marked, _ := storage.ReadTopicIncarnation(storage.TopicDir(dataDir, "orders"))
	l, err := storage.NewLog(dir, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got := l.NextOffset(); got != 7 {
		t.Errorf("LOSS: the successor's partition directory recovers next offset %d after the rollback, want 7 (its 7 committed records); topic marker %q (marked %v)", got, id, marked)
	}
}
