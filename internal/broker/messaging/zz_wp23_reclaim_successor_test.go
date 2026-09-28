package messaging

// ReclaimMovedPartitionGuarded checks the assignment, closes the
// partition's log and resets its consumer state, then acts on the
// partition's PATH: storage.NewLog(dir) to recover the copy's next
// offset, then a rename of dir to dir.quarantine or os.RemoveAll(dir).
// If the topic is deleted and recreated in between and this node serves
// the successor's partition (the successor's open quarantines the old
// topic directory and makes a new partition directory), a reclaim that
// re-checks nothing removes or renames the SUCCESSOR's partition
// directory, with its records and consumer state. The reclaim now acts
// under the topic's guard with the partition's log closed, and only
// while the topic marker is absent or names the incarnation it read.
//
// The only seam is the drop notifier (production wires the committer's
// Forget there), which runs the recreate and the successor's first
// produce at the reclaim's reset, standing for the recreate landing
// between the reclaim's assignment check and its removal.

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

func TestZZWP23ReclaimNeverRemovesSuccessorDir(t *testing.T) {
	for _, known := range []bool{false, true} {
		name := "unguarded"
		if known {
			name = "guard-known"
		}
		t.Run(name, func(t *testing.T) { zzWP23Reclaim(t, known) })
	}
}

func zzWP23Reclaim(t *testing.T, known bool) {
	ctx := context.Background()
	store := newTestStore(t)
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23StaleOldID, Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	dataDir := t.TempDir()
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })

	// This node owned orders/0 earlier and holds its old copy; the
	// partition has since moved to node-other.
	old, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := old.Append(storage.EncodeKeyedRecord("k", 1, []byte("old"))); err != nil {
			t.Fatal(err)
		}
	}
	if err := old.CommitDurable(0, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-other"); err != nil {
		t.Fatal(err)
	}

	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	var successorHWM int64
	var hookErr error
	fired := false
	offsets.SetDropNotifier(func(string, int) {
		if fired {
			return
		}
		fired = true
		if hookErr = store.DeleteTopic(ctx, "orders"); hookErr != nil {
			return
		}
		if hookErr = store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: zzWP23StaleNewID, Partitions: 1}); hookErr != nil {
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
	})
	e := NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "node-self")
	t.Cleanup(func() { e.dispatch.close() })

	rerr := e.ReclaimMovedPartitionGuarded(ctx, "orders", 0, ReclaimGuard{Known: known, PromotedHWM: 5})
	if hookErr != nil {
		t.Fatalf("setup: the successor's produce failed: %v", hookErr)
	}
	if successorHWM != 7 {
		t.Fatalf("setup: the successor committed hwm %d, want 7", successorHWM)
	}
	t.Logf("reclaim: %v", rerr)
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
		t.Errorf("LOSS: the successor's partition directory recovers next offset %d after the reclaim, want 7 (its 7 committed records); topic marker %q (marked %v)", got, id, marked)
	}
}
