package messaging

// A consume that resolved a deleted incarnation's log (the scan's Get
// fast path) and reserves after the retired hook's DropTopic makes a
// FRESH shard: it reads the path after the rename or the removal, so it
// recovers nothing of the deleted incarnation, and the create fence has
// nothing to wait for. The shard is keyed by the topic name, so it is
// the successor's shard. The scan then reads the reserved offset from
// the closed old log. Below the old log's oldest retained offset that
// read answered ErrOffsetNotFound, and resolveUnreadable skipped the
// successor's shard to the OLD log's oldest retained offset: the
// successor's records below it were never delivered, and the committer
// persisted the skipped frontier into the successor's directory. A
// closed log now answers ErrLogClosed, which resolveUnreadable treats as
// transient (release the reservation, fail the consume), never as a gap.
//
// The only seam is a park inside the caps resolver (after it read the
// topic record, as cmd/narad's capsResolver does), which stands for the
// consume goroutine being descheduled between its Get and its shard
// store. Everything else is production code: a Raft metastore,
// runtime.Logs, consumer.InFlight and the committer wired as
// cmd/narad/serve_wiring.go wires them, the engine's consume scan, and a
// retired hook that does what topics.Manager.dropTopicState does.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

const (
	zzWP23StaleOldID = "aaaaaaaaaaaaaaaa"
	zzWP23StaleNewID = "bbbbbbbbbbbbbbbb"
)

func TestZZWP23StaleLogNeverSkipsSuccessor(t *testing.T) {
	for _, via := range []string{"quarantine-open", "purge"} {
		t.Run(via, func(t *testing.T) { zzWP23StaleLogSkip(t, via) })
	}
}

func zzWP23StalePersisted(dir string) int64 {
	frontier := int64(-1)
	if off, ok, err := storage.ReadConsumerOffset(dir); err == nil && ok {
		frontier = off
	}
	if rec, ok, err := storage.ReadConsumerAhead(dir); err == nil && ok {
		frontier = max(frontier, rec.Committed)
	}
	return frontier
}

func zzWP23StaleLogSkip(t *testing.T, via string) {
	ctx := context.Background()
	store := newTestStore(t)
	dataDir := t.TempDir()
	partDir := storage.TopicPartitionDir(dataDir, "orders", 0)
	mk := func(id string) topic.Topic {
		return topic.Topic{Name: "orders", ID: id, Partitions: 1, VisibilityTimeoutMs: 60_000, RetentionMs: 1}
	}
	if err := store.CreateTopic(ctx, mk(zzWP23StaleOldID)); err != nil {
		t.Fatal(err)
	}

	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 2 * time.Millisecond, SegmentBytes: 256}, store, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	committer := runtime.NewConsumerOffsetCommitter(dataDir, 10*time.Millisecond, nil)
	committerClosed := false
	t.Cleanup(func() {
		if !committerClosed {
			_ = committer.Close()
		}
	})

	var armed atomic.Bool
	parked := make(chan struct{})
	release := make(chan struct{})
	offsets := consumer.NewInFlight(func(ctx context.Context, name string) (consumer.Caps, error) {
		// As cmd/narad's capsResolver: the caps come from the record.
		if _, err := store.GetTopic(ctx, name); err != nil {
			return consumer.Caps{}, err
		}
		if armed.CompareAndSwap(true, false) {
			close(parked)
			<-release
		}
		return consumer.Caps{MaxInFlight: 1000, MaxAckedAhead: 1000}, nil
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

	e := NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	var hookRuns atomic.Int32
	logs.SetTopicRetiredHook(func(name string) {
		// topics.Manager.dropTopicState, minus the schema registry.
		hookRuns.Add(1)
		e.ReleaseTopicWaiters(name)
		offsets.DropTopic(name)
		e.ForgetTopic(name)
	})

	// The old incarnation: records, and retention reaped its head.
	for i := range 40 {
		if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", zzWP23StaleOldID, 0, 5, fmt.Sprint("a", i))); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	oldLog, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	oldLog.SweepRetentionNow()
	for i := range 40 {
		if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", zzWP23StaleOldID, 0, 5, fmt.Sprint("b", i))); err != nil {
			t.Fatal(err)
		}
	}
	oldest := oldLog.OldestOffset()
	if oldest < 21 {
		t.Fatalf("setup: retention reaped too little: oldest=%d", oldest)
	}
	t.Logf("old incarnation: oldest retained %d, hwm %d", oldest, oldLog.HighWatermark())

	// Consumer A: a plain consume of the old incarnation. Its scan takes
	// the old log from Get's fast path and reserves; no shard exists yet
	// (or the retire just dropped it), so the reserve makes one and is
	// descheduled there.
	armed.Store(true)
	type aResult struct {
		msg   topic.Message
		found bool
		err   error
	}
	aDone := make(chan aResult, 1)
	go func() {
		m, f, err := e.Consume(ctx, "orders", ConsumeOpts{})
		aDone <- aResult{m, f, err}
	}()
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("setup: consumer A never reached its shard create")
	}

	// The retire.
	switch via {
	case "quarantine-open":
		if err := store.DeleteTopic(ctx, "orders"); err != nil {
			t.Fatal(err)
		}
		if err := store.CreateTopic(ctx, mk(zzWP23StaleNewID)); err != nil {
			t.Fatal(err)
		}
		// Consumer or producer B of the recreated topic: its Get
		// quarantines the old directory and runs the retired hook.
		if _, err := logs.Get("orders", 0); err != nil {
			t.Fatal(err)
		}
	case "purge":
		if err := store.DeleteTopic(ctx, "orders"); err != nil {
			t.Fatal(err)
		}
		if purged, err := logs.PurgeTopic("orders", zzWP23StaleOldID); err != nil || !purged {
			t.Fatalf("purge: %v %v", purged, err)
		}
	}
	if hookRuns.Load() == 0 {
		t.Fatal("setup: the retire never ran the retired hook")
	}
	close(release)
	a := <-aDone
	t.Logf("consumer A (old log): found=%v offset=%d err=%v; hook ran %d times", a.found, a.msg.Offset, a.err, hookRuns.Load())
	if committed, ok := offsets.CommittedOffset("orders", 0); ok {
		t.Logf("live shard for orders/0 after A: committed %d", committed)
	}

	if via == "purge" {
		if err := store.CreateTopic(ctx, mk(zzWP23StaleNewID)); err != nil {
			t.Fatal(err)
		}
	}
	// The recreated topic's first 220 records.
	for i := range 44 {
		if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", zzWP23StaleNewID, 0, 5, fmt.Sprint("n", i))); err != nil {
			t.Fatal(err)
		}
	}
	var got []int64
	for range 1000 {
		m, found, err := e.Consume(ctx, "orders", ConsumeOpts{})
		if err != nil {
			t.Fatalf("consume of the recreated topic: %v", err)
		}
		if !found {
			break
		}
		got = append(got, m.Offset)
		h, err := consumer.DecodeHandle(m.ReceiptHandle)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Ack(ctx, "orders", h); err != nil {
			t.Fatal(err)
		}
	}
	if err := committer.Close(); err != nil {
		t.Logf("committer close: %v", err)
	}
	committerClosed = true
	persisted := zzWP23StalePersisted(partDir)
	first := int64(-1)
	if len(got) > 0 {
		first = got[0]
	}
	if len(got) != 220 || first != 0 {
		t.Errorf("LOSS: the recreated topic holds offsets 0..219; its consumers were handed %d records from offset %d and its directory holds frontier %d (the old incarnation's oldest retained offset was %d)",
			len(got), first, persisted, oldest)
	} else if persisted != 219 {
		t.Errorf("the recreated topic's directory holds frontier %d after acking 0..219, want 219", persisted)
	} else {
		t.Logf("the recreated topic delivered 0..219 and persisted frontier 219")
	}
}
