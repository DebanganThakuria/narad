package messaging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Incarnation ids of a deleted topic and the same-named topic created
// after it.
const (
	retiredIncarnation   = "aaaaaaaaaaaaaaaa"
	successorIncarnation = "bbbbbbbbbbbbbbbb"
)

// incarnationRecords builds n accepted records of one topic incarnation
// for partition, tagged so their payloads differ.
func incarnationRecords(topicName, topicID string, partition, n int, tag string) []ingress.ProduceRecord {
	records := make([]ingress.ProduceRecord, n)
	for i := range records {
		records[i] = ingress.ProduceRecord{
			Topic: topicName, TopicID: topicID, TargetPartition: partition,
			Key: fmt.Sprintf("%s-%d", tag, i), Payload: fmt.Appendf(nil, `{"tag":%q,"i":%d}`, tag, i),
		}
	}
	return records
}

// A consume that resolved a deleted incarnation's log (the scan's Get
// fast path) and reserves after the retire's DropTopic makes a fresh
// shard: it reads the path after the rename or the removal, so it
// recovers nothing of the deleted incarnation, and the shard, keyed by
// the topic name, is the successor's. The scan then reads the reserved
// offset from the closed old log. Below the old log's oldest retained
// offset that read used to answer "offset not found", and the scan
// skipped the successor's shard to the old log's oldest retained offset:
// the successor's records below it were never delivered. A closed log
// answers storage.ErrLogClosed, which the scan treats as transient,
// never as a gap.
//
// The only seam is a park inside the caps resolver (after it read the
// topic record, as cmd/narad's resolver does), standing for the consume
// goroutine being descheduled between its Get and its shard store.
func TestConsumeHoldingARetiredLogNeverSkipsTheSuccessor(t *testing.T) {
	for _, retire := range []string{"an open of the successor quarantines it", "a purge removes it"} {
		t.Run(retire, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			dataDir := t.TempDir()
			partDir := topicPartitionDirT(t, dataDir, "orders", 0)
			mk := func(id string) topic.Topic {
				return topic.Topic{Name: "orders", ID: id, Partitions: 1, VisibilityTimeoutMs: 60_000, RetentionMs: 1}
			}
			if err := store.CreateTopic(ctx, mk(retiredIncarnation)); err != nil {
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
				committed, ok, err := storage.ReadConsumerOffset(topicPartitionDirT(t, dataDir, topicName, p))
				return committed, ok && err == nil
			})
			offsets.SetAheadRecovery(func(topicName string, p int) (int64, []int64, bool) {
				rec, ok, err := storage.ReadConsumerAhead(topicPartitionDirT(t, dataDir, topicName, p))
				return rec.Committed, rec.Offsets, ok && err == nil
			})
			committer.SetAheadSource(offsets.AheadSnapshot)
			offsets.SetDropNotifier(committer.Forget)

			e := NewEngine(store, schema.NewAlwaysValid(), fixedPartitionManager{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
			t.Cleanup(func() { e.dispatch.close() })
			var retires atomic.Int32
			logs.SetTopicRetiredHook(func(name string) {
				// topics.Manager.dropTopicState, minus the schema registry.
				retires.Add(1)
				e.ReleaseTopicWaiters(name)
				offsets.DropTopic(name)
				e.ForgetTopic(name)
			})

			// The deleted incarnation: records, and retention reaped its
			// head.
			for i := range 40 {
				if _, err := e.CommitAcceptedProduceBatch(ctx, incarnationRecords("orders", retiredIncarnation, 0, 5, fmt.Sprint("a", i))); err != nil {
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
				if _, err := e.CommitAcceptedProduceBatch(ctx, incarnationRecords("orders", retiredIncarnation, 0, 5, fmt.Sprint("b", i))); err != nil {
					t.Fatal(err)
				}
			}
			oldest := oldLog.OldestOffset()
			if oldest < 21 {
				t.Fatalf("setup: retention reaped too little: oldest=%d", oldest)
			}

			// Consumer A: its scan takes the old log from Get's fast path
			// and is descheduled making the partition's shard.
			armed.Store(true)
			aDone := make(chan error, 1)
			go func() {
				_, _, err := e.Consume(ctx, "orders", ConsumeOpts{})
				aDone <- err
			}()
			select {
			case <-parked:
			case <-time.After(10 * time.Second):
				t.Fatal("setup: consumer A never reached its shard create")
			}

			if err := store.DeleteTopic(ctx, "orders"); err != nil {
				t.Fatal(err)
			}
			if retire == "a purge removes it" {
				if purged, err := logs.PurgeTopic("orders", retiredIncarnation); err != nil || !purged {
					t.Fatalf("purge: %v %v", purged, err)
				}
			} else {
				if err := store.CreateTopic(ctx, mk(successorIncarnation)); err != nil {
					t.Fatal(err)
				}
				if _, err := logs.Get("orders", 0); err != nil {
					t.Fatal(err)
				}
			}
			if retires.Load() == 0 {
				t.Fatal("setup: the retire never ran the retired hook")
			}
			close(release)
			t.Logf("consumer A (retired log): %v", <-aDone)
			if retire == "a purge removes it" {
				if err := store.CreateTopic(ctx, mk(successorIncarnation)); err != nil {
					t.Fatal(err)
				}
			}

			// The successor's first 220 records.
			for i := range 44 {
				if _, err := e.CommitAcceptedProduceBatch(ctx, incarnationRecords("orders", successorIncarnation, 0, 5, fmt.Sprint("n", i))); err != nil {
					t.Fatal(err)
				}
			}
			var got []int64
			for range 1000 {
				m, found, err := e.Consume(ctx, "orders", ConsumeOpts{})
				if err != nil {
					t.Fatalf("consume of the successor: %v", err)
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
			first := int64(-1)
			if len(got) > 0 {
				first = got[0]
			}
			if len(got) != 220 || first != 0 {
				t.Fatalf("the successor holds offsets 0..219; its consumers were handed %d records from offset %d (the deleted incarnation's oldest retained offset was %d)",
					len(got), first, oldest)
			}
			persisted := int64(-1)
			if off, ok, err := storage.ReadConsumerOffset(partDir); err == nil && ok {
				persisted = off
			}
			if rec, ok, err := storage.ReadConsumerAhead(partDir); err == nil && ok {
				persisted = max(persisted, rec.Committed)
			}
			if persisted != 219 {
				t.Fatalf("the successor's directory holds frontier %d after acking 0..219, want 219", persisted)
			}
		})
	}
}
