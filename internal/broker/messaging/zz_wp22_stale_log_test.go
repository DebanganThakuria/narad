package messaging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP22HookMS is the metastore as the partition-log map reads it. While
// armed, each GetTopic a lazy log open makes returns what the replica
// held at that moment and then runs hook, which models the replica
// applying changes while storage.NewLog recovers the partition.
type zzWP22HookMS struct {
	*zzWP7aLockedMetastore
	armed atomic.Int32
	hook  func()
}

func (m *zzWP22HookMS) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := m.zzWP7aLockedMetastore.GetTopic(ctx, name)
	if m.armed.Load() > 0 && m.armed.Add(-1) >= 0 {
		m.hook()
	}
	return t, err
}

type zzWP22Env struct {
	base   *zzWP7aLockedMetastore
	hooked *zzWP22HookMS
	logs   *runtime.Logs
	e      *Engine
}

func zzWP22Setup(t *testing.T) *zzWP22Env {
	t.Helper()
	base := &zzWP7aLockedMetastore{messagingFakeMetastore: newMessagingFakeMetastore()}
	if err := base.CreateTopic(context.Background(), topic.Topic{Name: "orders", ID: "inc-1", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	hooked := &zzWP22HookMS{zzWP7aLockedMetastore: base}
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, hooked, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(base, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	return &zzWP22Env{base: base, hooked: hooked, logs: logs, e: e}
}

// zzWP22WaitQueued waits until n commits are queued on the partition's
// combiner (the leader's own included).
func zzWP22WaitQueued(t *testing.T, e *Engine, n int) {
	t.Helper()
	c := e.combinerFor("orders", 0)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		queued := len(c.queue)
		c.mu.Unlock()
		if queued >= n {
			return
		}
		time.Sleep(100 * time.Microsecond)
	}
	t.Errorf("%d commits never queued on the combiner", n)
}

// A commit cycle resolves the partition's log once, and batches keep
// queueing until its drain. The owner's replica applies a delete and a
// recreate of the topic while the leader's lazy open of the old
// incarnation's log runs, and a commit for the new incarnation queues
// behind it. That commit must end up in the new incarnation's log (or
// be refused, for its caller to retry), never acked from the old log,
// which is quarantined and purged with the old incarnation.
func TestWP22CommitCycleRechecksItsLogAfterARecreate(t *testing.T) {
	ctx := context.Background()
	riders := []struct {
		name    string
		payload []byte
		commit  func(e *Engine, payload []byte) (int64, error)
	}{
		{
			name: "stamped", payload: []byte("stamped-new"),
			commit: func(e *Engine, payload []byte) (int64, error) {
				recs := zzWP7aRecords("orders", "inc-2", 0, 1, "x")
				recs[0].Payload = payload
				offs, err := e.CommitAcceptedProduceBatch(ctx, recs)
				if err != nil {
					return 0, err
				}
				return offs[0], nil
			},
		},
		{
			// A fan-out commit, or a record from a node that does not stamp
			// incarnations.
			name: "unstamped", payload: []byte("unstamped-new"),
			commit: func(e *Engine, payload []byte) (int64, error) {
				recs := zzWP7aRecords("orders", "", 0, 1, "x")
				recs[0].Payload = payload
				offs, err := e.CommitAcceptedProduceBatch(ctx, recs)
				if err != nil {
					return 0, err
				}
				return offs[0], nil
			},
		},
		{
			name: "sync produce", payload: []byte(`{"tag":"sync-new"}`),
			commit: func(e *Engine, payload []byte) (int64, error) {
				off, _, err := e.Produce(ctx, "orders", "k", payload, 0)
				return off, err
			},
		},
	}
	for _, rider := range riders {
		t.Run(rider.name, func(t *testing.T) {
			env := zzWP22Setup(t)
			// inc-1 holds a record, and its log is closed (idle eviction,
			// a move, a restart).
			if _, err := env.e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 1, "old")); err != nil {
				t.Fatal(err)
			}
			if err := env.logs.ClosePartition("orders", 0); err != nil {
				t.Fatal(err)
			}

			var riderOff int64
			var riderErr error
			var wg sync.WaitGroup
			env.hooked.hook = func() {
				if err := env.base.DeleteTopic(ctx, "orders"); err != nil {
					t.Error(err)
				}
				if err := env.base.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "inc-2", Partitions: 1}); err != nil {
					t.Error(err)
				}
				wg.Go(func() { riderOff, riderErr = rider.commit(env.e, rider.payload) })
				zzWP22WaitQueued(t, env.e, 2)
			}
			env.hooked.armed.Store(1)
			// A late commit of inc-1 (records still in some WAL) opens the log.
			_, leaderErr := env.e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 1, "late-old"))
			wg.Wait()
			if !errors.Is(leaderErr, ErrTopicIncarnationMismatch) {
				t.Errorf("the late inc-1 commit: err = %v, want ErrTopicIncarnationMismatch", leaderErr)
			}
			if riderErr != nil {
				t.Fatalf("the inc-2 commit was refused (%v); it should have been written to the inc-2 log", riderErr)
			}

			live, err := env.logs.Get("orders", 0)
			if err != nil {
				t.Fatal(err)
			}
			if riderOff >= live.HighWatermark() {
				t.Fatalf("the inc-2 commit was acked at offset %d, but the inc-2 log's high-watermark is %d: it went into the inc-1 log",
					riderOff, live.HighWatermark())
			}
			_, _, got, err := live.ReadKeyed(riderOff)
			if err != nil || !bytes.Equal(got, rider.payload) {
				t.Fatalf("inc-2 log offset %d holds %q (err %v), want %q", riderOff, got, err, rider.payload)
			}
		})
	}
}

// A topic record that changes under every resolution of the log does
// not keep the cycle going: after staleLogAttempts resolutions it refuses
// the whole cycle with a retriable error and writes nothing.
func TestWP22CommitCycleGivesUpOnALogThatNeverSettles(t *testing.T) {
	ctx := context.Background()
	env := zzWP22Setup(t)
	if _, err := env.e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "old")); err != nil {
		t.Fatal(err)
	}
	if err := env.logs.ClosePartition("orders", 0); err != nil {
		t.Fatal(err)
	}
	var gen atomic.Int64
	gen.Store(1)
	env.hooked.hook = func() {
		id := fmt.Sprintf("inc-%d", gen.Add(1))
		if err := env.base.DeleteTopic(ctx, "orders"); err != nil {
			t.Error(err)
		}
		if err := env.base.CreateTopic(ctx, topic.Topic{Name: "orders", ID: id, Partitions: 1}); err != nil {
			t.Error(err)
		}
	}
	env.hooked.armed.Store(staleLogAttempts)
	_, err := env.e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "rider"))
	if !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("err = %v, want ErrNotPartitionOwner once every resolution was stale", err)
	}
	if got := gen.Load() - 1; got != staleLogAttempts {
		t.Fatalf("the cycle resolved its log %d times, want %d", got, staleLogAttempts)
	}
	live, err := env.logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if hwm := live.HighWatermark(); hwm != 0 {
		t.Fatalf("the live log's high-watermark is %d after a refused cycle, want 0", hwm)
	}
	// With the record settled, the retry commits.
	if _, err := env.e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "retry")); err != nil {
		t.Fatalf("retry after the record settled: %v", err)
	}
}
