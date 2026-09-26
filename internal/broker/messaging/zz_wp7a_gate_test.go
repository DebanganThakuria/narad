package messaging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// produce-dispatch-commit#4. A commit passed the owner and freeze gate,
// then waited for the produce lock behind a commit in progress. A
// handoff froze the partition meanwhile; its own lock section, which
// reads the final high-watermark, would run after the late commit. The
// gate is checked again under the lock, so the late commit is turned
// away with nothing appended instead of landing after the fence.
func TestZZWP7aCommitChecksFreezeUnderProduceLock(t *testing.T) {
	for _, path := range []string{"batch", "single", "produce"} {
		t.Run(path, func(t *testing.T) {
			ms := newMessagingFakeMetastore()
			ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
			e := newTestEngine(t, ms, nil, nil)
			ctx := context.Background()
			if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "first")); err != nil {
				t.Fatal(err)
			}

			release := zzWP7aHoldProduceLock(t, e, "orders", 0)
			out := make(chan error, 1)
			go func() {
				var err error
				switch path {
				case "batch":
					_, err = e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 3, "late"))
				case "single":
					_, err = e.CommitAcceptedProduce(ctx, zzWP7aRecords("orders", "", 0, 1, "late")[0])
				case "produce":
					_, _, err = e.Produce(ctx, "orders", "late", []byte(`{"late":true}`))
				}
				out <- err
			}()
			zzWP7aWaitStack(t, "the late commit to queue on the produce lock", func(count func(string) int) bool {
				return count("(*Logs).lockProduce(") >= 1
			})

			e.PauseProduceForHandoff("orders", 0, time.Minute)
			release()
			err := <-out
			if !errors.Is(err, ErrNotPartitionOwner) {
				t.Fatalf("late commit after the freeze: err = %v, want ErrNotPartitionOwner", err)
			}
			if got := zzWP7aHWM(t, e, "orders", 0); got != 1 {
				t.Fatalf("high-watermark = %d after the refused commit, want 1", got)
			}
		})
	}
}

// A commit whose caller gave up while it waited for the produce lock is
// not appended: the caller retries anyway, so appending it would only
// commit a duplicate.
func TestZZWP7aCommitSkipsCallerThatGaveUp(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	if _, err := e.CommitAcceptedProduceBatch(context.Background(), zzWP7aRecords("orders", "", 0, 1, "first")); err != nil {
		t.Fatal(err)
	}
	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan error, 1)
	go func() {
		_, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 2, "late"))
		out <- err
	}()
	zzWP7aWaitStack(t, "the commit to queue on the produce lock", func(count func(string) int) bool {
		return count("(*Logs).lockProduce(") >= 1
	})
	cancel()
	release()
	if err := <-out; !errors.Is(err, context.Canceled) {
		t.Fatalf("commit of a caller that gave up: err = %v, want context.Canceled", err)
	}
	if got := zzWP7aHWM(t, e, "orders", 0); got != 1 {
		t.Fatalf("high-watermark = %d, want 1 (nothing of the abandoned commit)", got)
	}
}

// A commit the gate turns away under the produce lock is a rejection the
// dispatcher reroutes, not a produce error: the check before the lock
// never counted one, and its repeat under the lock must not either.
func TestZZWP7aRefusedCommitIsNotAProduceError(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry())
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, m, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	ctx := context.Background()
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "first")); err != nil {
		t.Fatal(err)
	}
	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	out := make(chan error, 1)
	go func() {
		_, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "late"))
		out <- err
	}()
	zzWP7aWaitStack(t, "the commit to queue on the produce lock", func(count func(string) int) bool {
		return count("(*Logs).lockProduce(") >= 1
	})
	e.PauseProduceForHandoff("orders", 0, time.Minute)
	release()
	if err := <-out; !errors.Is(err, ErrNotPartitionOwner) {
		t.Fatalf("err = %v, want ErrNotPartitionOwner", err)
	}
	if n := testutil.CollectAndCount(m.ErrorsTotal); n != 0 {
		t.Fatalf("the refused commit was counted as a produce error (%d error series)", n)
	}
}
