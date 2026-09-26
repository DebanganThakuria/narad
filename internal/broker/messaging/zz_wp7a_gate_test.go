package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
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
