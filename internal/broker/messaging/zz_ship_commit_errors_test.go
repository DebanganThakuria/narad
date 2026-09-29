package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// A combined cycle whose partition log cannot be resolved committed
// nothing, so every batch queued into it shares that failure: each
// caller gets the error and returns, none is left parked on its wake.
// Here the topic is deleted while the batches wait for the produce lock.
func TestCombinedCycleLogResolutionFailureFailsEveryCaller(t *testing.T) {
	ms := &zzWP7aLockedMetastore{messagingFakeMetastore: newMessagingFakeMetastore()}
	ctx := context.Background()
	if err := ms.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "inc-1", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	e := zzWP7aEngineOver(t, ms)
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 1, "first")); err != nil {
		t.Fatal(err)
	}

	const callers = 3
	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for c := range callers {
		wg.Go(func() {
			_, errs[c] = e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 2, fmt.Sprint("c", c)))
		})
	}
	zzWP7aWaitQueued(t, e, "orders", 0, callers)
	if err := ms.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	release()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("callers queued into a cycle whose log could not be resolved never returned")
	}
	for c, err := range errs {
		if !errors.Is(err, ErrTopicNotFound) {
			t.Fatalf("caller %d: err = %v, want ErrTopicNotFound", c, err)
		}
	}
	if l, open := e.logs.Peek("orders", 0); open && l.HighWatermark() != 1 {
		t.Fatalf("high-watermark = %d after the failed cycle, want 1", l.HighWatermark())
	}
}

// A batch the incarnation re-check under the produce lock turns away
// (the topic was deleted and recreated while it waited) is a rejection
// the dispatcher retries, not a produce error, and is not counted as
// one.
func TestIncarnationMismatchUnderLockIsNotAProduceError(t *testing.T) {
	ms := &zzWP7aLockedMetastore{messagingFakeMetastore: newMessagingFakeMetastore()}
	ctx := context.Background()
	if err := ms.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "inc-1", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	e := zzWP7aEngineOver(t, ms)
	m := metrics.New(prometheus.NewRegistry())
	e.metrics = m
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 1, "first")); err != nil {
		t.Fatal(err)
	}

	release := zzWP7aHoldProduceLock(t, e, "orders", 0)
	out := make(chan error, 1)
	go func() {
		_, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "inc-1", 0, 2, "late"))
		out <- err
	}()
	zzWP7aWaitQueued(t, e, "orders", 0, 1)
	if err := ms.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "inc-2", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-out; !errors.Is(err, ErrTopicIncarnationMismatch) {
		t.Fatalf("err = %v, want ErrTopicIncarnationMismatch", err)
	}
	if n := testutil.CollectAndCount(m.ErrorsTotal); n != 0 {
		t.Fatalf("the refused commit was counted as a produce error (%d error series)", n)
	}
}
