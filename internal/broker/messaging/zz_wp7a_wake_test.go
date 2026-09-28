package messaging

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// consume-local#2. The pump took waiter A off the queue and was still
// scanning for it when the only other waiter, B, timed out: its dequeue
// stored hasWaiters false, and a commit landing then was dropped by the
// wake notifier. The pump put A back without looking again, so A slept
// its whole wait with a record it could have had.
func TestZZWP7aPumpDoesNotLoseWakeWhileHoldingWaiter(t *testing.T) {
	dataDir := t.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 2, VisibilityTimeoutMs: 60_000}
	var armed, once atomic.Bool
	entered := make(chan struct{})
	release := make(chan struct{})
	resolver := func(context.Context, string) (consumer.Caps, error) {
		if armed.Load() && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(resolver, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	ctx := context.Background()
	if err := offsets.Init(ctx, "orders", 0, -1); err != nil {
		t.Fatal(err)
	}
	for p := range 2 {
		if _, err := e.logs.Get("orders", p); err != nil {
			t.Fatal(err)
		}
	}

	p0 := 0
	bDone := make(chan bool, 1)
	go func() {
		_, found, err := e.Consume(ctx, "orders", ConsumeOpts{Partition: &p0, Wait: 300 * time.Millisecond})
		if err != nil {
			t.Errorf("B: %v", err)
		}
		bDone <- found
	}()
	waitFor(t, func() bool { return e.dispatch.stateFor("orders").hasWaiters.Load() }, "B never parked")

	armed.Store(true)
	type result struct {
		found bool
		took  time.Duration
	}
	aDone := make(chan result, 1)
	aStart := time.Now()
	go func() {
		_, found, _, err := e.ConsumeWait(ctx, &ConsumeWaiter{
			topic: "orders", scan: []int{0, 1}, visibilityTimeout: time.Minute, start: time.Now(),
		}, 3*time.Second, nil)
		if err != nil {
			t.Errorf("A: %v", err)
		}
		aDone <- result{found, time.Since(aStart)}
	}()
	select {
	case <-entered: // the pump holds A, stuck creating p1's shard
	case <-time.After(5 * time.Second):
		t.Fatal("the pump never reached p1's shard creation")
	}
	if <-bDone {
		t.Fatal("B got a record")
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "late")); err != nil {
		t.Fatal(err)
	}
	close(release)
	r := <-aDone
	if !r.found || r.took > 2*time.Second {
		t.Fatalf("A: found=%v after %v, want the record p0 got while the pump held A", r.found, r.took)
	}
}
