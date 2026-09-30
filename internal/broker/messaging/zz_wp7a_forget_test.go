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

// critic#4 remainder. Forgetting a retired topic drops its dispatch
// state, so the dispatcher no longer keeps an entry per topic name this
// node ever opened a log for.
func TestZZWP7aForgetTopicDropsDispatchState(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	e := newTestEngine(t, ms, nil, nil)
	if _, err := e.CommitAcceptedProduceBatch(context.Background(), zzWP7aRecords("orders", "", 0, 1, "x")); err != nil {
		t.Fatal(err)
	}
	if e.dispatch.peekState("orders") == nil {
		t.Fatal("opening the log created no dispatch state; the test exercises nothing")
	}
	e.ReleaseTopicWaiters("orders")
	e.ForgetTopic("orders")
	if st := e.dispatch.peekState("orders"); st != nil {
		t.Fatal("the forgotten topic's dispatch state is still in the map")
	}
}

// After a forget, a log opened before it (its wake notifier captured the
// forgotten state) still wakes a consumer that parks on the topic later.
func TestZZWP7aWakeAfterForgetReachesNewWaiter(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	if _, err := e.logs.Get("orders", 0); err != nil {
		t.Fatal(err)
	}
	e.ForgetTopic("orders")

	type result struct {
		found bool
		took  time.Duration
	}
	out := make(chan result, 1)
	begin := time.Now()
	go func() {
		_, found, err := e.Consume(ctx, "orders", ConsumeOpts{Wait: 5 * time.Second})
		if err != nil {
			t.Errorf("consume: %v", err)
		}
		out <- result{found, time.Since(begin)}
	}()
	waitFor(t, func() bool {
		st := e.dispatch.peekState("orders")
		return st != nil && st.hasWaiters.Load()
	}, "the consumer never parked")
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "x")); err != nil {
		t.Fatal(err)
	}
	if r := <-out; !r.found || r.took > 2*time.Second {
		t.Fatalf("parked consumer: found=%v after %v, want the record at once", r.found, r.took)
	}
}

// A forget keeps a state that still has a consumer parked on it (one
// that arrived after the topic's waiters were released).
func TestZZWP7aForgetKeepsStateWithParkedConsumer(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	out := make(chan bool, 1)
	go func() {
		_, found, err := e.Consume(ctx, "orders", ConsumeOpts{Wait: 5 * time.Second})
		if err != nil {
			t.Errorf("consume: %v", err)
		}
		out <- found
	}()
	waitFor(t, func() bool {
		st := e.dispatch.peekState("orders")
		return st != nil && st.hasWaiters.Load()
	}, "the consumer never parked")
	e.ForgetTopic("orders")
	if e.dispatch.peekState("orders") == nil {
		t.Fatal("forget dropped a state with a consumer parked on it")
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "x")); err != nil {
		t.Fatal(err)
	}
	if !<-out {
		t.Fatal("the parked consumer never got the record")
	}
}

// Forget leaves a state alone while the pump holds one of its waiters
// off the queue: dropping it then would strand the waiter the pump puts
// back.
func TestZZWP7aForgetWaitsForHeldWaiter(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	var armed, once atomic.Bool
	entered := make(chan struct{})
	release := make(chan struct{})
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		if armed.Load() && once.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	ctx := context.Background()
	if _, err := e.logs.Get("orders", 0); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	out := make(chan bool, 1)
	go func() {
		_, found, _, err := e.ConsumeWait(ctx, &ConsumeWaiter{
			topic: "orders", scan: []int{0}, visibilityTimeout: time.Minute, start: time.Now(),
		}, 5*time.Second, nil)
		if err != nil {
			t.Errorf("wait: %v", err)
		}
		out <- found
	}()
	<-entered // the pump holds the waiter, creating the partition's shard
	e.ForgetTopic("orders")
	st := e.dispatch.peekState("orders")
	if st == nil || st.retired.Load() {
		t.Fatal("forget dropped the state while the pump held one of its waiters")
	}
	close(release)
	if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aRecords("orders", "", 0, 1, "x")); err != nil {
		t.Fatal(err)
	}
	if !<-out {
		t.Fatal("the held waiter never got the record")
	}
}
