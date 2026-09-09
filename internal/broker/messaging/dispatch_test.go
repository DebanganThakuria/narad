package messaging

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// commitRecords appends n records to (topic, 0) and advances the high
// watermark so they are visible to consumers.
func commitRecords(t *testing.T, e *Engine, topicName string, n int) {
	t.Helper()
	log, err := e.logs.Get(topicName, 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	for i := range n {
		if _, err := log.Append(storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	if err := log.AdvanceHighWatermark(int64(n)); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
}

// parkConsumer probes and then parks a consumer, reporting the outcome
// on the returned channel. It returns once the consumer is queued.
func parkConsumer(t *testing.T, e *Engine, topicName string, wait time.Duration) <-chan bool {
	t.Helper()
	_, found, w, err := e.ConsumeProbe(context.Background(), topicName, ConsumeOpts{})
	if err != nil || found {
		t.Fatalf("ConsumeProbe() = (found %v, err %v), want a waiter", found, err)
	}
	out := make(chan bool, 1)
	ready := make(chan struct{})
	go func() {
		close(ready)
		_, got, err := e.ConsumeWait(context.Background(), w, wait)
		if err != nil {
			t.Errorf("ConsumeWait() error = %v", err)
		}
		out <- got
	}()
	<-ready
	return out
}

// TestOneCommitServesExactlyOneWaiter is the point of the dispatcher.
// With many consumers parked on a topic and a single record committed,
// exactly one is served and the rest stay parked. The design this
// replaced woke every one of them to rebuild a reflect.Select and lose a
// scan on the partition shard mutex.
func TestOneCommitServesExactlyOneWaiter(t *testing.T) {
	const waiters = 8
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	engine := newTestEngine(t, ms, nil, nil)

	outs := make([]<-chan bool, waiters)
	for i := range waiters {
		outs[i] = parkConsumer(t, engine, "orders", 400*time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let them all park

	commitRecords(t, engine, "orders", 1)

	served := 0
	for _, out := range outs {
		select {
		case got := <-out:
			if got {
				served++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a parked consumer never returned")
		}
	}
	if served != 1 {
		t.Fatalf("served %d consumers from one record, want exactly 1", served)
	}
}

// TestWaitersServedUpToRecordsAvailable pins the other half: the pump
// keeps handing records out while both sides of the gate hold, so three
// records committed at once reach three of the parked consumers rather
// than being drip-fed one per wake-up.
func TestWaitersServedUpToRecordsAvailable(t *testing.T) {
	const waiters = 6
	const records = 3
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	engine := newTestEngine(t, ms, nil, nil)

	outs := make([]<-chan bool, waiters)
	for i := range waiters {
		outs[i] = parkConsumer(t, engine, "orders", 400*time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)

	commitRecords(t, engine, "orders", records)

	served := 0
	for _, out := range outs {
		select {
		case got := <-out:
			if got {
				served++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("a parked consumer never returned")
		}
	}
	if served != records {
		t.Fatalf("served %d consumers from %d records, want %d", served, records, records)
	}
}

// TestNoReservationWithoutAWaiter pins the invariant that removes the
// give-back problem: the pump reserves only once it holds a waiter. A
// record committed while nobody is parked must still be sitting there,
// unreserved, for the next probe to take.
func TestNoReservationWithoutAWaiter(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	engine := newTestEngine(t, ms, nil, nil)

	commitRecords(t, engine, "orders", 1)
	// Give the pump every chance to wake and speculatively reserve.
	time.Sleep(100 * time.Millisecond)

	msg, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
	if err != nil {
		t.Fatalf("ConsumeProbe() error = %v", err)
	}
	if !found {
		t.Fatal("ConsumeProbe() found nothing: the pump reserved a record with no waiter to give it to")
	}
	if msg.Offset != 0 {
		t.Fatalf("probe returned offset %d, want 0", msg.Offset)
	}
}

// TestConsumeWaitReleasesRecordWhenClientLeaves pins the one give-back
// left on this side. A record handed over at the instant the request is
// cancelled has nobody to receive it, so it must go back immediately
// rather than sit invisible for a full visibility timeout.
func TestConsumeWaitReleasesRecordWhenClientLeaves(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	engine := newTestEngine(t, ms, nil, nil)

	_, found, w, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
	if err != nil || found {
		t.Fatalf("ConsumeProbe() = (found %v, err %v), want a waiter", found, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, got, err := engine.ConsumeWait(ctx, w, 5*time.Second); err != nil || got {
			t.Errorf("ConsumeWait() = (found %v, err %v), want empty after cancellation", got, err)
		}
	}()

	time.Sleep(30 * time.Millisecond)
	// Cancel and commit together so the delivery races the cancellation.
	cancel()
	commitRecords(t, engine, "orders", 1)
	wg.Wait()

	// Whether the pump got there first or not, the record must end up
	// available again rather than reserved to a consumer that has gone.
	deadline := time.Now().Add(3 * time.Second)
	for {
		msg, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
		if err != nil {
			t.Fatalf("ConsumeProbe() error = %v", err)
		}
		if found {
			if msg.Offset != 0 {
				t.Fatalf("recovered offset %d, want 0", msg.Offset)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the record was never released: it stayed reserved for a consumer that had already gone")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
