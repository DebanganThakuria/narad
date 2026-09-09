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

// fakeRemote is a peer's standing interest, scripted for tests. It
// records every notification and lets the test decide the verdict.
type fakeRemote struct {
	mu       sync.Mutex
	notified int
	verdicts []bool // consumed in order; missing entries mean "claiming"
	expired  bool
	refuse   bool // report that nothing was spent (outbound queue full)
}

func (f *fakeRemote) Notify(_ string, done func(bool)) bool {
	f.mu.Lock()
	if f.refuse {
		f.mu.Unlock()
		return false
	}
	f.notified++
	claiming := true
	if len(f.verdicts) > 0 {
		claiming, f.verdicts = f.verdicts[0], f.verdicts[1:]
	}
	f.mu.Unlock()
	// Real peers answer over the network, never inline under the pump.
	go done(claiming)
	return true
}

func (f *fakeRemote) Expired() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.expired
}

func (f *fakeRemote) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.notified
}

func remoteTestEngine(t *testing.T) *Engine {
	t.Helper()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	return newTestEngine(t, ms, nil, nil)
}

// TestRemoteDemandIsNotifiedNotReserved is the invariant that makes the
// cross-node half safe: a peer is only ever TOLD that records exist. The
// record must still be sitting there unreserved, because a peer that
// never comes back must not be able to strand it.
func TestRemoteDemandIsNotifiedNotReserved(t *testing.T) {
	engine := remoteTestEngine(t)
	rd := &fakeRemote{}
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}

	commitRecords(t, engine, "orders", 1)
	waitFor(t, func() bool { return rd.count() == 1 }, "the peer was never notified")

	msg, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
	if err != nil {
		t.Fatalf("ConsumeProbe() error = %v", err)
	}
	if !found {
		t.Fatal("the record was reserved for the peer: notifying must not reserve")
	}
	if msg.Offset != 0 {
		t.Fatalf("claimed offset %d, want 0", msg.Offset)
	}
}

// TestRemoteDemandNotPromisedTwice pins the outstanding gate: one record
// yields one notification, not one per turn of the queue, so the same
// record is never promised to two peers at once.
func TestRemoteDemandNotPromisedTwice(t *testing.T) {
	engine := remoteTestEngine(t)
	// Two peers interested, one record to go round.
	a, b := &fakeRemote{verdicts: []bool{true}}, &fakeRemote{verdicts: []bool{true}}
	for _, rd := range []*fakeRemote{a, b} {
		if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
			t.Fatalf("RegisterRemoteDemand() error = %v", err)
		}
	}

	commitRecords(t, engine, "orders", 1)
	waitFor(t, func() bool { return a.count()+b.count() >= 1 }, "no peer was notified")
	time.Sleep(150 * time.Millisecond) // let any second notification escape

	if total := a.count() + b.count(); total != 1 {
		t.Fatalf("sent %d notifications for one record, want exactly 1", total)
	}
}

// TestRemoteDeclineFreesTheRecordAtOnce pins the pass path: a peer that
// says it will not claim releases its hold immediately, so the next peer
// hears about the record a round trip later rather than after a timeout.
func TestRemoteDeclineFreesTheRecordAtOnce(t *testing.T) {
	engine := remoteTestEngine(t)
	declining := &fakeRemote{verdicts: []bool{false}}
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", declining); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}
	taker := &fakeRemote{}
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", taker); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}

	commitRecords(t, engine, "orders", 1)
	waitFor(t, func() bool { return taker.count() >= 1 },
		"the declined record was never offered to the next peer")
}

// TestExpiredRemoteDemandLeavesTheQueue pins the lazy cleanup: expiry is
// noticed when the entry is popped, never by sweeping the queues.
func TestExpiredRemoteDemandLeavesTheQueue(t *testing.T) {
	engine := remoteTestEngine(t)
	dead := &fakeRemote{expired: true}
	live := &fakeRemote{}
	for _, rd := range []*fakeRemote{dead, live} {
		if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
			t.Fatalf("RegisterRemoteDemand() error = %v", err)
		}
	}

	commitRecords(t, engine, "orders", 1)
	waitFor(t, func() bool { return live.count() >= 1 }, "the live peer was never reached")
	if n := dead.count(); n != 0 {
		t.Fatalf("expired demand was notified %d times, want 0", n)
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
