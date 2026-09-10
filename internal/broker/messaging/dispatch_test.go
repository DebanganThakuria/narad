package messaging

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// parkedResult is what a parked consumer came back with, for tests that
// need the message itself and not just whether one arrived.
type parkedResult struct {
	msg   topic.Message
	found bool
}

// parkForMessage is parkConsumer where the caller needs the delivered
// message, typically so it can ack it.
func parkForMessage(t *testing.T, e *Engine, topicName string, wait time.Duration) <-chan parkedResult {
	t.Helper()
	_, found, w, err := e.ConsumeProbe(context.Background(), topicName, ConsumeOpts{})
	if err != nil || found {
		t.Fatalf("ConsumeProbe() = (found %v, err %v), want a waiter", found, err)
	}
	out := make(chan parkedResult, 1)
	ready := make(chan struct{})
	go func() {
		close(ready)
		msg, got, _, err := e.ConsumeWait(context.Background(), w, wait, nil)
		if err != nil {
			t.Errorf("ConsumeWait() error = %v", err)
		}
		out <- parkedResult{msg: msg, found: got}
	}()
	<-ready
	return out
}

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
		_, got, _, err := e.ConsumeWait(context.Background(), w, wait, nil)
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
	wg.Go(func() {
		if _, got, _, err := engine.ConsumeWait(ctx, w, 5*time.Second, nil); err != nil || got {
			t.Errorf("ConsumeWait() = (found %v, err %v), want empty after cancellation", got, err)
		}
	})

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

// TestGivingUpMidReservationStrandsNothing targets the window the test
// above almost never lands in. Between the pump taking a waiter off the
// queue and the delivery reaching its channel, the waiter is in neither
// place, so a consumer that gives up right then cannot see the record it
// is about to be handed: it reports "nothing arrived" and releases
// nothing, while the pump hands a reserved record to a channel nobody
// will ever read.
//
// Each round times a commit to land on a consumer's expiry, so some
// rounds fall inside that window. The visibility timeout is 60s, which
// is what makes a strand fatal rather than merely slow: a record left
// reserved for a departed consumer cannot come back on its own, so one
// occurrence in the whole run fails the test. In production the same bug
// is a treadmill, every strand costing a full visibility timeout, which
// reads as a stall rather than as loss.
func TestGivingUpMidReservationStrandsNothing(t *testing.T) {
	const rounds = 300
	const wait = 20 * time.Millisecond

	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{
		Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000,
		MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
	}
	engine := newTestEngine(t, ms, nil, nil)

	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// Every record this test takes is acked, so reservations cannot pile
	// up against the in-flight cap and make a later round look stranded.
	ack := func(round int, msg topic.Message) {
		t.Helper()
		h, err := consumer.DecodeHandle(msg.ReceiptHandle)
		if err != nil {
			t.Fatalf("round %d: DecodeHandle() error = %v", round, err)
		}
		if err := engine.Ack(context.Background(), "orders", h); err != nil {
			t.Fatalf("round %d: Ack() error = %v", round, err)
		}
	}

	committed := 0
	served, recovered := 0, 0
	for round := range rounds {
		out := parkForMessage(t, engine, "orders", wait)

		// Aim the commit at the moment the consumer's budget runs out,
		// sweeping either side of it so the abandonment lands before,
		// during and after the pump's reservation across the run.
		time.Sleep(wait - time.Millisecond + time.Duration(round%20)*100*time.Microsecond)
		if _, err := log.Append(storage.EncodeKeyedRecord("", 1, []byte(`{"id":1}`))); err != nil {
			t.Fatalf("Append(%d) error = %v", round, err)
		}
		committed++
		if err := log.AdvanceHighWatermark(int64(committed)); err != nil {
			t.Fatalf("AdvanceHighWatermark(%d) error = %v", committed, err)
		}

		select {
		case got := <-out:
			if got.found {
				ack(round, got.msg)
				served++
				continue
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: the parked consumer never returned", round)
		}

		// The consumer left empty-handed, so the record it did not get
		// must come back for the next consumer. The give-back runs on the
		// pump, so allow a moment for it; two seconds against a sixty
		// second visibility timeout keeps the assertion sharp, since a
		// record that really was stranded cannot reappear inside it.
		deadline := time.Now().Add(2 * time.Second)
		for {
			msg, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
			if err != nil {
				t.Fatalf("round %d: ConsumeProbe() error = %v", round, err)
			}
			if found {
				ack(round, msg)
				recovered++
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("round %d: a record was reserved for a consumer that had already given up, "+
					"and is now invisible until its visibility timeout (%d served, %d recovered of %d committed)",
					round, served, recovered, committed)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}

	if served+recovered != committed {
		t.Fatalf("accounted for %d records (%d served, %d recovered) of %d committed",
			served+recovered, served, recovered, committed)
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

// TestRemoteClaimReleasesHoldAtOnce pins the fix for the one-second floor
// on cross-node delivery: a notification a peer said it would claim held
// its record until claimDeadline even after the claim had been served, and
// that hold gated the pump for every later record on the topic. The
// claim's arrival must release it, so a second record produced right
// after the first is offered immediately.
func TestRemoteClaimReleasesHoldAtOnce(t *testing.T) {
	engine := remoteTestEngine(t)
	rd := &fakeRemote{}
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}
	commitRecords(t, engine, "orders", 1)
	waitFor(t, func() bool { return rd.count() == 1 }, "the peer was never notified")

	// The peer claims: a local-only consume reserves the record, and the
	// cluster layer reports the claim's arrival.
	if _, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{}); err != nil || !found {
		t.Fatalf("claim: found=%v err=%v", found, err)
	}
	engine.NoteRemoteClaim("orders")

	// The peer re-registers (its token was single use) and a second
	// record lands. Before the fix this notification waited out the
	// first one's claimDeadline.
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}
	// commitRecords sets the high-watermark to n rather than advancing
	// it, so asking for 2 here appends two more records and makes offset
	// 1 the one newly visible record.
	start := time.Now()
	commitRecords(t, engine, "orders", 2)
	waitFor(t, func() bool { return rd.count() == 2 }, "the second record was never offered")
	if elapsed := time.Since(start); elapsed > claimDeadline/2 {
		t.Fatalf("second notification took %s: the served claim's hold was not released", elapsed)
	}
}

// TestConsumableExcludesRecordsAlreadyHandedOut pins the estimate the
// pump gates on: a record reserved by a consumer (in flight, not yet
// acked) is not free, so a peer registering afterwards must not be
// offered it. Before the fix the estimate used the ack frontier and the
// spurious offer cost the peer an empty claim and the topic a
// claimDeadline of silence.
func TestConsumableExcludesRecordsAlreadyHandedOut(t *testing.T) {
	engine := remoteTestEngine(t)
	commitRecords(t, engine, "orders", 1)
	if _, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{}); err != nil || !found {
		t.Fatalf("local reserve: found=%v err=%v", found, err)
	}
	rd := &fakeRemote{}
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}
	st := engine.dispatch.stateFor("orders")
	if n := engine.dispatch.consumable("orders", st.scan); n != 0 {
		t.Fatalf("consumable = %d, want 0 with the only record in flight", n)
	}
	time.Sleep(150 * time.Millisecond)
	if n := rd.count(); n != 0 {
		t.Fatalf("peer was offered %d records that were already in flight", n)
	}
}

// TestConsumableExcludesAckedAheadRecords covers the other term: a record
// acked ahead of a gap sits above the frontier too, and is not free.
func TestConsumableExcludesAckedAheadRecords(t *testing.T) {
	engine := remoteTestEngine(t)
	commitRecords(t, engine, "orders", 2)
	if _, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{}); err != nil || !found {
		t.Fatalf("reserve offset 0: found=%v err=%v", found, err)
	}
	second, found, _, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
	if err != nil || !found {
		t.Fatalf("reserve offset 1: found=%v err=%v", found, err)
	}
	h, err := consumer.DecodeHandle(second.ReceiptHandle)
	if err != nil {
		t.Fatalf("DecodeHandle() error = %v", err)
	}
	if err := engine.Ack(context.Background(), "orders", h); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
	// Offset 0 in flight, offset 1 acked ahead of it: nothing is free.
	rd := &fakeRemote{}
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}
	st := engine.dispatch.stateFor("orders")
	if n := engine.dispatch.consumable("orders", st.scan); n != 0 {
		t.Fatalf("consumable = %d, want 0 (1 in flight + 1 acked ahead)", n)
	}
}

// TestReleaseTopicWaitersWakesParkedConsumers pins the delete path: a
// consumer parked on a topic returns empty at once when the topic's
// waiters are released, rather than at the end of its wait.
func TestReleaseTopicWaitersWakesParkedConsumers(t *testing.T) {
	engine := remoteTestEngine(t)
	_, _, w, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if w == nil {
		t.Fatal("expected a waiter for an empty topic")
	}
	type res struct {
		found bool
		took  time.Duration
	}
	done := make(chan res, 1)
	go func() {
		start := time.Now()
		_, found, _, _ := engine.ConsumeWait(context.Background(), w, 10*time.Second, nil)
		done <- res{found: found, took: time.Since(start)}
	}()
	waitFor(t, func() bool { return engine.dispatch.stateFor("orders").hasWaiters.Load() }, "the consumer never parked")
	engine.ReleaseTopicWaiters("orders")
	select {
	case r := <-done:
		if r.found {
			t.Fatal("a released waiter was handed a record")
		}
		if r.took > 2*time.Second {
			t.Fatalf("the parked consumer took %s to return after the release", r.took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the parked consumer did not return after ReleaseTopicWaiters")
	}
}

// holdState reads a topic's hold bookkeeping under its lock.
func holdState(e *Engine, topicName string) (outstanding, holds int) {
	st := e.dispatch.stateFor(topicName)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.outstanding, len(st.holds)
}

// setOutstanding seeds a topic's in-flight notification count so the
// hold paths can be driven without a pump round trip.
func setOutstanding(e *Engine, topicName string, n int) {
	st := e.dispatch.stateFor(topicName)
	st.mu.Lock()
	st.outstanding = n
	st.mu.Unlock()
}

// addHold records a notification the way the pump does right before it
// leaves: outstanding is already counted, the hold waits for a claim or
// its deadline.
func addHold(e *Engine, topicName string) *claimHold {
	st := e.dispatch.stateFor(topicName)
	st.mu.Lock()
	defer st.mu.Unlock()
	return e.dispatch.addHoldLocked(topicName, st)
}

// TestHoldExpiresWhenNoClaimArrives pins the deadline path: a peer that
// said it would claim and never came has its hold given back at
// claimDeadline, so the record it was promised is free for the next
// peer rather than held forever.
func TestHoldExpiresWhenNoClaimArrives(t *testing.T) {
	engine := remoteTestEngine(t)
	setOutstanding(engine, "orders", 1)
	addHold(engine, "orders")
	if out, holds := holdState(engine, "orders"); out != 1 || holds != 1 {
		t.Fatalf("after a notification outstanding=%d holds=%d, want 1/1 (the record stays held until the deadline)", out, holds)
	}
	waitFor(t, func() bool {
		out, holds := holdState(engine, "orders")
		return out == 0 && holds == 0
	}, "the hold never expired at claimDeadline")
}

// TestExpiredHoldAlreadyClaimedIsSkipped pins exactly-once retirement: a
// hold that a claim already retired is no longer in the list, so the
// deadline callback firing afterwards must not retire a second
// notification, which would let the pump promise a record still held for
// another peer.
func TestExpiredHoldAlreadyClaimedIsSkipped(t *testing.T) {
	engine := remoteTestEngine(t)
	d := engine.dispatch
	setOutstanding(engine, "orders", 2)
	h := addHold(engine, "orders")

	d.claimArrived("orders")
	if out, holds := holdState(engine, "orders"); out != 1 || holds != 0 {
		t.Fatalf("after the claim outstanding=%d holds=%d, want 1/0", out, holds)
	}
	// The deadline fires for the hold the claim already retired.
	d.expireHold("orders", h)
	if out, holds := holdState(engine, "orders"); out != 1 || holds != 0 {
		t.Fatalf("after the stale deadline outstanding=%d holds=%d, want 1/0 (a hold is retired exactly once)", out, holds)
	}
}

// TestClaimArrivedWithoutHoldsIsNoOp pins the probe case: a local-only
// consume that answers no notification (a probe, or a claim for a
// hold that already expired) finds no hold and must not retire a
// notification that belongs to some other peer.
func TestClaimArrivedWithoutHoldsIsNoOp(t *testing.T) {
	engine := remoteTestEngine(t)
	setOutstanding(engine, "orders", 1)
	engine.NoteRemoteClaim("orders")
	if out, holds := holdState(engine, "orders"); out != 1 || holds != 0 {
		t.Fatalf("outstanding=%d holds=%d after a claim with no hold, want 1/0 untouched", out, holds)
	}
}

// TestReleaseTopicUnknownTopicIsNoOp pins the delete of a topic this node
// never served a long poll for: there is no state to drop, and the
// release must not create any (stateFor would), so a same-name recreate
// still starts from nothing.
func TestReleaseTopicUnknownTopicIsNoOp(t *testing.T) {
	engine := remoteTestEngine(t)
	engine.ReleaseTopicWaiters("never-seen")
	engine.dispatch.mu.RLock()
	_, ok := engine.dispatch.topics["never-seen"]
	engine.dispatch.mu.RUnlock()
	if ok {
		t.Fatal("releasing an unknown topic created dispatch state for it")
	}
}

// TestReleaseTopicDropsPeersAndCancelsHolds pins the peer half of the
// delete path: a peer's standing token leaves the queue, the holds its
// claims were promised are cancelled, and the topic's state is reset
// outright so a recreate under the same name starts clean.
func TestReleaseTopicDropsPeersAndCancelsHolds(t *testing.T) {
	engine := remoteTestEngine(t)
	d := engine.dispatch
	rd := &fakeRemote{refuse: true} // never notified, so the token stays queued
	if err := engine.RegisterRemoteDemand(context.Background(), "orders", rd); err != nil {
		t.Fatalf("RegisterRemoteDemand() error = %v", err)
	}
	setOutstanding(engine, "orders", 1)
	addHold(engine, "orders")
	old := d.stateFor("orders")
	old.mu.Lock()
	queued, holds := old.queue.len(), len(old.holds)
	old.mu.Unlock()
	if queued != 1 || holds != 1 {
		t.Fatalf("precondition: queued=%d holds=%d, want 1/1", queued, holds)
	}

	engine.ReleaseTopicWaiters("orders")

	old.mu.Lock()
	queued, holds, outstanding, has := old.queue.len(), len(old.holds), old.outstanding, old.hasWaiters.Load()
	old.mu.Unlock()
	if queued != 0 || holds != 0 || outstanding != 0 || has {
		t.Fatalf("after release queued=%d holds=%d outstanding=%d hasWaiters=%v, want all cleared", queued, holds, outstanding, has)
	}
	// The state stays in the map (open logs' wake notifiers point at it)
	// and is reused by a same-name recreate, with its generation bumped
	// so a pump mid-read notices the release.
	if fresh := d.stateFor("orders"); fresh != old {
		t.Fatal("a release replaced the dispatch state instead of resetting it in place")
	}
	old.mu.Lock()
	gen := old.gen
	old.mu.Unlock()
	if gen != 1 {
		t.Fatalf("release generation = %d, want 1", gen)
	}
}

// TestReleasedWaiterNeverYieldsAPhantomRecord pins dequeue's closed-channel
// handling: a consumer whose topic was released while its wait was ending
// must come back empty, never with the zero message a closed channel
// would otherwise hand it.
func TestReleasedWaiterNeverYieldsAPhantomRecord(t *testing.T) {
	engine := remoteTestEngine(t)
	for range 20 {
		_, _, w, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
		if err != nil || w == nil {
			t.Fatalf("ConsumeProbe: waiter=%v err=%v", w, err)
		}
		type res struct {
			found bool
			msg   topic.Message
		}
		done := make(chan res, 1)
		go func() {
			msg, found, _, _ := engine.ConsumeWait(context.Background(), w, 5*time.Millisecond, nil)
			done <- res{found: found, msg: msg}
		}()
		// Release right around the wait's end so the timer and the close
		// race inside the select.
		time.Sleep(4 * time.Millisecond)
		engine.ReleaseTopicWaiters("orders")
		r := <-done
		if r.found {
			t.Fatalf("a released waiter reported found=true with %+v", r.msg)
		}
	}
}

// TestParkedConsumerWakesAfterATopicRelease pins the in-place reset: the
// wake notifier an open log captured at open time must keep working
// after the topic's waiters were released (a delete-and-recreate opens
// the new log before the old state is retired), so a consumer parked
// afterwards is still woken by a produce.
func TestParkedConsumerWakesAfterATopicRelease(t *testing.T) {
	engine := remoteTestEngine(t)
	if _, err := engine.logs.Get("orders", 0); err != nil { // installs the notifier
		t.Fatalf("Get() error = %v", err)
	}
	engine.ReleaseTopicWaiters("orders")

	_, _, w, err := engine.ConsumeProbe(context.Background(), "orders", ConsumeOpts{})
	if err != nil || w == nil {
		t.Fatalf("ConsumeProbe: waiter=%v err=%v", w, err)
	}
	done := make(chan bool, 1)
	go func() {
		_, found, _, _ := engine.ConsumeWait(context.Background(), w, 5*time.Second, nil)
		done <- found
	}()
	waitFor(t, func() bool { return engine.dispatch.stateFor("orders").hasWaiters.Load() }, "the consumer never parked")
	commitRecords(t, engine, "orders", 1)
	select {
	case found := <-done:
		if !found {
			t.Fatal("the parked consumer was not handed the record after a release")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the parked consumer was never woken after a release: the log's wake notifier points at dead state")
	}
}
