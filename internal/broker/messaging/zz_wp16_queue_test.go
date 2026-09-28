package messaging

import (
	"context"
	"testing"
	"time"
)

func zzWP16Waiter() *waiter {
	return &waiter{cw: &ConsumeWaiter{}, ch: make(chan waiterDelivery, 1)}
}

// zzWP16Drain pops every entry, in order.
func zzWP16Drain(q *entryQueue) []queueEntry {
	var out []queueEntry
	for {
		e, ok := q.pop()
		if !ok {
			return out
		}
		out = append(out, e)
	}
}

// Removing entries anywhere in the FIFO leaves the others in their
// order, and the length counts only what is still queued.
func TestZZWP16QueueRemovalKeepsFIFOOrder(t *testing.T) {
	q := &entryQueue{}
	ws := make([]*waiter, 6)
	for i := range ws {
		ws[i] = zzWP16Waiter()
		q.push(queueEntry{waiter: ws[i]})
	}
	tok := &zzWP16Token{1}
	q.push(queueEntry{remote: tok})
	last := zzWP16Waiter()
	q.push(queueEntry{waiter: last})

	for _, i := range []int{0, 3, 5} {
		if !q.removeWaiter(ws[i]) {
			t.Fatalf("waiter %d not found", i)
		}
	}
	if q.removeWaiter(ws[3]) {
		t.Fatal("a removed waiter was found again")
	}
	if !q.removeRemote(tok) {
		t.Fatal("token not found")
	}
	if q.removeRemote(tok) {
		t.Fatal("a removed token was found again")
	}
	if got := q.len(); got != 4 {
		t.Fatalf("len = %d, want 4", got)
	}
	want := []*waiter{ws[1], ws[2], ws[4], last}
	got := zzWP16Drain(q)
	if len(got) != len(want) {
		t.Fatalf("drained %d entries, want %d", len(got), len(want))
	}
	for i, e := range got {
		if e.waiter != want[i] {
			t.Fatalf("entry %d is not the waiter expected there", i)
		}
	}
	if q.len() != 0 {
		t.Fatalf("len after draining = %d, want 0", q.len())
	}
}

// A peer interest queued twice (it re-registered after its turn) leaves
// once per removal.
func TestZZWP16QueueRemovesOneOfARepeatedToken(t *testing.T) {
	q := &entryQueue{}
	tok := &zzWP16Token{1}
	w := zzWP16Waiter()
	q.push(queueEntry{remote: tok})
	q.push(queueEntry{waiter: w})
	q.push(queueEntry{remote: tok})
	if !q.removeRemote(tok) {
		t.Fatal("first removal found nothing")
	}
	if q.len() != 2 {
		t.Fatalf("len = %d, want 2", q.len())
	}
	if !q.removeRemote(tok) {
		t.Fatal("second removal found nothing")
	}
	if q.removeRemote(tok) {
		t.Fatal("a third removal found a token queued twice")
	}
	got := zzWP16Drain(q)
	if len(got) != 1 || got[0].waiter != w {
		t.Fatalf("left %d entries, want only the waiter", len(got))
	}
}

// A waiter the pump already took off the queue is not found by a
// removal (the consumer then marks itself abandoned), and one the pump
// put back is.
func TestZZWP16QueueRemovalAfterPopAndPushFront(t *testing.T) {
	q := &entryQueue{}
	a, b := zzWP16Waiter(), zzWP16Waiter()
	q.push(queueEntry{waiter: a})
	q.push(queueEntry{waiter: b})
	e, ok := q.pop()
	if !ok || e.waiter != a {
		t.Fatal("pop did not return the head")
	}
	if q.removeWaiter(a) {
		t.Fatal("removal found a waiter the pump holds")
	}
	// Removals while the pump holds a, including one that leaves only
	// removed entries behind, must not break putting it back.
	if !q.removeWaiter(b) {
		t.Fatal("b not found")
	}
	q.pushFront(queueEntry{waiter: a})
	if q.len() != 1 {
		t.Fatalf("len = %d, want 1", q.len())
	}
	if !q.removeWaiter(a) {
		t.Fatal("removal did not find a waiter put back")
	}
	if _, ok := q.peek(); ok {
		t.Fatal("peek found an entry in a queue whose entries were all removed")
	}
}

// Rotation skips removed entries: a removed token never reaches the
// head, and turns keep going round the live ones in order.
func TestZZWP16QueueRotateSkipsRemoved(t *testing.T) {
	q := &entryQueue{}
	toks := []*zzWP16Token{{0}, {1}, {2}, {3}}
	for _, tok := range toks {
		q.push(queueEntry{remote: tok})
	}
	q.removeRemote(toks[0])
	q.removeRemote(toks[2])
	var order []int
	for range 4 {
		e, ok := q.peek()
		if !ok {
			t.Fatal("empty queue")
		}
		order = append(order, e.remote.(*zzWP16Token).id)
		q.rotate()
	}
	if want := []int{1, 3, 1, 3}; !equalInts(order, want) {
		t.Fatalf("turn order = %v, want %v", order, want)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A long run of removals behind a head that never leaves (a waiter
// the pump cannot serve) does not grow the queue's backing array
// without bound.
func TestZZWP16QueueRemovalsDoNotGrowTheBacking(t *testing.T) {
	q := &entryQueue{}
	q.push(queueEntry{waiter: zzWP16Waiter()})
	toks := [2]*zzWP16Token{{0}, {1}}
	q.push(queueEntry{remote: toks[0]})
	for i := range 100_000 {
		q.push(queueEntry{remote: toks[(i+1)%2]})
		if !q.removeRemote(toks[i%2]) {
			t.Fatalf("replacement %d: previous token not found", i)
		}
	}
	if q.len() != 2 {
		t.Fatalf("len = %d, want 2", q.len())
	}
	if n := cap(q.items); n > 64 {
		t.Fatalf("backing array holds %d slots for 2 queued entries", n)
	}
}

// cluster-consume-ack#5: replacing a peer's token removed the old one
// with a scan and copy of the whole FIFO, under the topic lock the pump
// and every local enqueue need, so each re-registration on a topic with
// many parked consumers cost time in proportion to them. It costs the
// same however many are queued.
func TestZZWP16TokenReplacementDoesNotScaleWithTheQueue(t *testing.T) {
	cost := func(waiters int) time.Duration {
		q := zzWP16QueueOf(waiters)
		toks := [2]*zzWP16Token{{0}, {1}}
		q.push(queueEntry{remote: toks[0]})
		const reps = 20_000
		best := time.Duration(1<<63 - 1)
		for range 5 {
			start := time.Now()
			for i := range reps {
				q.push(queueEntry{remote: toks[(i+1)%2]})
				q.removeRemote(toks[i%2])
			}
			best = min(best, time.Since(start))
		}
		return best / reps
	}
	small, large := cost(16), cost(16_384)
	if large > 20*small+time.Microsecond {
		t.Fatalf("replacing a token with 16384 waiters queued took %s, with 16 %s: the removal scales with the queue", large, small)
	}
}

// The dispatcher path the token holder drives: a peer's replaced token
// leaves the topic's FIFO, so a record is offered to the new one, and
// the parked local consumer ahead of both is still served first.
func TestZZWP16ReplacedTokenIsNeverOffered(t *testing.T) {
	engine := remoteTestEngine(t)
	ctx := context.Background()
	prev, next := &fakeRemote{}, &fakeRemote{}
	if err := engine.RegisterRemoteDemand(ctx, "orders", prev); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterRemoteDemand(ctx, "orders", next); err != nil {
		t.Fatal(err)
	}
	engine.DropRemoteDemand("orders", prev)
	st := engine.dispatch.stateFor("orders")
	st.mu.Lock()
	queued := st.queue.len()
	st.mu.Unlock()
	if queued != 1 {
		t.Fatalf("queued = %d after the replacement, want 1", queued)
	}
	commitRecords(t, engine, "orders", 1)
	waitFor(t, func() bool { return next.count() == 1 }, "the new token was never offered the record")
	time.Sleep(50 * time.Millisecond)
	if n := prev.count(); n != 0 {
		t.Fatalf("the replaced token was offered %d records", n)
	}
}
