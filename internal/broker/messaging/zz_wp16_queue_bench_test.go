package messaging

import (
	"context"
	"fmt"
	"testing"
)

// zzWP16Token is a peer token that never expires and never spends:
// only its identity matters to the queue.
type zzWP16Token struct{ id int }

func (*zzWP16Token) Notify(string, func(bool)) bool { return false }
func (*zzWP16Token) Expired() bool                  { return false }

// zzWP16QueueOf returns a FIFO holding waiters local waiters.
func zzWP16QueueOf(waiters int) *entryQueue {
	q := &entryQueue{}
	for range waiters {
		q.push(queueEntry{waiter: &waiter{cw: &ConsumeWaiter{}, ch: make(chan waiterDelivery, 1)}})
	}
	return q
}

// BenchmarkZZWP16ReplaceToken is the owner half of a peer
// re-registering on a topic with local consumers parked: the new token
// joins the FIFO and the peer's previous one leaves it. Both run under
// the topic's lock, which the pump and every local enqueue need.
func BenchmarkZZWP16ReplaceToken(b *testing.B) {
	for _, waiters := range []int{10, 1000} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			q := zzWP16QueueOf(waiters)
			tokens := [2]*zzWP16Token{{0}, {1}}
			q.push(queueEntry{remote: tokens[0]})
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				prev, next := tokens[i%2], tokens[(i+1)%2]
				q.push(queueEntry{remote: next})
				if !q.removeRemote(prev) {
					b.Fatal("previous token not queued")
				}
			}
		})
	}
}

// BenchmarkZZWP16DispatcherReplaceToken is the same replacement through
// the dispatcher's registerRemote and dropRemote, as the token holder
// drives it, with waiters parked on the topic and the pump stopped so
// only the replacement is timed.
func BenchmarkZZWP16DispatcherReplaceToken(b *testing.B) {
	for _, waiters := range []int{10, 1000} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, 1)
			d := e.dispatch
			d.close()
			_, _, cw, err := e.ConsumeProbe(context.Background(), "t", ConsumeOpts{})
			if err != nil || cw == nil {
				b.Fatalf("probe: waiter %v err %v", cw, err)
			}
			for range waiters {
				d.enqueue("t", &waiter{cw: cw, ch: make(chan waiterDelivery, 1)})
			}
			scan := []int{0}
			tokens := [2]*zzWP16Token{{0}, {1}}
			d.registerRemote("t", scan, tokens[0])
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				prev, next := tokens[i%2], tokens[(i+1)%2]
				d.registerRemote("t", scan, next)
				d.dropRemote("t", prev)
			}
		})
	}
}

// BenchmarkZZWP16QueuePushPop is the pump's side of the FIFO in steady
// state: a consumer parks at the back and the pump takes the head.
func BenchmarkZZWP16QueuePushPop(b *testing.B) {
	for _, waiters := range []int{10, 1000} {
		b.Run(fmt.Sprintf("waiters=%d", waiters), func(b *testing.B) {
			q := zzWP16QueueOf(waiters)
			w := &waiter{cw: &ConsumeWaiter{}, ch: make(chan waiterDelivery, 1)}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				q.push(queueEntry{waiter: w})
				e, ok := q.peek()
				if !ok || e.waiter == nil {
					b.Fatal("empty queue")
				}
				q.pop()
			}
		})
	}
}

// BenchmarkZZWP16QueueRotate is the pump offering a record to the peer
// at the head: its token moves to the back.
func BenchmarkZZWP16QueueRotate(b *testing.B) {
	q := &entryQueue{}
	for i := range 8 {
		q.push(queueEntry{remote: &zzWP16Token{i}})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, ok := q.peek(); !ok {
			b.Fatal("empty queue")
		}
		q.rotate()
	}
}

// BenchmarkZZWP16PumpDeliver is one pump pass that hands a record to a
// parked consumer: the waiter is queued, the pump pops it, reserves and
// reads the record and sends it over; with others parked behind it, the
// pass also pops the next one, finds nothing and puts it back. The
// record is released after each delivery so the next one takes it
// again.
func BenchmarkZZWP16PumpDeliver(b *testing.B) {
	for _, parked := range []int{0, 100} {
		b.Run(fmt.Sprintf("parked=%d", parked), func(b *testing.B) {
			e := zzWP7aBenchEngine(b, 1)
			d := e.dispatch
			d.close()
			ctx := context.Background()
			_, _, cw, err := e.ConsumeProbe(ctx, "t", ConsumeOpts{})
			if err != nil || cw == nil {
				b.Fatalf("probe: waiter %v err %v", cw, err)
			}
			if _, err := e.CommitAcceptedProduceBatch(ctx, zzWP7aBatch(1, "t-id")); err != nil {
				b.Fatal(err)
			}
			for range parked {
				d.enqueue("t", &waiter{cw: cw, ch: make(chan waiterDelivery, 1)})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				w := &waiter{cw: cw, ch: make(chan waiterDelivery, 1)}
				st := d.lockLiveState("t")
				w.st = st
				st.queue.pushFront(queueEntry{waiter: w})
				st.hasWaiters.Store(true)
				st.mu.Unlock()
				d.pumpTopic("t")
				select {
				case dl := <-w.ch:
					e.releaseUndelivered("t", dl.msg)
				default:
					b.Fatal("the pump did not hand the record over")
				}
			}
		})
	}
}
