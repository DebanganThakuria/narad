package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
)

// zzWP20DeliveriesAtRate is an RPC server whose clock advances by one
// step per remembered delivery, filled to steady state: a grace's worth
// of records (live of them) are remembered, so every further delivery
// expires about one. It returns the server and a function that
// remembers the next delivery.
func zzWP20DeliveriesAtRate(live int) (*RPCServer, func()) {
	step := deliveryCancelGrace / time.Duration(live)
	now := time.Unix(1_700_000_000, 0)
	s := &RPCServer{now: func() time.Time { return now }}
	handle := consumer.Handle{Partition: 2, Offset: 41, Nonce: 777}
	var request uint64
	next := func() {
		now = now.Add(step)
		request++
		s.rememberDelivery(requestKey{stream: 1, request: request}, "orders", handle)
	}
	for range live + 1 {
		next()
	}
	return s, next
}

// zzWP20PerDelivery is the best of five runs of n deliveries at steady
// state with live records inside the grace, per delivery.
func zzWP20PerDelivery(live, n int) time.Duration {
	_, next := zzWP20DeliveriesAtRate(live)
	best := time.Duration(1<<63 - 1)
	for range 5 {
		start := time.Now()
		for range n {
			next()
		}
		best = min(best, time.Since(start)/time.Duration(n))
	}
	return best
}

// TestZZWP20DeliveryExpiryCostIsFlat checks that remembering a forwarded
// delivery costs about the same however many records the cancel grace
// holds. Expiring the oldest record used to shift the whole expiry
// queue down by one, on every delivery once the queue was full, under
// the mutex every forwarded consume takes: at 50k deliveries a second
// (100k records in the grace) that was 57 us per delivery. Timing both
// sizes in one run and comparing them keeps the check independent of
// how fast the machine is.
func TestZZWP20DeliveryExpiryCostIsFlat(t *testing.T) {
	const n = 4096
	small := zzWP20PerDelivery(1<<10, n)
	large := zzWP20PerDelivery(1<<16, n)
	t.Logf("per delivery: %v with 1Ki records in the grace, %v with 64Ki", small, large)
	if large > 20*small {
		t.Fatalf("a delivery costs %v with 64Ki records in the grace and %v with 1Ki: it grows with the queue", large, small)
	}
}

// TestZZWP20DeliveryExpiryQueueStaysBounded checks the expiry queue's
// backing array does not grow without bound when entries are expired
// from its front: it holds at most about twice the live records.
func TestZZWP20DeliveryExpiryQueueStaysBounded(t *testing.T) {
	const live = 1000
	s, next := zzWP20DeliveriesAtRate(live)
	for range 20 * live {
		next()
	}
	if got := len(s.deliveries); got < live-deliveryExpiryBudget || got > live+deliveryExpiryBudget {
		t.Fatalf("%d records remembered at steady state, want about %d", got, live)
	}
	if c := cap(s.deliveryExpiry); c > 4*live {
		t.Fatalf("expiry queue capacity %d with %d live records, want it bounded", c, live)
	}
}

// BenchmarkZZWP20RememberDeliveryAtRate is the owner's bookkeeping for
// one forwarded delivery at steady state, at 1k, 10k and 50k deliveries
// a second (2k, 20k and 100k records inside the 2 s cancel grace).
func BenchmarkZZWP20RememberDeliveryAtRate(b *testing.B) {
	for _, rate := range []int{1_000, 10_000, 50_000} {
		b.Run(fmt.Sprintf("rate=%dk", rate/1000), func(b *testing.B) {
			_, next := zzWP20DeliveriesAtRate(int(deliveryCancelGrace.Seconds() * float64(rate)))
			b.ReportAllocs()
			for b.Loop() {
				next()
			}
		})
	}
}
