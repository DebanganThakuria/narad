package main

import "testing"

// A crash can make the broker store an accepted message twice. Two
// consumers may then lease and ack their own copy at the same time, and
// both acks answer 204: that is the documented at-least-once duplicate,
// not a double lease. Two 204s for the same stored copy are.
func TestDoubleLeaseNeedsTheSameStoredCopy(t *testing.T) {
	y := messageCopy{topic: "orders", partition: 3, offset: 842}
	cases := []struct {
		name    string
		x       messageCopy
		further bool
		double  bool
	}{
		{"another stored copy", messageCopy{topic: "orders", partition: 3, offset: 851}, true, false},
		{"a copy on another partition", messageCopy{topic: "orders", partition: 7, offset: 12}, true, false},
		{"the same stored copy", y, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &inFlightRecord{}
			// Both deliveries return before either ack is confirmed.
			if rec.noteCopy(y) {
				t.Fatal("the first copy delivered counted as a further copy")
			}
			if got := rec.noteCopy(tc.x); got != tc.further {
				t.Fatalf("noteCopy(%+v) = %v, want %v", tc.x, got, tc.further)
			}
			yAckedAtDelivery, xAckedAtDelivery := rec.acked.Load(), rec.acked.Load()

			// Y's ack is confirmed first, then X's.
			yAgain := rec.confirmCopy(y)
			yFirst := rec.acked.CompareAndSwap(false, true)
			if !yFirst || doubleLease(yAckedAtDelivery, yFirst, yAgain) {
				t.Fatal("the first confirmed ack was counted as a double lease")
			}
			xAgain := rec.confirmCopy(tc.x)
			xFirst := rec.acked.CompareAndSwap(false, true)
			if got := doubleLease(xAckedAtDelivery, xFirst, xAgain); got != tc.double {
				t.Fatalf("second 204 (copy %+v after %+v): double lease = %v, want %v", tc.x, y, got, tc.double)
			}
		})
	}
}

// A redelivery of a copy after its ack was confirmed is acked again and
// answers 204. That is at-least-once (counted as dupAfterAck), not a
// double lease.
func TestRedeliveryAfterAckIsNotADoubleLease(t *testing.T) {
	rec := &inFlightRecord{}
	c := messageCopy{topic: "orders", partition: 0, offset: 5}
	rec.noteCopy(c)
	rec.confirmCopy(c)
	rec.acked.Store(true)

	rec.noteCopy(c)
	ackedAtDelivery := rec.acked.Load()
	again := rec.confirmCopy(c)
	first := rec.acked.CompareAndSwap(false, true)
	if doubleLease(ackedAtDelivery, first, again) {
		t.Fatal("a redelivery after a confirmed ack was counted as a double lease")
	}
}
