package messaging

import (
	"context"
	"testing"
)

// TestZZWP18ConsumeBatchMaxBytes checks the byte bound a forwarded batch
// consume sets: the batch ends once the key and payload bytes it took
// reach MaxBytes, it always takes a first record however large, and the
// records it left stay unreserved for the next consume.
func TestZZWP18ConsumeBatchMaxBytes(t *testing.T) {
	e := zzWP12Engine(t, 1)
	ctx := context.Background()
	zzWP12Produce(t, e, 0, 10, "p0") // key "p0-i" (4 bytes), payload {"tag":"p0","i":i} (17 bytes)
	const recordBytes = 4 + 17

	msgs, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{MaxBytes: 3 * recordBytes}, 10, nil)
	if err != nil || len(msgs) != 3 {
		t.Fatalf("ConsumeBatch(MaxBytes %d) = %d records, %v; want 3", 3*recordBytes, len(msgs), err)
	}
	msgs, _, err = e.ConsumeBatch(ctx, "orders", ConsumeOpts{MaxBytes: 1}, 10, nil)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("ConsumeBatch(MaxBytes 1) = %d records, %v; want the first record alone", len(msgs), err)
	}
	msgs, _, err = e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 10, nil)
	if err != nil || len(msgs) != 6 {
		t.Fatalf("unbounded ConsumeBatch() = %d records, %v; want the 6 left", len(msgs), err)
	}
	if msgs[0].Offset != 4 {
		t.Fatalf("unbounded ConsumeBatch() starts at offset %d, want 4", msgs[0].Offset)
	}
}
