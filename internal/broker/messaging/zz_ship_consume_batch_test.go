package messaging

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// A scan that fails after it reserved a record ends the batch with that
// record and no error: the HTTP and cluster top-ups throw the slice away
// on an error, which would leave the reserved record invisible until its
// lease lapsed. A batch that reserved nothing reports the failure and
// counts it as a consume error.
func TestConsumeBatchKeepsRecordsReservedBeforeAFailure(t *testing.T) {
	e, dir := zzWP20Engine(t, 4)
	m := metrics.New(prometheus.NewRegistry())
	e.metrics = m
	for p := range 3 {
		if _, err := e.logs.Get("t", p); err != nil {
			t.Fatal(err)
		}
	}
	bad := storage.TopicPartitionDir(dir, "t", 3)
	if err := os.MkdirAll(filepath.Dir(bad), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, nil, 0o644); err != nil { // partition 3 cannot be opened
		t.Fatal(err)
	}
	if _, err := e.logs.Get("t", 3); err == nil {
		t.Fatal("partition 3 opened over a plain file")
	}
	zzWP20Append(t, e, 2, 1)

	start := 0
	msgs, waiter, err := e.ConsumeBatch(context.Background(), "t", ConsumeOpts{ScanStart: &start}, 10, nil)
	if err != nil || waiter != nil || len(msgs) != 1 || msgs[0].Partition != 2 {
		t.Fatalf("ConsumeBatch() = %d msgs, waiter %v, err %v; want partition 2's reserved record and no error", len(msgs), waiter, err)
	}
	if n := testutil.CollectAndCount(m.ErrorsTotal); n != 0 {
		t.Fatalf("a batch that returned its reserved record counted a consume error (%d error series)", n)
	}

	// Nothing reservable now: the same failure must surface.
	msgs, waiter, err = e.ConsumeBatch(context.Background(), "t", ConsumeOpts{ScanStart: &start}, 10, nil)
	if err == nil || waiter != nil || len(msgs) != 0 {
		t.Fatalf("empty ConsumeBatch() = %d msgs, waiter %v, err %v; want the unopenable partition's error", len(msgs), waiter, err)
	}
	if got := testutil.ToFloat64(m.ErrorsTotal.WithLabelValues("messaging", "consume")); got != 1 {
		t.Fatalf("consume errors = %v, want 1", got)
	}
}

// The waiter an empty partition-pinned batch hands back (a ?max=N
// &partition=P long-poll) parks on that partition only: a record on
// another partition is left for other consumers, and the wait is served
// from its own partition.
func TestConsumeBatchPinnedWaiterStaysOnItsPartition(t *testing.T) {
	e := zzWP12Engine(t, 2)
	ctx := context.Background()
	p := 1
	msgs, waiter, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{Partition: &p}, 10, nil)
	if err != nil || len(msgs) != 0 || waiter == nil {
		t.Fatalf("empty pinned ConsumeBatch() = %d msgs, waiter %v, err %v", len(msgs), waiter, err)
	}
	zzWP12Produce(t, e, 0, 1, "other") // reservable, but not on the pinned partition

	out := make(chan topic.Message, 1)
	go func() {
		msg, found, _, err := e.ConsumeWait(ctx, waiter, 5*time.Second, nil)
		if err != nil || !found {
			t.Errorf("ConsumeWait() found=%v err=%v", found, err)
		}
		out <- msg
	}()
	waitFor(t, func() bool {
		if len(out) > 0 {
			return true // served without parking: the check below says by what
		}
		st := e.dispatch.peekState("orders")
		return st != nil && st.hasWaiters.Load()
	}, "the pinned waiter never parked")
	zzWP12Produce(t, e, 1, 1, "pinned")
	if msg := <-out; msg.Partition != 1 {
		t.Fatalf("pinned batch waiter was served partition %d, want 1", msg.Partition)
	}

	// Partition 0's record was never handed out.
	other := 0
	msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{Partition: &other})
	if err != nil || !found || msg.Partition != 0 {
		t.Fatalf("Consume(partition 0) = p%d found=%v err=%v; want its untouched record", msg.Partition, found, err)
	}
}
