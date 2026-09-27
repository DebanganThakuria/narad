package messaging

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// zzWP12Engine is an engine over one topic "orders" with parts
// partitions, all owned here; the in-flight cap is newTestEngine's 10
// per partition.
func zzWP12Engine(t *testing.T, parts int) *Engine {
	t.Helper()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: parts, VisibilityTimeoutMs: 30_000}
	return newTestEngine(t, ms, nil, nil)
}

func zzWP12Produce(t *testing.T, e *Engine, partition, n int, tag string) {
	t.Helper()
	if _, err := e.CommitAcceptedProduceBatch(context.Background(), zzWP7aRecords("orders", "", partition, n, tag)); err != nil {
		t.Fatal(err)
	}
}

// A batch reserves up to max records in one scan, each with a receipt
// handle of its own that acks on its own, and takes one partition's
// backlog in offset order before moving on.
func TestZZWP12ConsumeBatchReservesEachRecord(t *testing.T) {
	e := zzWP12Engine(t, 2)
	ctx := context.Background()
	zzWP12Produce(t, e, 0, 4, "p0")
	zzWP12Produce(t, e, 1, 3, "p1")

	start := 0
	msgs, waiter, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{ScanStart: &start}, 5, nil)
	if err != nil || waiter != nil {
		t.Fatalf("ConsumeBatch() = waiter %v, err %v", waiter, err)
	}
	if len(msgs) != 5 {
		t.Fatalf("ConsumeBatch() returned %d records, want 5", len(msgs))
	}
	for i := range 4 {
		if msgs[i].Partition != 0 || msgs[i].Offset != int64(i) {
			t.Fatalf("record %d = p%d/%d, want partition 0's backlog in order", i, msgs[i].Partition, msgs[i].Offset)
		}
	}
	if msgs[4].Partition != 1 || msgs[4].Offset != 0 {
		t.Fatalf("record 4 = p%d/%d, want p1/0 once partition 0 ran dry", msgs[4].Partition, msgs[4].Offset)
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		if m.ReceiptHandle == "" || seen[m.ReceiptHandle] {
			t.Fatalf("receipt handle %q missing or repeated", m.ReceiptHandle)
		}
		seen[m.ReceiptHandle] = true
	}

	// The rest: two records left, a batch of ten takes both.
	rest, waiter, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 10, nil)
	if err != nil || waiter != nil || len(rest) != 2 {
		t.Fatalf("second ConsumeBatch() = %d records, waiter %v, err %v; want the 2 left", len(rest), waiter, err)
	}
	for _, m := range append(msgs, rest...) {
		if err := e.Ack(ctx, "orders", decodeHandleForTest(t, m.ReceiptHandle)); err != nil {
			t.Fatalf("Ack(%s) error = %v", m.ReceiptHandle, err)
		}
	}
	if more, waiter, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 10, nil); err != nil || len(more) != 0 || waiter == nil {
		t.Fatalf("drained ConsumeBatch() = %d records, waiter %v, err %v; want none and a waiter", len(more), waiter, err)
	}
}

// Every record of a batch counts against its partition's in-flight cap,
// exactly as that many single consumes would.
func TestZZWP12ConsumeBatchHonoursInFlightCap(t *testing.T) {
	e := zzWP12Engine(t, 1)
	ctx := context.Background()
	zzWP12Produce(t, e, 0, 25, "cap")
	msgs, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 25, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 10 {
		t.Fatalf("ConsumeBatch() reserved %d records, want the in-flight cap of 10", len(msgs))
	}
	if more, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 25, nil); err != nil || len(more) != 0 {
		t.Fatalf("ConsumeBatch() at the cap = %d records, err %v; want none", len(more), err)
	}
	if err := e.Ack(ctx, "orders", decodeHandleForTest(t, msgs[0].ReceiptHandle)); err != nil {
		t.Fatal(err)
	}
	if more, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 25, nil); err != nil || len(more) != 1 || more[0].Offset != 10 {
		t.Fatalf("ConsumeBatch() after one ack = %+v, err %v; want offset 10 alone", more, err)
	}
}

// A partition-pinned batch reads that partition only.
func TestZZWP12ConsumeBatchPinned(t *testing.T) {
	e := zzWP12Engine(t, 3)
	ctx := context.Background()
	zzWP12Produce(t, e, 0, 3, "p0")
	zzWP12Produce(t, e, 2, 3, "p2")
	p := 2
	msgs, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{Partition: &p}, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("pinned ConsumeBatch() = %d records, want 3", len(msgs))
	}
	for _, m := range msgs {
		if m.Partition != 2 {
			t.Fatalf("pinned batch returned a record from partition %d", m.Partition)
		}
	}
	bad := 7
	if _, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{Partition: &bad}, 10, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("out-of-range partition: err = %v, want ErrInvalid", err)
	}
}

// Nothing reservable: no records and a waiter that parks exactly like a
// probe's, so a record produced while it waits is delivered.
func TestZZWP12ConsumeBatchEmptyReturnsWaiter(t *testing.T) {
	e := zzWP12Engine(t, 2)
	ctx := context.Background()
	msgs, waiter, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 10, nil)
	if err != nil || len(msgs) != 0 || waiter == nil {
		t.Fatalf("empty ConsumeBatch() = %d records, waiter %v, err %v", len(msgs), waiter, err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		zzWP12Produce(t, e, 1, 1, "late")
	}()
	msg, found, _, err := e.ConsumeWait(ctx, waiter, 5*time.Second, nil)
	if err != nil || !found || msg.Partition != 1 {
		t.Fatalf("ConsumeWait() = %+v found %v err %v; want the late record", msg, found, err)
	}
}

func TestZZWP12ConsumeBatchRejectsBadArguments(t *testing.T) {
	e := zzWP12Engine(t, 1)
	ctx := context.Background()
	p, off := 0, int64(0)
	for name, call := range map[string]func() error{
		"zero max": func() error { _, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 0, nil); return err },
		"replay": func() error {
			_, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{Partition: &p, Offset: &off}, 5, nil)
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
	if _, _, err := e.ConsumeBatch(ctx, "missing", ConsumeOpts{}, 5, nil); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("unknown topic: err = %v, want ErrTopicNotFound", err)
	}
}

// Batches taken concurrently never hand one record to two consumers.
func TestZZWP12ConsumeBatchConcurrentNoDuplicates(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 4, VisibilityTimeoutMs: 30_000}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()
	const perPartition = 200
	for p := range 4 {
		zzWP12Produce(t, e, p, perPartition, fmt.Sprint("p", p))
	}
	type result struct {
		msgs []topic.Message
		err  error
	}
	results := make(chan result, 8)
	for range 8 {
		go func() {
			var got []topic.Message
			for {
				msgs, _, err := e.ConsumeBatch(ctx, "orders", ConsumeOpts{}, 7, nil)
				if err != nil {
					results <- result{err: err}
					return
				}
				if len(msgs) == 0 {
					results <- result{msgs: got}
					return
				}
				for _, m := range msgs {
					if err := e.Ack(ctx, "orders", decodeHandleForTest(t, m.ReceiptHandle)); err != nil && !errors.Is(err, consumer.ErrHandleStale) {
						results <- result{err: err}
						return
					}
				}
				got = append(got, msgs...)
			}
		}()
	}
	seen := map[string]bool{}
	for range 8 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		for _, m := range r.msgs {
			key := fmt.Sprintf("%d/%d", m.Partition, m.Offset)
			if seen[key] {
				t.Fatalf("record %s delivered twice", key)
			}
			seen[key] = true
		}
	}
	if len(seen) != 4*perPartition {
		t.Fatalf("delivered %d distinct records, want %d", len(seen), 4*perPartition)
	}
}
