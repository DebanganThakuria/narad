package messaging

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

type longPollResult struct {
	msg     topic.Message
	found   bool
	err     error
	elapsed time.Duration
}

// TestLongPollBroadcastWakesAllWaitersOnOneCommit pins the broadcast
// wake-up: one commit that makes N records visible must wake ALL N
// blocked long-pollers, not hand a single token to one of them.
func TestLongPollBroadcastWakesAllWaitersOnOneCommit(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// Warm the engine's topic cache: the test fake metastore counts
	// GetTopic calls without synchronization, so concurrent cold-cache
	// loads would trip the race detector on test bookkeeping.
	if _, err := engine.getTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("getTopic() warm-up error = %v", err)
	}

	const waiters = 2
	results := make(chan longPollResult, waiters)
	for range waiters {
		go func() {
			start := time.Now()
			msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 10 * time.Second})
			results <- longPollResult{msg: msg, found: found, err: err, elapsed: time.Since(start)}
		}()
	}
	// Let both waiters park in waitForActivity before the commit.
	time.Sleep(100 * time.Millisecond)

	// One commit making two records visible: a single HWM advance.
	if _, _, err := log.AppendBatch([][]byte{[]byte(`{"id":1}`), []byte(`{"id":2}`)}); err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(2); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}

	seen := map[int64]bool{}
	for i := range waiters {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("waiter %d: Consume() error = %v", i, r.err)
			}
			if !r.found {
				t.Fatalf("waiter %d: Consume() found = false, want true", i)
			}
			if r.elapsed > 5*time.Second {
				t.Fatalf("waiter %d: Consume() took %v — waiter slept through the commit", i, r.elapsed)
			}
			if seen[r.msg.Offset] {
				t.Fatalf("waiter %d: offset %d delivered twice", i, r.msg.Offset)
			}
			seen[r.msg.Offset] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d never woke: one commit woke only %d of %d long-pollers", i, i, waiters)
		}
	}
	if !seen[0] || !seen[1] {
		t.Fatalf("delivered offsets = %v, want {0, 1}", seen)
	}
}

// TestLongPollWakesOnVisibilityTimeoutExpiry pins the expiry wake-up: a
// long-poller blocked while the only message is in-flight must be woken
// as soon as the reservation's visibility timeout expires (via the
// purger's release notifier), well before its own Wait deadline.
func TestLongPollWakesOnVisibilityTimeoutExpiry(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 200}
	engine := newTestEngine(t, ms, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}

	// Reserve the only message and never ack it.
	first, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0})
	if err != nil {
		t.Fatalf("first Consume() error = %v", err)
	}
	if !found || first.Offset != 0 {
		t.Fatalf("first Consume() = (%+v, %v), want offset 0", first, found)
	}

	// Background purger, as wired in production (cmd/narad runs it at 1s;
	// a short interval keeps the test fast).
	pctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go engine.offsets.RunPurger(pctx, 20*time.Millisecond)

	// Long-poll with Wait comfortably above the 200ms visibility
	// timeout. The redelivery must arrive shortly after expiry, not at
	// the Wait deadline.
	const wait = 5 * time.Second
	start := time.Now()
	msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: wait})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("long-poll Consume() error = %v", err)
	}
	if !found {
		t.Fatal("long-poll Consume() found = false, want redelivery of the expired reservation")
	}
	if msg.Offset != 0 {
		t.Fatalf("long-poll Consume() offset = %d, want 0", msg.Offset)
	}
	if elapsed >= wait/2 {
		t.Fatalf("long-poll Consume() took %v — expiry did not wake the waiter (slept toward the %v deadline)", elapsed, wait)
	}
	if msg.ReceiptHandle == first.ReceiptHandle {
		t.Fatal("redelivery reused the expired receipt handle")
	}
}

// newCapOneTestEngine builds an engine whose in-flight resolver caps
// every partition at MaxInFlight=1, so a single reservation saturates
// the partition and further consumes park on the cap.
func newCapOneTestEngine(t *testing.T, ms *messagingFakeMetastore) *Engine {
	t.Helper()
	logs := runtime.NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1, MaxAckedAhead: 10}, nil
	}, nil)
	return NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")
}

// TestLongPollWakesOnAckFreeingCapSlot pins the ack wake-up, sibling to
// TestLongPollWakesOnVisibilityTimeoutExpiry: a long-poller parked
// because the partition is at its MaxInFlight cap must be woken when an
// ack removes the live reservation and frees the slot (via the release
// notifier), well before its own Wait deadline — not left to sleep out
// the full Wait even though a message is deliverable.
func TestLongPollWakesOnAckFreeingCapSlot(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 60_000}
	engine := newCapOneTestEngine(t, ms)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, _, err := log.AppendBatch([][]byte{[]byte(`{"id":1}`), []byte(`{"id":2}`)}); err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}
	if err := log.AdvanceHighWatermark(2); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}

	// Warm the engine's topic cache: the test fake metastore counts
	// GetTopic calls without synchronization, so concurrent cold-cache
	// loads would trip the race detector on test bookkeeping.
	if _, err := engine.getTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("getTopic() warm-up error = %v", err)
	}

	// Consumer A reserves offset 0 — the partition's only cap slot.
	first, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0})
	if err != nil {
		t.Fatalf("first Consume() error = %v", err)
	}
	if !found || first.Offset != 0 {
		t.Fatalf("first Consume() = (%+v, %v), want offset 0", first, found)
	}

	// Consumer B long-polls; offset 1 is visible but the cap is full, so
	// B parks in waitForActivity. The 60s visibility timeout guarantees
	// no expiry-driven wake-up can mask a missing ack wake-up.
	const wait = 5 * time.Second
	results := make(chan longPollResult, 1)
	go func() {
		start := time.Now()
		msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: wait})
		results <- longPollResult{msg: msg, found: found, err: err, elapsed: time.Since(start)}
	}()
	// Let B park before the ack.
	time.Sleep(100 * time.Millisecond)

	// A acks: the live reservation is removed, freeing the cap slot. The
	// release notifier must wake B immediately.
	if err := engine.Ack(context.Background(), "orders", decodeHandleForTest(t, first.ReceiptHandle)); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	select {
	case r := <-results:
		if r.err != nil {
			t.Fatalf("long-poll Consume() error = %v", r.err)
		}
		if !r.found {
			t.Fatal("long-poll Consume() found = false, want the next message after the ack freed the cap slot")
		}
		if r.msg.Offset != 1 {
			t.Fatalf("long-poll Consume() offset = %d, want 1", r.msg.Offset)
		}
		if r.elapsed >= wait/2 {
			t.Fatalf("long-poll Consume() took %v — the ack did not wake the cap-parked waiter (slept toward the %v deadline)", r.elapsed, wait)
		}
	case <-time.After(wait + time.Second):
		t.Fatal("long-poller never returned")
	}
}
