package e2e

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// TestParallelConsumersOnePartitionDistinctMessages verifies that two
// concurrent consumers on a single partition each get a different
// offset (gap-skipping ReserveNext working end-to-end through HTTP).
func TestParallelConsumersOnePartitionDistinctMessages(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("para", 1, 2, int64(0))
	for i := 0; i < 5; i++ {
		env.produce("para", "k", `{"i": 1}`)
	}

	type result struct {
		offset int64
		handle string
	}
	const consumers = 3
	results := make(chan result, consumers)
	var wg sync.WaitGroup
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := env.consume("/v1/topics/para/consume?partition=0&wait=2s")
			results <- result{offset: msg.Offset, handle: msg.ReceiptHandle}
		}()
	}
	wg.Wait()
	close(results)

	seen := map[int64]struct{}{}
	for r := range results {
		if _, dup := seen[r.offset]; dup {
			t.Fatalf("offset %d delivered twice — gap-skipping is broken", r.offset)
		}
		seen[r.offset] = struct{}{}
		if r.handle == "" {
			t.Fatal("expected receipt_handle on every consume")
		}
	}
	if len(seen) != consumers {
		t.Fatalf("expected %d distinct offsets, got %d", consumers, len(seen))
	}
}

// TestOutOfOrderAckCommitAdvancesContiguous covers the walk-forward
// path: three reservations, ack arrives in 1, 2, 0 order, and the
// final committed offset should be 2.
func TestOutOfOrderAckCommitAdvancesContiguous(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("ooo", 1, 2, int64(0))
	for i := 0; i < 3; i++ {
		env.produce("ooo", "k", `{"i": 1}`)
	}

	// Pin partition so we get offsets 0,1,2 in order.
	m0 := env.consume("/v1/topics/ooo/consume?partition=0")
	m1 := env.consume("/v1/topics/ooo/consume?partition=0")
	m2 := env.consume("/v1/topics/ooo/consume?partition=0")
	if m0.Offset != 0 || m1.Offset != 1 || m2.Offset != 2 {
		t.Fatalf("expected offsets 0,1,2 from sequential reserves; got %d,%d,%d", m0.Offset, m1.Offset, m2.Offset)
	}

	// Ack 1 (out of order) — should NOT advance committed.
	env.ack("ooo", m1.ReceiptHandle)
	// Ack 2 — also out of order.
	env.ack("ooo", m2.ReceiptHandle)
	// Ack 0 — walk forward through 1, 2.
	env.ack("ooo", m0.ReceiptHandle)

	// Verify via partition_stats.next_offset (poll snapshot).
	resp := env.get("/v1/topics/ooo")
	expectOK(t, resp)
	details := readJSON[topic.Details](t, resp)
	if details.Partitions[0].NextOffset != 3 {
		t.Fatalf("partition next_offset: got %d, want 3", details.Partitions[0].NextOffset)
	}
	// A fresh consume should now return 204 — head is fully advanced.
	if resp := env.get("/v1/topics/ooo/consume?partition=0"); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 after all acked, got %d", resp.StatusCode)
	}
}

// TestAckTamperedHandleReturns400 verifies that a corrupted receipt
// handle is rejected. The handle format is base64url(json({t,p,o,n}))
// with no HMAC — corruption causes a decode failure → 400.
func TestAckTamperedHandleReturns400(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("tamper", 1, 2, int64(0))
	env.produce("tamper", "k", `{}`)

	// Flip a byte inside the base64 to produce invalid encoding.
	msg := env.consume("/v1/topics/tamper/consume")
	if len(msg.ReceiptHandle) < 4 {
		t.Fatalf("handle too short: %q", msg.ReceiptHandle)
	}
	tampered := []byte(msg.ReceiptHandle)
	tampered[2] ^= 0xFF // corrupt the middle of the base64 string
	resp := env.post("/v1/topics/tamper/ack", map[string]any{"receipt_handle": string(tampered)})
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestAckReusedHandleReturns410 covers a handle whose offset has
// already been committed — the visibility window has elapsed for the
// caller.
func TestAckReusedHandleReturns410(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("reuse", 1, 2, int64(0))
	env.produce("reuse", "k", `{}`)
	msg := env.consume("/v1/topics/reuse/consume")
	env.ack("reuse", msg.ReceiptHandle) // succeeds, committed advances

	// Replay the same handle.
	resp := env.post("/v1/topics/reuse/ack", map[string]any{"receipt_handle": msg.ReceiptHandle})
	expectStatus(t, resp, http.StatusGone)
}

// TestAckTopicMismatchReturns400 covers a handle for one topic sent
// against another's ack endpoint.
func TestAckTopicMismatchReturns400(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("topicA", 1, 2, int64(0))
	env.createTopic("topicB", 1, 2, int64(0))
	env.produce("topicA", "k", `{}`)
	msg := env.consume("/v1/topics/topicA/consume")

	resp := env.post("/v1/topics/topicB/ack", map[string]any{"receipt_handle": msg.ReceiptHandle})
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestAckEmptyHandleReturns400 covers the empty-handle short-circuit
// in the handler.
func TestAckEmptyHandleReturns400(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("empty-handle", 1, 2, int64(0))
	resp := env.post("/v1/topics/empty-handle/ack", map[string]any{"receipt_handle": ""})
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestInFlightCapBlocksFurtherReserves verifies that once
// MaxInFlightPerPartition is reached, additional consume calls
// (without acking) return 204 (no message available — partition cap
// hit).
func TestInFlightCapBlocksFurtherReserves(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.post("/v1/topics", map[string]any{
		"name":                        "capped",
		"partitions":                  1,
		"replication_factor":          2,
		"max_in_flight_per_partition": int64(2),
	})
	expectStatus(t, resp, http.StatusCreated)

	for i := 0; i < 5; i++ {
		env.produce("capped", "k", `{}`)
	}

	// Two reservations succeed.
	m1 := env.consume("/v1/topics/capped/consume?partition=0")
	m2 := env.consume("/v1/topics/capped/consume?partition=0")
	if m1.Offset == m2.Offset {
		t.Fatal("expected distinct offsets")
	}

	// Third reserve must fail (cap hit) — wait=0 → immediate 204.
	resp = env.get("/v1/topics/capped/consume?partition=0")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("third consume: got %d, want 204 (cap hit)", resp.StatusCode)
	}

	// Ack one, then a third consume should succeed.
	env.ack("capped", m1.ReceiptHandle)
	// The cap-skip notify path is documented as a v1 limitation, so we
	// drive a fresh consume rather than relying on long-poll wakeup.
	m3 := env.consume("/v1/topics/capped/consume?partition=0")
	if m3.Offset == m1.Offset || m3.Offset == m2.Offset {
		t.Fatalf("expected fresh offset after slot freed; got %d (m1=%d m2=%d)", m3.Offset, m1.Offset, m2.Offset)
	}
}

// TestAckedAheadCapReturns503 covers the ackedAhead-full path.
// Drives two out-of-order acks (cap=2), then a third triggers 503.
func TestAckedAheadCapReturns503(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.post("/v1/topics", map[string]any{
		"name":                          "stuckhead",
		"partitions":                    1,
		"replication_factor":            2,
		"max_acked_ahead_per_partition": int64(2),
	})
	expectStatus(t, resp, http.StatusCreated)

	// Produce 4 messages so we can reserve 0..3.
	for i := 0; i < 4; i++ {
		env.produce("stuckhead", "k", `{}`)
	}
	// Reserve all 4 in order.
	m0 := env.consume("/v1/topics/stuckhead/consume?partition=0")
	m1 := env.consume("/v1/topics/stuckhead/consume?partition=0")
	m2 := env.consume("/v1/topics/stuckhead/consume?partition=0")
	m3 := env.consume("/v1/topics/stuckhead/consume?partition=0")
	_ = m0 // intentionally unacked — keeps committed at 0

	// Ack m1 and m2 out of order — fills ackedAhead to cap (2).
	env.ack("stuckhead", m1.ReceiptHandle)
	env.ack("stuckhead", m2.ReceiptHandle)

	// Ack m3 — cap is full, must return 503.
	resp = env.post("/v1/topics/stuckhead/ack", map[string]any{"receipt_handle": m3.ReceiptHandle})
	expectStatus(t, resp, http.StatusServiceUnavailable)
}

// TestAlterCapsTakesEffect verifies that altering caps via PATCH
// updates the broker's view immediately (RefreshCaps integration).
func TestAlterCapsTakesEffect(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.post("/v1/topics", map[string]any{
		"name":                        "altercap",
		"partitions":                  1,
		"replication_factor":          2,
		"max_in_flight_per_partition": int64(1),
	})
	expectStatus(t, resp, http.StatusCreated)

	for i := 0; i < 3; i++ {
		env.produce("altercap", "k", `{}`)
	}

	// First consume succeeds; second is capped at 1, returns 204.
	env.consume("/v1/topics/altercap/consume?partition=0")
	if r := env.get("/v1/topics/altercap/consume?partition=0"); r.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 with cap=1; got %d", r.StatusCode)
	}

	// Raise cap to 5.
	resp = env.patch("/v1/topics/altercap", map[string]any{
		"max_in_flight_per_partition": int64(5),
	})
	expectOK(t, resp)

	// Now a second consume succeeds.
	env.consume("/v1/topics/altercap/consume?partition=0")
}

// TestParallelConsumersDoNotDuplicateMessages stress-tests the design:
// many consumer threads, single partition, every produced message
// must be delivered exactly once.
func TestParallelConsumersDoNotDuplicateMessages(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("stress", 1, 2, int64(0))
	const total = 50
	for i := 0; i < total; i++ {
		env.produce("stress", "k", `{}`)
	}

	const workers = 8
	var seen sync.Map // offset -> count
	var dupes atomic.Int64
	var done atomic.Int64

	deadline := time.Now().Add(5 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) && done.Load() < int64(total) {
				resp := env.get("/v1/topics/stress/consume?partition=0&wait=200ms")
				if resp.StatusCode == http.StatusNoContent {
					resp.Body.Close()
					continue
				}
				if resp.StatusCode != http.StatusOK {
					t.Errorf("unexpected status: %d", resp.StatusCode)
					resp.Body.Close()
					return
				}
				m := readJSON[topic.Message](t, resp)
				if _, loaded := seen.LoadOrStore(m.Offset, 1); loaded {
					dupes.Add(1)
				}
				env.ack("stress", m.ReceiptHandle)
				done.Add(1)
			}
		}()
	}
	wg.Wait()

	if d := dupes.Load(); d != 0 {
		t.Fatalf("got %d duplicate deliveries — gap-skipping is broken", d)
	}
	if got := done.Load(); got < int64(total) {
		t.Fatalf("processed only %d of %d messages within deadline", got, total)
	}
}
