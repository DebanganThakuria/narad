package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestConsume_RoundTripIntegrity is the most important data-plane
// test: produce a known JSON payload, consume it back, and confirm
// every byte (and the Topic/Partition/Offset metadata) survived the
// trip.
//
// We compare via canonical JSON re-serialisation rather than field
// equality because json.Unmarshal turns every number into float64,
// which makes naive map equality awkward.
func TestConsume_RoundTripIntegrity(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "rt", Partitions: 3})

	want := map[string]any{"event": "checkout", "user": 42, "amount": 19.99}
	pr := mustProduce(t, env, "rt", "k", want)

	msg, found := mustConsume(t, env, "rt", consumeQuery{})
	if !found {
		t.Fatal("expected a message; got 204")
	}
	if msg.Topic != "rt" {
		t.Errorf("topic: got %q want %q", msg.Topic, "rt")
	}
	if msg.Partition != pr.Partition {
		t.Errorf("partition: got %d want %d", msg.Partition, pr.Partition)
	}
	if msg.Offset != pr.Offset {
		t.Errorf("offset: got %d want %d", msg.Offset, pr.Offset)
	}

	// Round-trip through json.Marshal on both sides so map ordering
	// and number representations are normalized.
	wantJSON, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	var gotMap, wantMap map[string]any
	if err := json.Unmarshal(msg.Payload, &gotMap); err != nil {
		t.Fatalf("decode got: %v", err)
	}
	if err := json.Unmarshal(wantJSON, &wantMap); err != nil {
		t.Fatalf("decode want: %v", err)
	}
	gotNorm, _ := json.Marshal(gotMap)
	wantNorm, _ := json.Marshal(wantMap)
	if string(gotNorm) != string(wantNorm) {
		t.Errorf("payload mismatch:\n  got:  %s\n  want: %s", gotNorm, wantNorm)
	}
}

func TestConsume_ReturnsNoContentWhenEmpty(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "empty", Partitions: 3})

	_, found := mustConsume(t, env, "empty", consumeQuery{})
	if found {
		t.Error("empty topic should return 204")
	}
}

// TestConsume_AckAdvancesQueueCursor verifies the queue-mode contract:
// after Ack(offset=N), the next queue-style consume on the same
// partition returns offset N+1 (or 204 if drained).
func TestConsume_AckAdvancesQueueCursor(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "ack-cursor", Partitions: 3})

	for i := range 3 {
		mustProduce(t, env, "ack-cursor", "k", map[string]int{"i": i})
	}

	// First consume returns offset 0.
	msg, found := mustConsume(t, env, "ack-cursor", consumeQuery{})
	if !found || msg.Offset != 0 {
		t.Fatalf("first consume: found=%v offset=%d want offset=0", found, msg.Offset)
	}
	mustAck(t, env, "ack-cursor", msg.ReceiptHandle)

	// Next consume must skip past the acked offset.
	msg, found = mustConsume(t, env, "ack-cursor", consumeQuery{})
	if !found || msg.Offset != 1 {
		t.Fatalf("after ack: found=%v offset=%d want offset=1", found, msg.Offset)
	}
}

func TestConsume_PartitionPinned(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "pinned", Partitions: 4})

	pr := mustProduce(t, env, "pinned", "stable", map[string]int{"v": 1})

	// Consume from the same partition the produce landed on — should hit.
	msg, found := mustConsume(t, env, "pinned", consumeQuery{Partition: new(pr.Partition)})
	if !found {
		t.Fatalf("partition %d: expected message, got 204", pr.Partition)
	}
	if msg.Partition != pr.Partition {
		t.Errorf("partition: got %d want %d", msg.Partition, pr.Partition)
	}

	// Consume from a partition that DIDN'T receive the message — 204.
	other := (pr.Partition + 1) % 4
	if _, found := mustConsume(t, env, "pinned", consumeQuery{Partition: new(other)}); found {
		t.Errorf("partition %d: expected 204, got a message", other)
	}
}

// TestConsume_ReplayDoesNotAdvanceCommittedOffset confirms that
// passing &offset reads in replay mode and is idempotent — the
// committed offset is unaffected, so a queue-style consume after a
// replay still returns offset 0.
func TestConsume_ReplayDoesNotAdvanceCommittedOffset(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "replay", Partitions: 3})

	pr := mustProduce(t, env, "replay", "k", map[string]int{"v": 1})

	// Replay offset 0 twice — should return the same message both times.
	for i := range 2 {
		msg, found := mustConsume(t, env, "replay",
			consumeQuery{Partition: new(pr.Partition), Offset: new(int64(0))})
		if !found {
			t.Fatalf("replay #%d: expected message, got 204", i)
		}
		if msg.Offset != 0 {
			t.Errorf("replay #%d offset: got %d want 0", i, msg.Offset)
		}
	}

	// Queue-style consume still returns offset 0 because no ack happened.
	msg, found := mustConsume(t, env, "replay", consumeQuery{})
	if !found || msg.Offset != 0 {
		t.Errorf("queue consume after replays: found=%v offset=%d want offset=0", found, msg.Offset)
	}
}

// TestConsume_ReplayPastTailReturns204 verifies that asking for a
// future offset (>= LogEndOffset) returns 204 rather than blocking.
func TestConsume_ReplayPastTailReturns204(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "future", Partitions: 3})

	mustProduce(t, env, "future", "k", map[string]int{"v": 1})

	if _, found := mustConsume(t, env, "future",
		consumeQuery{Partition: new(0), Offset: new(int64(999))}); found {
		t.Error("offset 999 with no records there: expected 204, got a message")
	}
}

func TestConsume_RejectsOffsetWithoutPartition(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "no-part"})

	resp := getJSON(t, env.Server.URL+"/v1/topics/no-part/consume?offset=0")
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestConsume_RejectsInvalidPartition(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-part"})

	resp := getJSON(t, env.Server.URL+"/v1/topics/bad-part/consume?partition=abc")
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestConsume_RejectsOutOfRangePartition(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "oor", Partitions: 3})

	resp := getJSON(t, env.Server.URL+"/v1/topics/oor/consume?partition=99")
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestConsume_RejectsInvalidOffset(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-off"})

	resp := getJSON(t, env.Server.URL+"/v1/topics/bad-off/consume?partition=0&offset=xyz")
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestConsume_RejectsInvalidWait(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-wait"})

	resp := getJSON(t, env.Server.URL+"/v1/topics/bad-wait/consume?wait=notaduration")
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestConsume_NotFoundForUnknownTopic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/v1/topics/missing/consume")
	expectStatus(t, resp, http.StatusNotFound)
}

// TestConsume_LongPollWaitsAndReturnsOnArrival exercises the
// happy-path long-poll: a consumer blocks waiting, a producer arrives
// midway, and the consumer wakes up with the new message before its
// wait expires.
func TestConsume_LongPollWaitsAndReturnsOnArrival(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, withMaxConsumeWait(2*time.Second))
	mustCreateTopic(t, env, createTopicReq{Name: "longpoll", Partitions: 3})

	type result struct {
		msg   string
		err   error
		took  time.Duration
		found bool
	}
	done := make(chan result, 1)

	go func() {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			env.Server.URL+"/v1/topics/longpoll/consume?wait=1s", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		done <- result{
			took:  time.Since(start),
			found: resp.StatusCode == http.StatusOK,
		}
	}()

	// Give the long-poll a moment to engage, then produce.
	time.Sleep(100 * time.Millisecond)
	mustProduce(t, env, "longpoll", "k", map[string]int{"v": 1})

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("long-poll request: %v", r.err)
		}
		if !r.found {
			t.Errorf("long-poll did not receive the produced message")
		}
		if r.took >= time.Second {
			t.Errorf("long-poll took %v; should have woken up well before wait=1s", r.took)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long-poll did not return within 3s")
	}
}

// TestConsume_LongPollTimesOutWith204 verifies that wait expiry on an
// empty topic returns 204 cleanly, not an error.
func TestConsume_LongPollTimesOutWith204(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, withMaxConsumeWait(500*time.Millisecond))
	mustCreateTopic(t, env, createTopicReq{Name: "timeout", Partitions: 3})

	start := time.Now()
	resp := getJSON(t, env.Server.URL+"/v1/topics/timeout/consume?wait=200ms")
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status: got %d want 204; body=%s", resp.StatusCode, readBody(resp))
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("returned in %v; expected at least ~200ms wait", elapsed)
	}
	if elapsed > 800*time.Millisecond {
		t.Errorf("returned in %v; far longer than wait=200ms", elapsed)
	}
}
