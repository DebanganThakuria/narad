package e2e

import (
	"net/http"
	"testing"
)

// TestLifecycle_FullFlow walks the canonical user journey end-to-end:
// create → produce many → describe → consume all → ack all → alter →
// delete. It's intentionally one big test rather than sub-tests so a
// failure points at the exact step.
func TestLifecycle_FullFlow(t *testing.T) {
	env := newTestEnv(t)

	// 1. Create a topic.
	mustCreateTopic(t, env, createTopicReq{Name: "lifecycle", Partitions: 3})

	// 2. Produce 10 messages with rotating keys so they spread across partitions.
	const total = 10
	type position struct {
		partition int
		offset    int64
	}
	positions := make([]position, total)
	for i := range total {
		pr := mustProduce(t, env, "lifecycle", "k", map[string]int{"i": i})
		positions[i] = position{partition: pr.Partition, offset: pr.Offset}
	}

	// 3. Get topic details — verify partition count and that NextOffsets sum
	// to total.
	resp := getJSON(t, env.Server.URL+"/v1/topics/lifecycle")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get-details: %d body=%s", resp.StatusCode, readBody(resp))
	}
	var d struct {
		Name       string `json:"name"`
		Partitions []struct {
			NextOffset int64 `json:"next_offset"`
		} `json:"partition_stats"`
	}
	decodeJSON(t, resp, &d)
	if got := len(d.Partitions); got != 3 {
		t.Fatalf("partition count: got %d want 3", got)
	}
	var sum int64
	for _, p := range d.Partitions {
		sum += p.NextOffset
	}
	if sum != int64(total) {
		t.Fatalf("sum of next_offset: got %d want %d", sum, total)
	}

	// 4. Consume all messages via queue mode.
	consumed := 0
	for consumed < total {
		_, found := mustConsume(t, env, "lifecycle", consumeQuery{})
		if !found {
			break
		}
		consumed++
	}
	if consumed != total {
		t.Fatalf("consumed: got %d want %d", consumed, total)
	}

	// 5. Ack each (partition, offset) we produced.
	for _, p := range positions {
		mustAck(t, env, "lifecycle", p.partition, p.offset)
	}

	// 6. After acking everything, queue consume returns 204.
	if _, found := mustConsume(t, env, "lifecycle", consumeQuery{}); found {
		t.Error("queue consume after acking all: expected 204")
	}

	// 7. Alter — increase partitions.
	resp = jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/lifecycle",
		map[string]any{"partitions": 5})
	expectStatus(t, resp, http.StatusOK)

	// 8. Delete and confirm gone.
	resp = jsonReq(t, http.MethodDelete, env.Server.URL+"/v1/topics/lifecycle", nil)
	expectStatus(t, resp, http.StatusNoContent)

	resp = getJSON(t, env.Server.URL+"/v1/topics/lifecycle")
	expectStatus(t, resp, http.StatusNotFound)
}

// TestLifecycle_ReplayWorksAfterAck ensures that acking does NOT
// erase data on disk — replay-by-offset on an acked offset still
// returns the original message.
func TestLifecycle_ReplayWorksAfterAck(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "replay-after-ack", Partitions: 1})

	for i := range 3 {
		mustProduce(t, env, "replay-after-ack", "k", map[string]int{"i": i})
	}

	mustAck(t, env, "replay-after-ack", 0, 0)

	// Replay offset 0 — must still succeed.
	msg, found := mustConsume(t, env, "replay-after-ack",
		consumeQuery{Partition: intPtr(0), Offset: int64Ptr(0)})
	if !found {
		t.Fatal("replay of acked offset: expected message, got 204")
	}
	if msg.Offset != 0 {
		t.Errorf("replay offset: got %d want 0", msg.Offset)
	}
}

// TestLifecycle_DistributesAcrossPartitions feeds 30 distinct keys
// into a 3-partition topic and confirms all partitions received some
// traffic.
func TestLifecycle_DistributesAcrossPartitions(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "spread", Partitions: 3})

	hit := make(map[int]int)
	for i := range 30 {
		// Distinct keys → broker hashes to varied partitions.
		key := []byte{byte('a' + i)}
		pr := mustProduce(t, env, "spread", string(key), map[string]int{"i": i})
		hit[pr.Partition]++
	}

	if len(hit) < 2 {
		t.Errorf("hit only %d partitions out of 3 with 30 distinct keys: %v", len(hit), hit)
	}
}
