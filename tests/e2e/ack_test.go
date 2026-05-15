package e2e

import (
	"net/http"
	"testing"
)

func TestAck_HappyPath(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "ack", Partitions: 2})

	pr := mustProduce(t, env, "ack", "k", map[string]int{"v": 1})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/ack/ack",
		map[string]any{"partition": pr.Partition, "offset": pr.Offset})
	expectStatus(t, resp, http.StatusNoContent)
}

// TestAck_IsIdempotent verifies that re-acking the same offset is a
// no-op (still 204, no error). Important for at-least-once consumers
// that may retry an ack after a flaky network blip.
func TestAck_IsIdempotent(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "idem", Partitions: 1})

	pr := mustProduce(t, env, "idem", "k", map[string]int{"v": 1})

	for i := range 3 {
		resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/idem/ack",
			map[string]any{"partition": pr.Partition, "offset": pr.Offset})
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("ack #%d: got %d want 204", i, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
}

// TestAck_LowerThanCommittedIsNoOp acks at offset N, then again at
// offset 0. The second call must not regress the cursor — a queue
// consume should still skip past N.
func TestAck_LowerThanCommittedIsNoOp(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "monotonic-commit", Partitions: 1})

	for i := range 3 {
		mustProduce(t, env, "monotonic-commit", "k", map[string]int{"i": i})
	}

	// Ack offset 1 (the second message).
	mustAck(t, env, "monotonic-commit", 0, 1)

	// Try to "regress" to offset 0 — broker should treat it as a no-op.
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/monotonic-commit/ack",
		map[string]any{"partition": 0, "offset": 0})
	expectStatus(t, resp, http.StatusNoContent)

	// Queue-style consume must return offset 2, not offset 0.
	msg, found := mustConsume(t, env, "monotonic-commit", consumeQuery{})
	if !found || msg.Offset != 2 {
		t.Errorf("after regress attempt: found=%v offset=%d want offset=2", found, msg.Offset)
	}
}

func TestAck_RejectsOutOfRangePartition(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "oor-part", Partitions: 2})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/oor-part/ack",
		map[string]any{"partition": 99, "offset": 0})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAck_RejectsNegativeOffset(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "neg-off", Partitions: 1})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/neg-off/ack",
		map[string]any{"partition": 0, "offset": -1})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAck_NotFoundForUnknownTopic(t *testing.T) {
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/missing/ack",
		map[string]any{"partition": 0, "offset": 0})
	expectStatus(t, resp, http.StatusNotFound)
}

func TestAck_RejectsInvalidJSON(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-json"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/bad-json/ack",
		[]byte("{not json}"))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAck_RejectsUnknownFields(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "extra-fields"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/extra-fields/ack",
		map[string]any{"partition": 0, "offset": 0, "garbage": true})
	expectStatus(t, resp, http.StatusBadRequest)
}
