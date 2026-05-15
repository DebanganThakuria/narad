package e2e

import (
	"net/http"
	"testing"
)

func TestAck_HappyPath(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "ack", Partitions: 2})

	pr := mustProduce(t, env, "ack", "k", map[string]int{"v": 1})
	msg, found := mustConsume(t, env, "ack", consumeQuery{Partition: new(pr.Partition)})
	if !found {
		t.Fatal("expected a message after produce")
	}

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/ack/ack",
		map[string]any{"receipt_handle": msg.ReceiptHandle})
	expectStatus(t, resp, http.StatusNoContent)
}

// TestAck_LowerThanCommittedIsNoOp: acks out-of-order — offset 1 first
// (parks in ackedAhead), then offset 0 (commits in-order, chain collapses
// to 1). After both commits, queue consume returns offset 2.
func TestAck_LowerThanCommittedIsNoOp(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "monotonic-commit", Partitions: 1})

	for i := range 3 {
		mustProduce(t, env, "monotonic-commit", "k", map[string]int{"i": i})
	}

	// Consume offset 0 and offset 1.
	msg0, found0 := mustConsume(t, env, "monotonic-commit", consumeQuery{})
	if !found0 || msg0.Offset != 0 {
		t.Fatalf("first consume: found=%v offset=%d want 0", found0, msg0.Offset)
	}
	msg1, found1 := mustConsume(t, env, "monotonic-commit", consumeQuery{})
	if !found1 || msg1.Offset != 1 {
		t.Fatalf("second consume: found=%v offset=%d want 1", found1, msg1.Offset)
	}

	// Ack offset 1 first (out of order) — parks in ackedAhead.
	mustAck(t, env, "monotonic-commit", msg1.ReceiptHandle)

	// Ack offset 0 — in-order commit; chain collapses, committed advances to 1.
	mustAck(t, env, "monotonic-commit", msg0.ReceiptHandle)

	// Queue-style consume must return offset 2.
	msg, found := mustConsume(t, env, "monotonic-commit", consumeQuery{})
	if !found || msg.Offset != 2 {
		t.Errorf("after both acks: found=%v offset=%d want offset=2", found, msg.Offset)
	}
}

func TestAck_NotFoundForUnknownTopic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	// Create, produce, consume to get a valid handle, then delete the topic.
	mustCreateTopic(t, env, createTopicReq{Name: "gone"})
	mustProduce(t, env, "gone", "k", map[string]int{"v": 1})
	msg, found := mustConsume(t, env, "gone", consumeQuery{})
	if !found {
		t.Fatal("expected a message")
	}
	del := jsonReq(t, http.MethodDelete, env.Server.URL+"/v1/topics/gone", nil)
	expectStatus(t, del, http.StatusNoContent)

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/gone/ack",
		map[string]any{"receipt_handle": msg.ReceiptHandle})
	expectStatus(t, resp, http.StatusNotFound)
}

func TestAck_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-json"})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics/bad-json/ack",
		[]byte("{not json}"))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAck_RejectsUnknownFields(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "extra-fields"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/extra-fields/ack",
		map[string]any{"receipt_handle": "x", "garbage": true})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAck_RejectsMissingHandle(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "no-handle"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/no-handle/ack",
		map[string]any{})
	expectStatus(t, resp, http.StatusBadRequest)
}
