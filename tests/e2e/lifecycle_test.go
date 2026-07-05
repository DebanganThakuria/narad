package e2e

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestHealthz(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	resp := env.get("/healthz")
	expectOK(t, resp)
}

func TestReadyz(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	resp := env.get("/readyz")
	expectOK(t, resp)
}

// TestFullLifecycle walks a topic through its whole life:
// create → produce → consume+ack → alter → delete → 404.
func TestFullLifecycle(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	resp := env.post("/v1/topics", map[string]any{
		"name":         "full-cycle",
		"partitions":   4,
		"retention_ms": int64(3_600_000),
	})
	expectStatus(t, resp, http.StatusCreated)

	env.produce("full-cycle", "k1", `{"msg": "hello"}`)
	env.produce("full-cycle", "k2", `{"msg": "world"}`)

	msg := env.consume("/v1/topics/full-cycle/consume?partition=0")
	env.ack("full-cycle", msg.ReceiptHandle)

	resp = env.patch("/v1/topics/full-cycle", map[string]any{
		"partitions":   6,
		"retention_ms": int64(7_200_000),
	})
	expectOK(t, resp)

	updated := readJSON[topic.Topic](t, resp)
	if updated.Partitions != 6 {
		t.Fatalf("partitions: got %d, want 6", updated.Partitions)
	}
	if updated.RetentionMs != 7_200_000 {
		t.Fatalf("retention_ms: got %d, want 7200000", updated.RetentionMs)
	}

	resp = env.del("/v1/topics/full-cycle")
	expectStatus(t, resp, http.StatusNoContent)

	resp = env.get("/v1/topics/full-cycle")
	expectNotFound(t, resp)
}

// TestFullLifecycleWithSchema covers the schema arc on one topic:
// register → produce valid payload → consume → evolve additively → delete.
func TestFullLifecycleWithSchema(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	env.createTopic("schema-cycle", 3, 0)

	resp := env.patch("/v1/topics/schema-cycle", map[string]any{
		"schema": json.RawMessage(schemaV1),
	})
	expectOK(t, resp)

	env.produce("schema-cycle", "k", `{"id": 42, "name": "test"}`)

	resp = env.get("/v1/topics/schema-cycle/consume")
	expectOK(t, resp)

	resp = env.patch("/v1/topics/schema-cycle", map[string]any{
		"schema": json.RawMessage(schemaV2Additive),
	})
	expectOK(t, resp)

	resp = env.del("/v1/topics/schema-cycle")
	expectStatus(t, resp, http.StatusNoContent)
}
