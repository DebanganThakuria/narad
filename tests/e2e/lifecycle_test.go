package e2e

import (
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestHealthz(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.get("/healthz")
	expectOK(t, resp)
}

func TestReadyz(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.get("/readyz")
	expectOK(t, resp)
}

func TestFullLifecycle(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	// Create
	resp := env.post("/v1/topics", map[string]any{
		"name":         "full-cycle",
		"partitions":   4,
		"retention_ms": int64(3_600_000),
	})
	expectStatus(t, resp, http.StatusCreated)

	// Produce some messages
	env.produce("full-cycle", "k1", `{"msg": "hello"}`)
	env.produce("full-cycle", "k2", `{"msg": "world"}`)

	// Consume and ack
	msg := env.consume("/v1/topics/full-cycle/consume?partition=0")
	env.ack("full-cycle", msg.ReceiptHandle)

	// Alter partitions + retention together
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

	// Delete
	resp = env.del("/v1/topics/full-cycle")
	expectStatus(t, resp, http.StatusNoContent)

	// Gone
	resp = env.get("/v1/topics/full-cycle")
	expectNotFound(t, resp)
}

func TestFullLifecycleWithSchema(t *testing.T) {
	t.Parallel()
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-cycle", 3, 2, 0)

	// Register schema
	resp := env.patch("/v1/topics/schema-cycle", map[string]any{
		"schema": jsonRaw(schemaV1),
	})
	expectOK(t, resp)

	// Produce with valid data matching schema
	env.produce("schema-cycle", "k", `{"id": 42, "name": "test"}`)

	// Consume
	resp = env.get("/v1/topics/schema-cycle/consume")
	expectOK(t, resp)

	// Evolve schema (add email field)
	resp = env.patch("/v1/topics/schema-cycle", map[string]any{
		"schema": jsonRaw(schemaV2Additive),
	})
	expectOK(t, resp)

	// Delete
	resp = env.del("/v1/topics/schema-cycle")
	expectStatus(t, resp, http.StatusNoContent)
}
