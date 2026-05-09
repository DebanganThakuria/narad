package e2e

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/topic"
)

func TestCreateTopic(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	tp := env.createTopic("orders", 8, 2, topic.Retention{})
	if tp.Name != "orders" {
		t.Fatalf("name: got %q, want orders", tp.Name)
	}
	if tp.Partitions != 8 {
		t.Fatalf("partitions: got %d, want 8", tp.Partitions)
	}
}

func TestCreateTopicDefaults(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	tp := env.createTopic("events", 0, 0, topic.Retention{})
	if tp.Partitions != 4 {
		t.Fatalf("partitions: got %d, want default 4", tp.Partitions)
	}
	if tp.ReplicationFactor != 2 {
		t.Fatalf("rf: got %d, want default 2", tp.ReplicationFactor)
	}
}

func TestCreateTopicWithRetention(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	tp := env.createTopic("logs", 2, 2, topic.Retention{MaxAgeMs: 3_600_000, MaxBytes: 1 << 30})
	if tp.Retention.MaxAgeMs != 3_600_000 {
		t.Fatalf("max_age_ms: got %d, want 3600000", tp.Retention.MaxAgeMs)
	}
	if tp.Retention.MaxBytes != 1<<30 {
		t.Fatalf("max_bytes: got %d, want %d", tp.Retention.MaxBytes, 1<<30)
	}
}

func TestCreateTopicDuplicate(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("dup", 1, 2, topic.Retention{})
	resp := env.post("/v1/topics", map[string]any{"name": "dup"})
	expectConflict(t, resp)
}

func TestGetTopic(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("test-get", 2, 2, topic.Retention{})
	resp := env.get("/v1/topics/test-get")
	expectOK(t, resp)

	details := readJSON[topic.Details](t, resp)
	if details.Name != "test-get" {
		t.Fatalf("name: got %q, want test-get", details.Name)
	}
	if len(details.Partitions) != 2 {
		t.Fatalf("partition stats: got %d, want 2", len(details.Partitions))
	}
}

func TestGetTopicNotFound(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.get("/v1/topics/no-such-topic")
	expectNotFound(t, resp)
}

func TestListTopics(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("aaa", 1, 2, topic.Retention{})
	env.createTopic("bbb", 1, 2, topic.Retention{})

	resp := env.get("/v1/topics")
	expectOK(t, resp)

	var wrapper struct {
		Topics []topic.Topic `json:"topics"`
	}
	wrapper = readJSON[struct {
		Topics []topic.Topic `json:"topics"`
	}](t, resp)
	if len(wrapper.Topics) != 2 {
		t.Fatalf("count: got %d, want 2", len(wrapper.Topics))
	}
	if wrapper.Topics[0].Name != "aaa" || wrapper.Topics[1].Name != "bbb" {
		t.Fatalf("order: got %v", []string{wrapper.Topics[0].Name, wrapper.Topics[1].Name})
	}
}

func TestListTopicsPagination(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	for _, name := range []string{"p1", "p2", "p3"} {
		env.createTopic(name, 1, 2, topic.Retention{})
	}

	// Page 1
	resp := env.get("/v1/topics?limit=2")
	expectOK(t, resp)
	var wrapper struct {
		Topics        []topic.Topic `json:"topics"`
		NextPageToken string        `json:"next_page_token"`
	}
	wrapper = readJSON[struct {
		Topics        []topic.Topic `json:"topics"`
		NextPageToken string        `json:"next_page_token"`
	}](t, resp)

	if len(wrapper.Topics) != 2 {
		t.Fatalf("page 1: got %d topics, want 2", len(wrapper.Topics))
	}
	if wrapper.NextPageToken == "" {
		t.Fatal("expected next_page_token")
	}

	// Page 2
	resp = env.get("/v1/topics?limit=2&page_token=" + wrapper.NextPageToken)
	defer resp.Body.Close()
	wrapper = readJSON[struct {
		Topics        []topic.Topic `json:"topics"`
		NextPageToken string        `json:"next_page_token"`
	}](t, resp)

	if len(wrapper.Topics) != 1 {
		t.Fatalf("page 2: got %d topics, want 1", len(wrapper.Topics))
	}
}

func TestDeleteTopic(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("to-delete", 1, 2, topic.Retention{})
	resp := env.del("/v1/topics/to-delete")
	expectStatus(t, resp, 204) // broker returns 204 No Content

	// Confirm it's gone
	resp = env.get("/v1/topics/to-delete")
	expectNotFound(t, resp)
}

func TestDeleteTopicNotFound(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.del("/v1/topics/no-such-topic")
	expectNotFound(t, resp)
}

// ---- alter tests -----------------------------------------------------------

func TestAlterPartitions(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("scale", 2, 2, topic.Retention{})

	resp := env.patch("/v1/topics/scale", map[string]any{"partitions": 8})
	expectOK(t, resp)

	updated := readJSON[topic.Topic](t, resp)
	if updated.Partitions != 8 {
		t.Fatalf("partitions: got %d, want 8", updated.Partitions)
	}
}

func TestAlterPartitionsDecrease(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("nodec", 8, 2, topic.Retention{})
	resp := env.patch("/v1/topics/nodec", map[string]any{"partitions": 2})
	expectBadRequest(t, resp)
}

func TestAlterRetention(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("ret-topic", 1, 2, topic.Retention{})

	resp := env.patch("/v1/topics/ret-topic", map[string]any{
		"retention": map[string]any{"max_age_ms": 999_999, "max_bytes": 123_456},
	})
	expectOK(t, resp)

	updated := readJSON[topic.Topic](t, resp)
	if updated.Retention.MaxAgeMs != 999_999 {
		t.Fatalf("max_age_ms: got %d, want 999999", updated.Retention.MaxAgeMs)
	}
	if updated.Retention.MaxBytes != 123_456 {
		t.Fatalf("max_bytes: got %d, want 123456", updated.Retention.MaxBytes)
	}
}

// ---- alter with schema -----------------------------------------------------

const schemaV1 = `{
  "type": "object",
  "properties": {
    "id":    { "type": "integer" },
    "name":  { "type": "string" }
  },
  "required": ["id"]
}`

const schemaV2Additive = `{
  "type": "object",
  "properties": {
    "id":    { "type": "integer" },
    "name":  { "type": "string" },
    "email": { "type": "string" }
  },
  "required": ["id"]
}`

const schemaV2TypeChange = `{
  "type": "object",
  "properties": {
    "id":   { "type": "string" },
    "name": { "type": "string" }
  },
  "required": ["id"]
}`

const schemaV2RemoveField = `{
  "type": "object",
  "properties": {
    "id": { "type": "integer" }
  },
  "required": ["id"]
}`

func TestAlterSchemaFirstVersion(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-first", 1, 2, topic.Retention{})

	resp := env.patch("/v1/topics/schema-first", map[string]any{
		"schema": json.RawMessage(schemaV1),
	})
	expectOK(t, resp)
}

func TestAlterSchemaCompatible(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-compat", 1, 2, topic.Retention{})

	resp := env.patch("/v1/topics/schema-compat", map[string]any{
		"schema": json.RawMessage(schemaV1),
	})
	expectOK(t, resp)

	// Additive-only change → OK
	resp = env.patch("/v1/topics/schema-compat", map[string]any{
		"schema": json.RawMessage(schemaV2Additive),
	})
	expectOK(t, resp)
}

func TestAlterSchemaIncompatibleTypeChange(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-type", 1, 2, topic.Retention{})

	resp := env.patch("/v1/topics/schema-type", map[string]any{
		"schema": json.RawMessage(schemaV1),
	})
	expectOK(t, resp)

	// integer → string type change → 400
	resp = env.patch("/v1/topics/schema-type", map[string]any{
		"schema": json.RawMessage(schemaV2TypeChange),
	})
	expectBadRequest(t, resp)
	if msg := readError(t, resp); !strings.Contains(msg, "compatible") && !strings.Contains(msg, "type") {
		t.Fatalf("expected compatibility error, got: %s", msg)
	}
}

func TestAlterSchemaIncompatibleRemoval(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-rem", 1, 2, topic.Retention{})

	resp := env.patch("/v1/topics/schema-rem", map[string]any{
		"schema": json.RawMessage(schemaV1),
	})
	expectOK(t, resp)

	// v2 removes "name" property → 400
	resp = env.patch("/v1/topics/schema-rem", map[string]any{
		"schema": json.RawMessage(schemaV2RemoveField),
	})
	expectBadRequest(t, resp)
}

func TestAlterSchemaInvalidJSON(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-invalid", 1, 2, topic.Retention{})

	resp := env.patch("/v1/topics/schema-invalid", map[string]any{
		"schema": "not a valid json schema",
	})
	expectBadRequest(t, resp)
}

func TestAlterSchemaWithPartitions(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-multi", 2, 2, topic.Retention{})

	resp := env.patch("/v1/topics/schema-multi", map[string]any{
		"partitions": 6,
		"schema":     json.RawMessage(schemaV1),
	})
	expectOK(t, resp)

	updated := readJSON[topic.Topic](t, resp)
	if updated.Partitions != 6 {
		t.Fatalf("partitions: got %d, want 6", updated.Partitions)
	}
}

func TestAlterSchemaWithRetention(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("schema-ret", 1, 2, topic.Retention{})

	resp := env.patch("/v1/topics/schema-ret", map[string]any{
		"retention": map[string]any{"max_age_ms": 42_000},
		"schema":    json.RawMessage(schemaV1),
	})
	expectOK(t, resp)

	updated := readJSON[topic.Topic](t, resp)
	if updated.Retention.MaxAgeMs != 42_000 {
		t.Fatalf("max_age_ms: got %d, want 42000", updated.Retention.MaxAgeMs)
	}
}

func TestAlterAllThree(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("all-three", 2, 2, topic.Retention{})

	resp := env.patch("/v1/topics/all-three", map[string]any{
		"partitions": 8,
		"retention":  map[string]any{"max_age_ms": 99_999},
		"schema":     json.RawMessage(schemaV1),
	})
	expectOK(t, resp)

	updated := readJSON[topic.Topic](t, resp)
	if updated.Partitions != 8 {
		t.Fatalf("partitions: got %d, want 8", updated.Partitions)
	}
	if updated.Retention.MaxAgeMs != 99_999 {
		t.Fatalf("retention: got %d, want 99999", updated.Retention.MaxAgeMs)
	}
}

func TestAlterEmptyBody(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	env.createTopic("empty-body", 1, 2, topic.Retention{})
	resp := env.patch("/v1/topics/empty-body", map[string]any{})
	expectBadRequest(t, resp)
}

func TestAlterNotFound(t *testing.T) {
	env := newEnv(t, defaultOpts())
	defer env.close()

	resp := env.patch("/v1/topics/no-such", map[string]any{"partitions": 8})
	expectNotFound(t, resp)
}