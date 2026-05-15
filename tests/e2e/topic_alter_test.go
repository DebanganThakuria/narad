package e2e

import (
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/topic"
)

func TestAlterTopic_IncreasePartitions(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "alter", Partitions: 2})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/alter",
		map[string]any{"partitions": 6})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	var got topic.Topic
	decodeJSON(t, resp, &got)
	if got.Partitions != 6 {
		t.Errorf("partitions: got %d want 6", got.Partitions)
	}

	// Verify persisted. d.Topic.Partitions is the count; d.Partitions
	// (the explicit field on Details) shadows that with the slice of
	// PartitionStats.
	resp = getJSON(t, env.Server.URL+"/v1/topics/alter")
	var d topic.Details
	decodeJSON(t, resp, &d)
	if d.Topic.Partitions != 6 {
		t.Errorf("persisted partitions: got %d want 6", d.Topic.Partitions)
	}
	if len(d.Partitions) != 6 {
		t.Errorf("partition stats: got %d want 6", len(d.Partitions))
	}
}

func TestAlterTopic_RejectsPartitionDecrease(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "shrink", Partitions: 4})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/shrink",
		map[string]any{"partitions": 2})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_RejectsPartitionEqual(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "same", Partitions: 4})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/same",
		map[string]any{"partitions": 4})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_RejectsAboveMaxPartitions(t *testing.T) {
	env := newTestEnv(t, withPolicy(broker.TopicPolicy{
		DefaultPartitions:        2,
		MaxPartitions:            8,
		DefaultReplicationFactor: 2,
		DefaultRetention:         topic.Retention{MaxAgeMs: 1000},
	}))
	mustCreateTopic(t, env, createTopicReq{Name: "cap", Partitions: 4})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/cap",
		map[string]any{"partitions": 16})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_UpdateRetention(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{
		Name:      "retention",
		Retention: topic.Retention{MaxAgeMs: 60_000, MaxBytes: 1024},
	})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/retention",
		map[string]any{
			"retention": map[string]any{
				"max_age_ms": 7_200_000,
				"max_bytes":  524_288_000,
			},
		})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	var got topic.Topic
	decodeJSON(t, resp, &got)

	if got.Retention.MaxAgeMs != 7_200_000 {
		t.Errorf("max_age_ms: got %d want 7200000", got.Retention.MaxAgeMs)
	}
	if got.Retention.MaxBytes != 524_288_000 {
		t.Errorf("max_bytes: got %d want 524288000", got.Retention.MaxBytes)
	}
}

// TestAlterTopic_RetentionUpdateReopensPartitionLogs verifies that a
// retention update doesn't break the partition log cache. The broker
// closes cached logs on update; the next produce must succeed by
// reopening with the new bounds.
func TestAlterTopic_RetentionUpdateReopensPartitionLogs(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "reopen", Partitions: 1})

	// Open the partition log via a produce.
	mustProduce(t, env, "reopen", "k", map[string]int{"v": 1})

	// PATCH retention — should close cached logs.
	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/reopen",
		map[string]any{"retention": map[string]any{"max_age_ms": 10_000}})
	expectStatus(t, resp, http.StatusOK)

	// Subsequent produce exercises the reopen path.
	mustProduce(t, env, "reopen", "k", map[string]int{"v": 2})
}

func TestAlterTopic_RetentionPartialDefaulting(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{
		Name:      "partial",
		Retention: topic.Retention{MaxAgeMs: 60_000, MaxBytes: 4096},
	})

	// Update only MaxBytes; MaxAgeMs zero → resolveRetention falls
	// back to the policy default. Check the persisted value matches
	// the env's default (24h = 86_400_000 ms).
	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/partial",
		map[string]any{"retention": map[string]any{"max_bytes": 8192}})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	var got topic.Topic
	decodeJSON(t, resp, &got)

	if got.Retention.MaxBytes != 8192 {
		t.Errorf("max_bytes: got %d want 8192", got.Retention.MaxBytes)
	}
	if got.Retention.MaxAgeMs != int64(24*60*60*1000) {
		t.Errorf("max_age_ms: got %d want 86400000 (24h policy default)", got.Retention.MaxAgeMs)
	}
}

func TestAlterTopic_RejectsNegativeRetention(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "neg"})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/neg",
		map[string]any{"retention": map[string]any{"max_age_ms": -100}})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_RejectsBothFields(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "both"})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/both",
		map[string]any{
			"partitions": 8,
			"retention":  map[string]any{"max_age_ms": 1000},
		})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_RejectsNeitherField(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "neither"})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/neither",
		map[string]any{})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_NotFound(t *testing.T) {
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/missing",
		map[string]any{"partitions": 5})
	expectStatus(t, resp, http.StatusNotFound)
}

func TestAlterTopic_RejectsInvalidJSON(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-json"})

	resp := rawReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/bad-json", []byte("{not json}"))
	expectStatus(t, resp, http.StatusBadRequest)
}
