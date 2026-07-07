package e2e

import (
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestAlterTopic_IncreasePartitions(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "alter", Partitions: 3})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/alter",
		map[string]any{"partitions": 6})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	got := readJSON[topic.Topic](t, resp)
	if got.Partitions != 6 {
		t.Errorf("partitions: got %d want 6", got.Partitions)
	}

	resp = getJSON(t, env.Server.URL+"/v1/topics/alter")
	d := readJSON[topic.Details](t, resp)
	if d.Topic.Partitions != 6 {
		t.Errorf("persisted partitions: got %d want 6", d.Topic.Partitions)
	}
	if len(d.Partitions) != 6 {
		t.Errorf("partition stats: got %d want 6", len(d.Partitions))
	}
}

func TestAlterTopic_RejectsPartitionDecrease(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "shrink", Partitions: 4})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/shrink",
		map[string]any{"partitions": 3})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_RejectsPartitionEqual(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "same", Partitions: 4})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/same",
		map[string]any{"partitions": 4})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_RejectsAboveMaxPartitions(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, withPolicy(broker.TopicPolicy{
		DefaultPartitions:  2,
		MaxPartitions:      8,
		DefaultRetentionMs: 3_600_000,
	}))
	mustCreateTopic(t, env, createTopicReq{Name: "cap", Partitions: 4})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/cap",
		map[string]any{"partitions": 36})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_UpdateRetention(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "retention", RetentionMs: 3_600_000})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/retention",
		map[string]any{"retention_ms": int64(7_200_000)})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	got := readJSON[topic.Topic](t, resp)
	if got.RetentionMs != 7_200_000 {
		t.Errorf("retention_ms: got %d want 7200000", got.RetentionMs)
	}
}

// TestAlterTopic_RetentionUpdateReopensPartitionLogs verifies that a
// retention update doesn't break the partition log cache.
func TestAlterTopic_RetentionUpdateReopensPartitionLogs(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "reopen", Partitions: 3})

	mustProduce(t, env, "reopen", "k", map[string]int{"v": 1})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/reopen",
		map[string]any{"retention_ms": int64(3_600_000)})
	expectStatus(t, resp, http.StatusOK)

	mustProduce(t, env, "reopen", "k", map[string]int{"v": 2})
}

func TestAlterTopic_RetentionDefaultsWhenZero(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "default-ret", RetentionMs: 3_600_000})

	// Sending retention_ms=0 should fall back to the broker default.
	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/default-ret",
		map[string]any{"retention_ms": int64(0)})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	got := readJSON[topic.Topic](t, resp)
	if got.RetentionMs != int64(7*24*60*60*1000) {
		t.Errorf("retention_ms: got %d want %d (7-day env default)", got.RetentionMs, int64(7*24*60*60*1000))
	}
}

func TestAlterTopic_RejectsNegativeRetention(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "neg"})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/neg",
		map[string]any{"retention_ms": int64(-100)})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_RejectsNeitherField(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "neither"})

	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/neither",
		map[string]any{})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestAlterTopic_NotFound(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/missing",
		map[string]any{"partitions": 5})
	expectStatus(t, resp, http.StatusNotFound)
}

func TestAlterTopic_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "bad-json"})

	resp := rawReq(t, http.MethodPatch, env.Server.URL+"/v1/topics/bad-json", []byte("{not json}"))
	expectStatus(t, resp, http.StatusBadRequest)
}
