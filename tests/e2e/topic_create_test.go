package e2e

import (
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// TestCreateTopic_Defaults verifies that omitting partitions/RF/retention
// pulls them from TopicPolicy.
func TestCreateTopic_Defaults(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name": "defaults",
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	var got topic.Topic
	decodeJSON(t, resp, &got)

	if got.Name != "defaults" {
		t.Errorf("name: got %q want %q", got.Name, "defaults")
	}
	if got.Partitions != 4 {
		t.Errorf("partitions: got %d want 4 (policy default)", got.Partitions)
	}
	if got.RetentionMs == 0 {
		t.Errorf("retention_ms: zero (expected policy default)")
	}
}

// TestCreateTopic_ExplicitValues verifies that user-supplied
// partitions and retention are persisted as-is.
func TestCreateTopic_ExplicitValues(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	got := mustCreateTopic(t, env, createTopicReq{
		Name:        "explicit",
		Partitions:  8,
		RetentionMs: 3_600_000,
	})

	if got.Partitions != 8 {
		t.Errorf("partitions: got %d want 8", got.Partitions)
	}
	if got.RetentionMs != 3_600_000 {
		t.Errorf("retention_ms: got %d want 3600000", got.RetentionMs)
	}
}

func TestCreateTopic_RejectsDuplicateName(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "dup"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{"name": "dup"})
	expectStatus(t, resp, http.StatusConflict)
}

func TestCreateTopic_RejectsEmptyName(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{"name": ""})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsNegativePartitions(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":       "neg-partitions",
		"partitions": -1,
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestCreateTopic_RejectsAboveMaxPartitions exercises the policy bound.
func TestCreateTopic_RejectsAboveMaxPartitions(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t, withPolicy(broker.TopicPolicy{
		DefaultPartitions:  4,
		MaxPartitions:      8,
		DefaultRetentionMs: 1000,
	}))

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":       "too-big",
		"partitions": 9,
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsNegativeRetention(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":         "neg-retention",
		"retention_ms": -1,
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics", []byte("{not json}"))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsUnknownFields(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":    "extra-fields",
		"garbage": "nope",
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestCreateTopic_RejectsOversizedBody confirms the 1MiB body cap is
// rejected with 413 Request Entity Too Large.
func TestCreateTopic_RejectsOversizedBody(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	huge := make([]byte, 0, 2<<20)
	huge = append(huge, []byte(`{"name":"big","extra":"`)...)
	for len(huge) < (1<<20)+1024 {
		huge = append(huge, 'A')
	}
	huge = append(huge, []byte(`"}`)...)

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics", huge)
	expectStatus(t, resp, http.StatusRequestEntityTooLarge)
}
