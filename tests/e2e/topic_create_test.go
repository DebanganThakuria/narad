package e2e

import (
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/topic"
)

// TestCreateTopic_Defaults verifies that omitting partitions/RF/retention
// pulls them from TopicPolicy.
func TestCreateTopic_Defaults(t *testing.T) {
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
	if got.ReplicationFactor != 2 {
		t.Errorf("replication_factor: got %d want 2 (policy default)", got.ReplicationFactor)
	}
	if got.Retention.MaxAgeMs == 0 {
		t.Errorf("retention.max_age_ms: zero (expected policy default 24h)")
	}
}

// TestCreateTopic_ExplicitValues verifies that user-supplied
// partitions, replication factor, and retention are persisted as-is.
func TestCreateTopic_ExplicitValues(t *testing.T) {
	env := newTestEnv(t)

	got := mustCreateTopic(t, env, createTopicReq{
		Name:              "explicit",
		Partitions:        8,
		ReplicationFactor: 3,
		Retention: topic.Retention{
			MaxAgeMs: 3_600_000,
			MaxBytes: 1 << 30,
		},
	})

	if got.Partitions != 8 {
		t.Errorf("partitions: got %d want 8", got.Partitions)
	}
	if got.ReplicationFactor != 3 {
		t.Errorf("replication_factor: got %d want 3", got.ReplicationFactor)
	}
	if got.Retention.MaxAgeMs != 3_600_000 || got.Retention.MaxBytes != 1<<30 {
		t.Errorf("retention: got %+v want {3600000, 1GiB}", got.Retention)
	}
}

// TestCreateTopic_PartialRetention verifies that providing only one of
// {max_age_ms, max_bytes} fills the other axis from defaults.
func TestCreateTopic_PartialRetention(t *testing.T) {
	env := newTestEnv(t)

	got := mustCreateTopic(t, env, createTopicReq{
		Name:      "partial-retention",
		Retention: topic.Retention{MaxBytes: 4096},
	})

	if got.Retention.MaxBytes != 4096 {
		t.Errorf("max_bytes: got %d want 4096", got.Retention.MaxBytes)
	}
	if got.Retention.MaxAgeMs == 0 {
		t.Errorf("max_age_ms: 0 (expected policy default to fill in)")
	}
}

func TestCreateTopic_RejectsDuplicateName(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "dup"})

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{"name": "dup"})
	expectStatus(t, resp, http.StatusConflict)
}

func TestCreateTopic_RejectsEmptyName(t *testing.T) {
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{"name": ""})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsNegativePartitions(t *testing.T) {
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":       "neg-partitions",
		"partitions": -1,
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestCreateTopic_RejectsAboveMaxPartitions exercises the policy bound.
func TestCreateTopic_RejectsAboveMaxPartitions(t *testing.T) {
	env := newTestEnv(t, withPolicy(broker.TopicPolicy{
		DefaultPartitions:        4,
		MaxPartitions:            8,
		DefaultReplicationFactor: 2,
		DefaultRetention:         topic.Retention{MaxAgeMs: 1000},
	}))

	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":       "too-big",
		"partitions": 9,
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsRFBelowTwo(t *testing.T) {
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":               "rf-1",
		"replication_factor": 1,
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsNegativeRetention(t *testing.T) {
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name": "neg-retention",
		"retention": map[string]any{
			"max_age_ms": -1,
		},
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsInvalidJSON(t *testing.T) {
	env := newTestEnv(t)
	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics", []byte("{not json}"))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestCreateTopic_RejectsUnknownFields(t *testing.T) {
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", map[string]any{
		"name":    "extra-fields",
		"garbage": "nope",
	})
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestCreateTopic_RejectsOversizedBody confirms the 1MiB body cap.
// MaxBytesReader fires inside the JSON decoder, surfacing as 400.
func TestCreateTopic_RejectsOversizedBody(t *testing.T) {
	env := newTestEnv(t)

	// Build a > 1MiB payload by stuffing a string field. The
	// "garbage" field is unknown so this would also fail
	// DisallowUnknownFields, but MaxBytesReader trips first because
	// Decode reads bytes before checking field names.
	huge := make([]byte, 0, 2<<20)
	huge = append(huge, []byte(`{"name":"big","extra":"`)...)
	for len(huge) < (1<<20)+1024 {
		huge = append(huge, 'A')
	}
	huge = append(huge, []byte(`"}`)...)

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/v1/topics", huge)
	expectStatus(t, resp, http.StatusBadRequest)
}
