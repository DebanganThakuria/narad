package e2e

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
)

func TestInternalReplicate_AppendsAndReplicaReadReturnsPayload(t *testing.T) {
	env := newTestEnv(t)
	created := mustCreateTopic(t, env, createTopicReq{Name: "replicate-ok", Partitions: 3, ReplicationFactor: 2})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/internal/v1/replicate", []byte(`{"topic":"`+created.Name+`","partition":0,"offset":0,"payload":"eyJoZWxsbyI6InJlcGxpY2EifQ==","leader_id":"test-0"}`))
	expectStatus(t, resp, http.StatusNoContent)

	readURL := env.Server.URL + "/internal/v1/replicate?topic=" + url.QueryEscape(created.Name) + "&partition=0&offset=0"
	readResp := getJSON(t, readURL)
	expectStatus(t, readResp, http.StatusOK)

	var out struct {
		Payload json.RawMessage `json:"payload"`
	}
	decodeJSON(t, readResp, &out)
	if string(out.Payload) != `"eyJoZWxsbyI6InJlcGxpY2EifQ=="` {
		t.Fatalf("payload: got %s want %s", string(out.Payload), `"eyJoZWxsbyI6InJlcGxpY2EifQ=="`)
	}
}

func TestInternalReplicate_CommittedReadSeesReplicatedPayload(t *testing.T) {
	env := newTestEnv(t)
	created := mustCreateTopic(t, env, createTopicReq{Name: "replicate-committed", Partitions: 3, ReplicationFactor: 2})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/internal/v1/replicate", []byte(`{"topic":"`+created.Name+`","partition":0,"offset":0,"payload":"eyJoZWxsbyI6InJlcGxpY2EifQ==","leader_id":"test-0"}`))
	expectStatus(t, resp, http.StatusNoContent)

	readURL := env.Server.URL + "/internal/v1/replicate?topic=" + url.QueryEscape(created.Name) + "&partition=0&offset=0&committed=true"
	readResp := getJSON(t, readURL)
	expectStatus(t, readResp, http.StatusOK)

	var out struct {
		Payload []byte `json:"payload"`
	}
	decodeJSON(t, readResp, &out)
	if string(out.Payload) != `{"hello":"replica"}` {
		t.Fatalf("payload: got %s want %s", string(out.Payload), `{"hello":"replica"}`)
	}
}

func TestInternalReplicate_RejectsOffsetMismatch(t *testing.T) {
	env := newTestEnv(t)
	created := mustCreateTopic(t, env, createTopicReq{Name: "replicate-conflict", Partitions: 3, ReplicationFactor: 2})

	first := rawReq(t, http.MethodPost, env.Server.URL+"/internal/v1/replicate", []byte(`{"topic":"`+created.Name+`","partition":0,"offset":0,"payload":"eyJuIjoxfQ==","leader_id":"test-0"}`))
	expectStatus(t, first, http.StatusNoContent)

	conflict := rawReq(t, http.MethodPost, env.Server.URL+"/internal/v1/replicate", []byte(`{"topic":"`+created.Name+`","partition":0,"offset":0,"payload":"eyJuIjoyfQ==","leader_id":"test-0"}`))
	expectStatus(t, conflict, http.StatusConflict)
}

func TestInternalReplicate_RejectsInvalidPayload(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "replicate-invalid", Partitions: 3, ReplicationFactor: 2})

	resp := rawReq(t, http.MethodPost, env.Server.URL+"/internal/v1/replicate", []byte(`{"topic":"replicate-invalid","partition":0,"offset":0,"payload":not-json,"leader_id":"test-0"}`))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestReplicaRead_ReturnsNotFoundForMissingOffset(t *testing.T) {
	env := newTestEnv(t)
	created := mustCreateTopic(t, env, createTopicReq{Name: "replica-missing", Partitions: 3, ReplicationFactor: 2})

	resp := getJSON(t, env.Server.URL+"/internal/v1/replicate?topic="+url.QueryEscape(created.Name)+"&partition=0&offset=0")
	expectStatus(t, resp, http.StatusNotFound)
}

func TestReplicaRead_RejectsInvalidQueryParams(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "replica-bad-query", Partitions: 3, ReplicationFactor: 2})

	cases := []struct {
		name string
		url  string
	}{
		{name: "missing topic", url: env.Server.URL + "/internal/v1/replicate?partition=0&offset=0"},
		{name: "bad partition", url: env.Server.URL + "/internal/v1/replicate?topic=replica-bad-query&partition=-1&offset=0"},
		{name: "bad offset", url: env.Server.URL + "/internal/v1/replicate?topic=replica-bad-query&partition=0&offset=-1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := getJSON(t, tc.url)
			expectStatus(t, resp, http.StatusBadRequest)
		})
	}
}
