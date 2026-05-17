package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/replication"
)

// envOption tunes the env returned by newTestEnv.
type envOption func(*envOpts)

func withMetrics() envOption {
	return func(o *envOpts) { o.metrics = true }
}

func withPolicy(p broker.TopicPolicy) envOption {
	return func(o *envOpts) {
		if p.DefaultPartitions > 0 {
			o.defaultParts = p.DefaultPartitions
		}
		if p.MaxPartitions > 0 {
			o.maxParts = p.MaxPartitions
		}
		if p.DefaultReplicationFactor > 0 {
			o.defaultRF = p.DefaultReplicationFactor
		}
		if p.DefaultRetentionMs > 0 {
			o.defaultRetentionMs = p.DefaultRetentionMs
		}
	}
}

func withMaxConsumeWait(d time.Duration) envOption {
	return func(o *envOpts) { o.maxConsumeWait = d }
}

func withReplicatorFactory(f func(*metastore.Store, *http.Client) replication.Replicator) envOption {
	return func(o *envOpts) { o.replicatorFactory = f }
}

// newTestEnv builds an env with t.Cleanup wired for close.
func newTestEnv(t *testing.T, opts ...envOption) *env {
	t.Helper()
	o := defaultOpts()
	for _, opt := range opts {
		opt(&o)
	}
	e := newEnv(t, o)
	t.Cleanup(e.close)
	return e
}

// createTopicReq is the input for mustCreateTopic.
type createTopicReq struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	RetentionMs       int64
}

// mustCreateTopic creates a topic and fatals if the server rejects it.
func mustCreateTopic(t *testing.T, e *env, req createTopicReq) topic.Topic {
	t.Helper()
	body := map[string]any{"name": req.Name}
	if req.Partitions > 0 {
		body["partitions"] = req.Partitions
	}
	if req.ReplicationFactor > 0 {
		body["replication_factor"] = req.ReplicationFactor
	}
	if req.RetentionMs != 0 {
		body["retention_ms"] = req.RetentionMs
	}
	resp := jsonReq(t, http.MethodPost, e.Server.URL+"/v1/topics", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("mustCreateTopic %q: got %d body=%s", req.Name, resp.StatusCode, readBody(resp))
	}
	var out topic.Topic
	decodeJSON(t, resp, &out)

	store, ok := e.ms.(*metastore.Store)
	if !ok {
		t.Fatalf("unexpected metastore type %T", e.ms)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		assignments, err := store.ListAssignments(req.Name)
		if err == nil && len(assignments) == out.Partitions {
			return out
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("mustCreateTopic %q: timed out waiting for partition assignments", req.Name)
	return topic.Topic{}
}

// produceResult holds the fields returned by the produce endpoint.
type produceResult struct {
	Offset    int64 `json:"offset"`
	Partition int   `json:"partition"`
}

// mustProduce produces a single message and fatals on error.
func mustProduce(t *testing.T, e *env, topicName, key string, val any) produceResult {
	t.Helper()
	payload, err := json.Marshal(val)
	if err != nil {
		t.Fatalf("mustProduce marshal: %v", err)
	}
	resp := jsonReq(t, http.MethodPost, e.Server.URL+"/v1/topics/"+topicName+"/produce",
		map[string]any{"key": key, "message": json.RawMessage(payload)})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mustProduce %q: got %d body=%s", topicName, resp.StatusCode, readBody(resp))
	}
	var out produceResult
	decodeJSON(t, resp, &out)
	return out
}

// jsonReq sends a JSON-encoded body with the given method and URL.
func jsonReq(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("jsonReq marshal: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatalf("jsonReq: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("jsonReq do: %v", err)
	}
	return resp
}

// getJSON issues a GET and returns the response.
func getJSON(t *testing.T, url string) *http.Response {
	t.Helper()
	return jsonReq(t, http.MethodGet, url, nil)
}

// rawReq sends raw bytes with Content-Type: application/json.
func rawReq(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("rawReq: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rawReq do: %v", err)
	}
	return resp
}

// readBody drains and returns the response body as a string.
func readBody(resp *http.Response) string {
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// decodeJSON decodes the response body into out.
func decodeJSON[T any](t *testing.T, resp *http.Response, out *T) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decodeJSON: %v", err)
	}
}

// consumeQuery holds optional query parameters for mustConsume.
type consumeQuery struct {
	Partition *int
	Offset    *int64
	Wait      string
}

// mustConsume issues a GET /v1/topics/{topic}/consume with optional query
// parameters. Returns (message, true) on 200, (zero, false) on 204.
func mustConsume(t *testing.T, e *env, topicName string, q consumeQuery) (topic.Message, bool) {
	t.Helper()
	u := e.Server.URL + "/v1/topics/" + topicName + "/consume"
	sep := "?"
	if q.Partition != nil {
		u += sep + "partition=" + itoa(*q.Partition)
		sep = "&"
	}
	if q.Offset != nil {
		u += sep + "offset=" + i64toa(*q.Offset)
		sep = "&"
	}
	if q.Wait != "" {
		u += sep + "wait=" + q.Wait
	}
	resp := getJSON(t, u)
	if resp.StatusCode == http.StatusNoContent {
		resp.Body.Close()
		return topic.Message{}, false
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mustConsume %q: got %d body=%s", topicName, resp.StatusCode, readBody(resp))
	}
	var msg topic.Message
	decodeJSON(t, resp, &msg)
	return msg, true
}

// mustAck acks a message by its receipt handle and fatals on non-204.
func mustAck(t *testing.T, e *env, topicName, receiptHandle string) {
	t.Helper()
	resp := jsonReq(t, http.MethodPost, e.Server.URL+"/v1/topics/"+topicName+"/ack",
		map[string]any{"receipt_handle": receiptHandle})
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("mustAck %q: got %d body=%s", topicName, resp.StatusCode, readBody(resp))
	}
}

func intPtr(n int) *int       { return &n }
func int64Ptr(n int64) *int64 { return &n }

func itoa(n int) string     { return strconv.Itoa(n) }
func i64toa(n int64) string { return strconv.FormatInt(n, 10) }
