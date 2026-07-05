package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// ---- topics ----------------------------------------------------------------

// createTopicReq is the input for mustCreateTopic.
type createTopicReq struct {
	Name        string
	Partitions  int
	RetentionMs int64
}

// mustCreateTopic creates a topic and waits until every partition has an
// assignment, so follow-up produces and consumes don't race the controller.
func mustCreateTopic(t *testing.T, e *env, req createTopicReq) topic.Topic {
	t.Helper()

	body := map[string]any{"name": req.Name}
	if req.Partitions > 0 {
		body["partitions"] = req.Partitions
	}
	if req.RetentionMs != 0 {
		body["retention_ms"] = req.RetentionMs
	}
	resp := jsonReq(t, http.MethodPost, e.url("/v1/topics"), body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic %q: got %d body=%s", req.Name, resp.StatusCode, readBody(resp))
	}
	created := readJSON[topic.Topic](t, resp)

	if !e.awaitPartitionAssignments(created.Name, created.Partitions) {
		t.Fatalf("create topic %q: timed out waiting for partition assignments", req.Name)
	}
	return created
}

// createTopic is the env-method flavor of mustCreateTopic. Zero
// partitions/retentionMs fall back to broker defaults.
func (e *env) createTopic(name string, partitions int, retentionMs int64) topic.Topic {
	e.t.Helper()
	return mustCreateTopic(e.t, e, createTopicReq{Name: name, Partitions: partitions, RetentionMs: retentionMs})
}

// awaitPartitionAssignments polls the metastore until topicName has one
// assignment per partition, and reports whether they all appeared within
// the deadline.
func (e *env) awaitPartitionAssignments(topicName string, partitions int) bool {
	e.t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		assignments, err := e.ms.ListAssignments(topicName)
		if err == nil && len(assignments) == partitions {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}

// ---- produce ---------------------------------------------------------------

type produceResult struct {
	Partition int
	Offset    int64
}

// mustProduce produces one JSON-marshaled message and blocks until it is
// visible in a partition log. Produce is asynchronous (202 + ingress WAL),
// so returning at accept time would let follow-up consumes race the
// dispatcher.
func mustProduce(t *testing.T, e *env, topicName, key string, val any) produceResult {
	t.Helper()
	payload, err := json.Marshal(val)
	if err != nil {
		t.Fatalf("marshal produce payload: %v", err)
	}
	return produceAndAwaitVisibility(t, e, topicName, key, payload)
}

// produce is the env-method flavor of mustProduce for raw string payloads.
func (e *env) produce(topicName, key, msg string) (offset int64, partition int) {
	e.t.Helper()
	res := produceAndAwaitVisibility(e.t, e, topicName, key, []byte(msg))
	return res.Offset, res.Partition
}

func produceAndAwaitVisibility(t *testing.T, e *env, topicName, key string, payload []byte) produceResult {
	t.Helper()

	before := topicNextOffsets(t, e, topicName)
	u := e.url("/v1/topics/" + topicName + "/produce")
	if key != "" {
		u += "?key=" + url.QueryEscape(key)
	}
	resp := rawReq(t, http.MethodPost, u, payload)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("produce to %q: got %d body=%s", topicName, resp.StatusCode, readBody(resp))
	}
	resp.Body.Close()

	offset, partitionIdx := waitForAnyVisibleOffset(t, e, topicName, before)
	return produceResult{Partition: partitionIdx, Offset: offset}
}

// topicNextOffsets snapshots NextOffset for every partition; the produce
// helpers diff against it to detect where a new record landed.
func topicNextOffsets(t *testing.T, e *env, topicName string) []int64 {
	t.Helper()
	details, err := e.Broker.GetTopicDetails(context.Background(), topicName)
	if err != nil {
		t.Fatalf("get topic details %q: %v", topicName, err)
	}
	offsets := make([]int64, len(details.Partitions))
	for i, p := range details.Partitions {
		offsets[i] = p.NextOffset
	}
	return offsets
}

// waitForAnyVisibleOffset polls until some partition's high watermark
// moves past its snapshot in previousNext, then returns the offset the
// new record landed at and its partition.
func waitForAnyVisibleOffset(t *testing.T, e *env, topicName string, previousNext []int64) (int64, int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		details, err := e.Broker.GetTopicDetails(context.Background(), topicName)
		if err != nil {
			t.Fatalf("get topic details %q: %v", topicName, err)
		}
		for partitionIdx, before := range previousNext {
			if partitionIdx < len(details.Partitions) && details.Partitions[partitionIdx].HighWatermark > before {
				return before, partitionIdx
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for produce visibility topic=%q", topicName)
	return 0, 0
}

// waitForVisibleDelta polls until at least want records (summed across
// partitions) became visible since the previousNext snapshot.
func waitForVisibleDelta(t *testing.T, e *env, topicName string, previousNext []int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		details, err := e.Broker.GetTopicDetails(context.Background(), topicName)
		if err != nil {
			t.Fatalf("get topic details %q: %v", topicName, err)
		}
		var got int64
		for partitionIdx, before := range previousNext {
			if partitionIdx < len(details.Partitions) && details.Partitions[partitionIdx].HighWatermark > before {
				got += details.Partitions[partitionIdx].HighWatermark - before
			}
		}
		if got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d visible produced messages topic=%q", want, topicName)
}

// ---- consume / ack ---------------------------------------------------------

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

	query := url.Values{}
	if q.Partition != nil {
		query.Set("partition", strconv.Itoa(*q.Partition))
	}
	if q.Offset != nil {
		query.Set("offset", strconv.FormatInt(*q.Offset, 10))
	}
	if q.Wait != "" {
		query.Set("wait", q.Wait)
	}
	u := e.url("/v1/topics/" + topicName + "/consume")
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	resp := getJSON(t, u)
	if resp.StatusCode == http.StatusNoContent {
		resp.Body.Close()
		return topic.Message{}, false
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("consume from %q: got %d body=%s", topicName, resp.StatusCode, readBody(resp))
	}
	return readJSON[topic.Message](t, resp), true
}

// consume issues a single Consume against path (typically built from the
// topic name and any partition/wait params) and returns the parsed
// Message. Fails the test on non-200; use the raw get helper to inspect
// a 204 or an error status.
func (e *env) consume(path string) topic.Message {
	e.t.Helper()
	resp := e.get(path)
	expectOK(e.t, resp)
	return readJSON[topic.Message](e.t, resp)
}

// mustAck acks a message by its receipt handle and fatals on non-204.
func mustAck(t *testing.T, e *env, topicName, receiptHandle string) {
	t.Helper()
	resp := jsonReq(t, http.MethodPost, e.url("/v1/topics/"+topicName+"/ack?receipt_handle="+url.QueryEscape(receiptHandle)), nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("ack on %q: got %d body=%s", topicName, resp.StatusCode, readBody(resp))
	}
	resp.Body.Close()
}

// ack is the env-method flavor of mustAck.
func (e *env) ack(topicName, receiptHandle string) {
	e.t.Helper()
	mustAck(e.t, e, topicName, receiptHandle)
}

// ---- schema fixtures -------------------------------------------------------

const schemaV1 = `{
  "type": "object",
  "properties": {
    "id":    { "type": "integer" },
    "name":  { "type": "string" }
  },
  "required": ["id"]
}`

// schemaV2Additive only adds an optional field — a compatible evolution.
const schemaV2Additive = `{
  "type": "object",
  "properties": {
    "id":    { "type": "integer" },
    "name":  { "type": "string" },
    "email": { "type": "string" }
  },
  "required": ["id"]
}`

// schemaV2TypeChange flips id from integer to string — incompatible.
const schemaV2TypeChange = `{
  "type": "object",
  "properties": {
    "id":   { "type": "string" },
    "name": { "type": "string" }
  },
  "required": ["id"]
}`

// schemaV2RemoveField drops the name property — incompatible.
const schemaV2RemoveField = `{
  "type": "object",
  "properties": {
    "id": { "type": "integer" }
  },
  "required": ["id"]
}`
