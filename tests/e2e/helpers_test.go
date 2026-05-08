// Package e2e runs end-to-end HTTP tests against a fully wired narad
// broker (real metastore, real partition logs on a temp dir) via
// httptest. Each test gets its own isolated environment.
//
// Files in this package are split by HTTP endpoint or feature surface
// — see the file-level docstring on each. Shared infrastructure lives
// in this file.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/httpserver"
	"github.com/debanganthakuria/narad/internal/httpserver/handlers"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/observability/metrics"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
	"github.com/debanganthakuria/narad/internal/storage"
	"github.com/debanganthakuria/narad/internal/topic"
)

// testEnv bundles the test server and broker so tests can hit HTTP
// endpoints and, when necessary, interact with the broker directly.
//
// Metrics is non-nil when the env was built with newTestEnvWithMetrics;
// the /metrics endpoint is mounted only in that mode.
type testEnv struct {
	Server   *httptest.Server
	Broker   broker.Broker
	Metrics  *metrics.Metrics
	Registry *prometheus.Registry
}

// envOpt customises newTestEnv. Tests that need bespoke knobs (small
// SegmentBytes for retention testing, custom default policy, real
// JSON-Schema validator, etc.) pass one or more of these.
type envOpt func(*envConfig)

type envConfig struct {
	policy          broker.TopicPolicy
	storage         storage.Options
	maxConsumeWait  time.Duration
	schemaRegistry  schema.Registry
	enableMetrics   bool
}

func defaultEnvConfig() *envConfig {
	return &envConfig{
		policy: broker.TopicPolicy{
			DefaultPartitions:        4,
			MaxPartitions:            128,
			DefaultReplicationFactor: 2,
			DefaultRetention: topic.Retention{
				MaxAgeMs: int64(24 * time.Hour / time.Millisecond),
			},
		},
		storage: storage.Options{
			Codec:         storage.NewNoopCodec(),
			FlushBytes:    1 << 20,
			FlushRecords:  1000,
			FlushInterval: 50 * time.Millisecond,
			SegmentBytes:  64 << 20,
			Retention: storage.RetentionConfig{
				CheckInterval: time.Minute,
			},
		},
		maxConsumeWait: 2 * time.Second,
		schemaRegistry: schema.NewAlwaysValid(),
	}
}

// withPolicy overrides the broker TopicPolicy for tests that exercise
// partition or retention bounds.
func withPolicy(p broker.TopicPolicy) envOpt {
	return func(c *envConfig) { c.policy = p }
}

// withMaxConsumeWait shortens (or lengthens) the long-poll cap for
// tests that exercise the wait path.
func withMaxConsumeWait(d time.Duration) envOpt {
	return func(c *envConfig) { c.maxConsumeWait = d }
}

// withMetrics enables the metrics surface — registers collectors and
// mounts /metrics. Off by default to keep most tests cheap.
func withMetrics() envOpt {
	return func(c *envConfig) { c.enableMetrics = true }
}

// newTestEnv builds a real broker (SQLite metastore + temp partition
// logs), wires it into the handler set, and returns an httptest server.
// Call any number of envOpts to customise.
func newTestEnv(t *testing.T, opts ...envOpt) *testEnv {
	t.Helper()

	cfg := defaultEnvConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	dataDir := t.TempDir()
	ms, err := metastore.NewSQLiteStore(filepath.Join(dataDir, "metastore", "metadata.db"))
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	// Discard logs by default so test output stays focused. Bump to
	// LevelDebug if a specific test needs to see broker internals.
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	var (
		reg *prometheus.Registry
		m   *metrics.Metrics
	)
	if cfg.enableMetrics {
		reg = prometheus.NewRegistry()
		m = metrics.New(reg)
	}

	br, err := broker.New(broker.Deps{
		DataDir:     dataDir,
		LogOptions:  cfg.storage,
		TopicPolicy: cfg.policy,
		Metastore:   ms,
		Partitions:  partition.NewHashRoundRobin(),
		Schemas:     cfg.schemaRegistry,
		Offsets:     consumer.NewMetastoreBacked(ms),
		Replicator:  replication.NewLocal(),
		Logger:      log,
		Metrics:     m,
	})
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	t.Cleanup(func() { _ = br.Close() })

	h := handlers.New(handlers.Deps{
		Broker:         br,
		Logger:         log,
		MaxConsumeWait: cfg.maxConsumeWait,
	})
	router := httpserver.NewRouter(h, log, m, reg)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	return &testEnv{Server: srv, Broker: br, Metrics: m, Registry: reg}
}

// ---------------------------------------------------------------------------
// HTTP request helpers
// ---------------------------------------------------------------------------

// jsonReq builds a JSON request with the given method/url/body and
// returns the response. body == nil sends an empty body. Tests own
// the response — close the body when done.
func jsonReq(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()

	var rdr *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

// rawReq sends a request with a raw byte body — used for malformed-JSON
// tests where we don't want json.Marshal to clean things up.
func rawReq(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

func getJSON(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// ---------------------------------------------------------------------------
// Body / response helpers
// ---------------------------------------------------------------------------

// readBody drains and returns the body, closing it. Safe to call on a
// nil resp (returns nil).
func readBody(resp *http.Response) []byte {
	if resp == nil {
		return nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return b
}

// decodeJSON closes resp.Body after decoding.
func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

// expectStatus asserts the response status and includes the body in
// the failure message — the body usually carries the actual error.
func expectStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		body := readBody(resp)
		t.Fatalf("status: got %d want %d, body=%s", resp.StatusCode, want, body)
		return
	}
	// Drain so the connection can be reused. Tests that need the body
	// should call decodeJSON or readBody before this.
	_ = resp.Body.Close()
}

// ---------------------------------------------------------------------------
// must* convenience helpers
// ---------------------------------------------------------------------------

// createTopicReq is the input to mustCreateTopic. Zero-valued fields
// fall through to broker defaults.
type createTopicReq struct {
	Name              string          `json:"name"`
	Partitions        int             `json:"partitions,omitempty"`
	ReplicationFactor int             `json:"replication_factor,omitempty"`
	Retention         topic.Retention `json:"retention,omitempty"`
}

func mustCreateTopic(t *testing.T, env *testEnv, req createTopicReq) topic.Topic {
	t.Helper()
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics", req)
	if resp.StatusCode != http.StatusCreated {
		body := readBody(resp)
		t.Fatalf("create topic %q: status %d, body=%s", req.Name, resp.StatusCode, body)
	}
	var out topic.Topic
	decodeJSON(t, resp, &out)
	return out
}

// produceResult is the response shape from POST .../produce.
type produceResult struct {
	Offset    int64 `json:"offset"`
	Partition int   `json:"partition"`
}

// mustProduce sends one message and returns the assigned (offset,
// partition). msg is JSON-serialisable; empty key uses the round-robin
// path.
func mustProduce(t *testing.T, env *testEnv, topicName, key string, msg any) produceResult {
	t.Helper()
	body := map[string]any{"message": msg}
	if key != "" {
		body["key"] = key
	}
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/"+topicName+"/produce", body)
	if resp.StatusCode != http.StatusOK {
		b := readBody(resp)
		t.Fatalf("produce: status %d, body=%s", resp.StatusCode, b)
	}
	var out produceResult
	decodeJSON(t, resp, &out)
	return out
}

// mustAck ackowledges (topic, partition, offset).
func mustAck(t *testing.T, env *testEnv, topicName string, partitionIdx int, offset int64) {
	t.Helper()
	resp := jsonReq(t, http.MethodPost, env.Server.URL+"/v1/topics/"+topicName+"/ack", map[string]any{
		"partition": partitionIdx,
		"offset":    offset,
	})
	expectStatus(t, resp, http.StatusNoContent)
}

// consumeQuery describes optional consume query params.
type consumeQuery struct {
	Partition *int
	Offset    *int64
	Wait      time.Duration
}

func (q consumeQuery) String() string {
	out := ""
	first := true
	add := func(s string) {
		if first {
			out += "?"
			first = false
		} else {
			out += "&"
		}
		out += s
	}
	if q.Partition != nil {
		add(fmt.Sprintf("partition=%d", *q.Partition))
	}
	if q.Offset != nil {
		add(fmt.Sprintf("offset=%d", *q.Offset))
	}
	if q.Wait > 0 {
		add(fmt.Sprintf("wait=%s", q.Wait))
	}
	return out
}

// mustConsume hits the consume endpoint and returns the parsed message
// (when found=true) or an empty struct + found=false on 204.
func mustConsume(t *testing.T, env *testEnv, topicName string, q consumeQuery) (msg topic.Message, found bool) {
	t.Helper()
	resp := getJSON(t, env.Server.URL+"/v1/topics/"+topicName+"/consume"+q.String())
	switch resp.StatusCode {
	case http.StatusNoContent:
		_ = resp.Body.Close()
		return topic.Message{}, false
	case http.StatusOK:
		decodeJSON(t, resp, &msg)
		return msg, true
	default:
		b := readBody(resp)
		t.Fatalf("consume: unexpected status %d, body=%s", resp.StatusCode, b)
		return topic.Message{}, false
	}
}

// intPtr / int64Ptr are small helpers since consumeQuery uses pointers
// to distinguish "not set" from "set to zero".
func intPtr(v int) *int       { return &v }
func int64Ptr(v int64) *int64 { return &v }
