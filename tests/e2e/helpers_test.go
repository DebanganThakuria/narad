// Package e2e holds HTTP-level end-to-end tests against a real broker
// (SQLite metastore + temp partition logs) exposed via httptest.Server.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/httpserver"
	"github.com/debanganthakuria/narad/internal/httpserver/handlers"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
	"github.com/debanganthakuria/narad/internal/storage"
	"github.com/debanganthakuria/narad/internal/topic"
	"github.com/prometheus/client_golang/prometheus"
)

// envOpts lets tests override broker policy values without a fat constructor.
type envOpts struct {
	dataDir            string
	defaultParts       int
	maxParts           int
	defaultRF          int
	defaultRetentionMs int64
	maxConsumeWait     time.Duration
	metrics            bool // when true, wire real Prometheus metrics and /metrics endpoint
	logOptions         storage.Options
}

func defaultOpts() envOpts {
	return envOpts{
		defaultParts:       4,
		maxParts:           128,
		defaultRF:          2,
		defaultRetentionMs: 7 * 24 * 3600 * 1000,
		maxConsumeWait:     5 * time.Second,
		logOptions: storage.Options{
			Codec:         storage.NewNoopCodec(),
			FlushBytes:    1 << 20,
			FlushRecords:  100,
			FlushInterval: 100 * time.Millisecond,
			SegmentBytes:  64 << 20,
			Retention:     storage.RetentionConfig{CheckInterval: time.Hour}, // no reaper in e2e
		},
	}
}

// env bundles a running server, its broker, and a client helper for a
// single test. Call env.close() to clean up.
type env struct {
	t      *testing.T
	dir    string
	broker broker.Broker
	server *httptest.Server
	client *http.Client
	ms     *metastore.SQLiteStore

	reg *prometheus.Registry // non-nil only when metrics:true
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	if opts.dataDir == "" {
		opts.dataDir = t.TempDir()
	}
	if err := os.MkdirAll(opts.dataDir, 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	ms, err := metastore.NewSQLiteStore(filepath.Join(opts.dataDir, "meta.db"))
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	var reg *prometheus.Registry
	if opts.metrics {
		reg = prometheus.NewRegistry()
	}

	br, err := broker.New(broker.Deps{
		DataDir:        opts.dataDir,
		StorageOptions: opts.logOptions,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions:        opts.defaultParts,
			MaxPartitions:            opts.maxParts,
			DefaultReplicationFactor: opts.defaultRF,
			DefaultRetentionMs:       opts.defaultRetentionMs,
		},
		Metastore:       ms,
		Partitions:      partition.NewHashRoundRobin(),
		Schemas:         schema.NewJSONSchema(),
		ConsumerOffsets: consumer.NewInFlight(consumer.NewMetastoreBacked(ms)),
		Replicator:      replication.NewLocal(),
		Logger:          log,
		Metrics:         nil,
	})
	if err != nil {
		t.Fatalf("broker: %v", err)
	}

	h := handlers.New(handlers.Deps{
		Broker:         br,
		Logger:         log,
		MaxConsumeWait: opts.maxConsumeWait,
	})

	router := httpserver.NewRouter(h, log, nil, reg)
	ts := httptest.NewServer(router)

	return &env{
		t:      t,
		dir:    opts.dataDir,
		broker: br,
		server: ts,
		client: ts.Client(),
		ms:     ms,
		reg:    reg,
	}
}

func (e *env) close() {
	e.server.Close()
	_ = e.broker.Close()
	_ = e.ms.Close()
}

// ---- request helpers -------------------------------------------------------

func (e *env) url(path string) string { return e.server.URL + path }

func (e *env) post(path string, body any) *http.Response {
	e.t.Helper()
	return e.do(http.MethodPost, path, body)
}

func (e *env) get(path string) *http.Response {
	e.t.Helper()
	return e.do(http.MethodGet, path, nil)
}

func (e *env) patch(path string, body any) *http.Response {
	e.t.Helper()
	return e.do(http.MethodPatch, path, body)
}

func (e *env) del(path string) *http.Response {
	e.t.Helper()
	return e.do(http.MethodDelete, path, nil)
}

// rawPost sends a raw string body (for testing invalid JSON payloads).
func (e *env) rawPost(path, rawBody string) *http.Response {
	e.t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.url(path), strings.NewReader(rawBody))
	if err != nil {
		e.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatalf("do: %v", err)
	}
	return resp
}

func (e *env) do(method, path string, body any) *http.Response {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			e.t.Fatalf("marshal: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.url(path), r)
	if err != nil {
		e.t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatalf("do: %v", err)
	}
	return resp
}

// ---- response helpers ------------------------------------------------------

func readJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return v
}

func readError(t *testing.T, resp *http.Response) string {
	t.Helper()
	m := readJSON[map[string]string](t, resp)
	return m["error"]
}

func expectStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("status: got %d, want %d (body: %s)", resp.StatusCode, want, string(body))
	}
}

func expectOK(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusOK)
}

func expectBadRequest(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusBadRequest)
}

func expectNotFound(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusNotFound)
}

func expectConflict(t *testing.T, resp *http.Response) {
	t.Helper()
	expectStatus(t, resp, http.StatusConflict)
}

// createTopic is a convenience wrapper. Pass zero partitions/RF/retentionMs to use defaults.
func (e *env) createTopic(name string, partitions, rf int, retentionMs int64) topic.Topic {
	e.t.Helper()
	body := map[string]any{"name": name}
	if partitions > 0 {
		body["partitions"] = partitions
	}
	if rf > 0 {
		body["replication_factor"] = rf
	}
	if retentionMs > 0 {
		body["retention_ms"] = retentionMs
	}
	resp := e.post("/v1/topics", body)
	expectStatus(e.t, resp, http.StatusCreated)
	return readJSON[topic.Topic](e.t, resp)
}

// jsonRaw returns its argument as json.RawMessage. Shorthand for inline
// schema strings in test tables.
func jsonRaw(s string) json.RawMessage { return json.RawMessage(s) }

func (e *env) produce(topicName, key, msg string) (offset int64, partition int) {
	e.t.Helper()
	resp := e.post(fmt.Sprintf("/v1/topics/%s/produce", topicName), map[string]any{
		"key":     key,
		"message": json.RawMessage(msg),
	})
	expectOK(e.t, resp)
	var result struct {
		Offset    int64 `json:"offset"`
		Partition int   `json:"partition"`
	}
	result = readJSON[struct {
		Offset    int64 `json:"offset"`
		Partition int   `json:"partition"`
	}](e.t, resp)
	return result.Offset, result.Partition
}
