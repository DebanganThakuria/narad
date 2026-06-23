// Package e2e holds HTTP-level end-to-end tests against a real broker
// (SQLite metastore + temp partition logs) exposed via httptest.Server.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	obsmetrics "github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	"github.com/debanganthakuria/narad/internal/transport/httpserver"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	"github.com/prometheus/client_golang/prometheus"
)

const e2eMemberDeadTimeout = 5 * time.Minute

// envOpts lets tests override broker policy values without a fat constructor.
type envOpts struct {
	dataDir                    string
	defaultParts               int
	maxParts                   int
	defaultRF                  int
	defaultRetentionMs         int64
	defaultVisibilityTimeoutMs int64
	maxConsumeWait             time.Duration
	metrics                    bool // when true, wire real Prometheus metrics and /metrics endpoint
	logOptions                 storage.Options
}

func defaultOpts() envOpts {
	return envOpts{
		defaultParts:               4,
		maxParts:                   128,
		defaultRF:                  2,
		defaultRetentionMs:         7 * 24 * 3600 * 1000,
		defaultVisibilityTimeoutMs: 30_000, // 30s — long enough that no e2e test races against expiry
		maxConsumeWait:             5 * time.Second,
		logOptions: storage.Options{
			Codec:         codec.NewNoopCodec(),
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
	Broker broker.Broker
	Server *httptest.Server
	client *http.Client
	ms     metastore.Metastore
	logs   *runtime.Logs
	cancel context.CancelFunc
	done   chan struct{}

	controllerCancel context.CancelFunc
	controllerDone   chan struct{}
	closeOnce        sync.Once

	Registry *prometheus.Registry // non-nil only when metrics:true
	Metrics  *obsmetrics.Metrics  // non-nil only when metrics:true
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	if opts.dataDir == "" {
		opts.dataDir = t.TempDir()
	}
	if err := os.MkdirAll(opts.dataDir, 0o755); err != nil {
		t.Fatalf("create data dir: %v", err)
	}

	ms, err := metastore.New(metastore.Config{
		NodeID:   "test-0",
		DataDir:  filepath.Join(opts.dataDir, "metastore"),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	t.Cleanup(func() { ms.Close() })

	// Wait for Raft to elect a leader before any broker operations.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ms.IsLeader() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ms.IsLeader() {
		t.Fatal("metastore: timed out waiting for Raft leader")
	}
	if err := ms.RegisterMember(context.Background(), metastore.Member{
		ID:            "test-0",
		Addr:          "127.0.0.1:0",
		Status:        metastore.MemberAlive,
		LastHeartbeat: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("register member test-0: %v", err)
	}
	if err := ms.RegisterMember(context.Background(), metastore.Member{
		ID:            "test-1",
		Addr:          "127.0.0.1:1",
		Status:        metastore.MemberAlive,
		LastHeartbeat: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("register member test-1: %v", err)
	}
	if err := ms.RegisterMember(context.Background(), metastore.Member{
		ID:            "test-2",
		Addr:          "127.0.0.1:2",
		Status:        metastore.MemberAlive,
		LastHeartbeat: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("register member test-2: %v", err)
	}
	ctrlCtx, ctrlCancel := context.WithCancel(context.Background())
	ctrlDone := make(chan struct{})
	t.Cleanup(func() {
		ctrlCancel()
		waitDone(t, "controller", ctrlDone)
	})
	go func() {
		defer close(ctrlDone)
		controller.New(ms, controller.Config{
			ReconcileInterval: 50 * time.Millisecond,
			DeadTimeout:       e2eMemberDeadTimeout,
		}).Run(ctrlCtx)
	}()

	logWriter := io.Writer(io.Discard)
	if testing.Verbose() {
		logWriter = os.Stdout
	}
	log := slog.New(slog.NewTextHandler(logWriter, nil))

	var (
		reg *prometheus.Registry
		m   *obsmetrics.Metrics
	)
	if opts.metrics {
		reg = prometheus.NewRegistry()
		m = obsmetrics.New(reg)
	}

	resolveCaps := func(_ context.Context, topicName string) (consumer.Caps, error) {
		t, err := ms.GetTopic(context.Background(), topicName)
		if err != nil {
			return consumer.Caps{}, err
		}
		maxIF := int(t.MaxInFlightPerPartition)
		if maxIF <= 0 {
			maxIF = 1024
		}
		maxAA := int(t.MaxAckedAheadPerPartition)
		if maxAA <= 0 {
			maxAA = 1024
		}
		return consumer.Caps{MaxInFlight: maxIF, MaxAckedAhead: maxAA}, nil
	}

	logs := runtime.NewLogs(opts.dataDir, opts.logOptions, ms, m)
	lifecycle := runtime.NewLifecycle(logs)
	ingressManager, err := ingress.OpenManager(opts.dataDir, ingress.DefaultWALOptions())
	if err != nil {
		t.Fatalf("ingress: %v", err)
	}
	br, err := broker.New(broker.Deps{
		DataDir:        opts.dataDir,
		StorageOptions: opts.logOptions,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions:                opts.defaultParts,
			MaxPartitions:                    opts.maxParts,
			DefaultRetentionMs:               opts.defaultRetentionMs,
			DefaultVisibilityTimeoutMs:       opts.defaultVisibilityTimeoutMs,
			DefaultMaxInFlightPerPartition:   1024,
			DefaultMaxAckedAheadPerPartition: 1024,
		},
		Metastore:       ms,
		Partitions:      partition.NewHashRoundRobin(),
		Schemas:         schema.NewJSONSchema(),
		ConsumerOffsets: consumer.NewInFlight(resolveCaps, nil),
		Logs:            logs,
		Ingress:         ingressManager,
		Logger:          log,
		Lifecycle:       lifecycle,
		Metrics:         m,
	})
	if err != nil {
		t.Fatalf("broker: %v", err)
	}
	lifecycle.MarkReady()
	dispatchCtx, dispatchCancel := context.WithCancel(context.Background())
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		cluster.NewProduceDispatcher(ingressManager, ms, "", br, nil, log, cluster.ProduceDispatcherConfig{
			PollInterval: 5 * time.Millisecond,
		}, m).Run(dispatchCtx)
	}()

	h := handlers.New(handlers.Deps{
		Broker:         br,
		Logs:           logs,
		Logger:         log,
		MaxConsumeWait: opts.maxConsumeWait,
	})

	router := httpserver.NewRouter(h, log, m, reg)
	ts := httptest.NewServer(router)

	return &env{
		t:      t,
		dir:    opts.dataDir,
		broker: br,
		Broker: br,
		Server: ts,
		client: ts.Client(),
		ms:     ms,
		logs:   logs,
		cancel: dispatchCancel,
		done:   dispatchDone,

		controllerCancel: ctrlCancel,
		controllerDone:   ctrlDone,
		Registry:         reg,
		Metrics:          m,
	}
}

func (e *env) close() {
	e.closeOnce.Do(func() {
		if e.Server != nil {
			e.Server.Close()
		}
		if e.cancel != nil {
			e.cancel()
			waitDone(e.t, "produce dispatcher", e.done)
		}
		if e.controllerCancel != nil {
			e.controllerCancel()
			waitDone(e.t, "controller", e.controllerDone)
		}
		if e.broker != nil {
			_ = e.broker.Close()
		}
		if e.ms != nil {
			_ = e.ms.Close()
		}
	})
}

func waitDone(t *testing.T, name string, done <-chan struct{}) {
	t.Helper()
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Errorf("%s did not stop", name)
	}
}

// ---- request helpers -------------------------------------------------------

func (e *env) url(path string) string { return e.Server.URL + path }

func (e *env) post(path string, body any) *http.Response {
	e.t.Helper()
	resp := e.do(http.MethodPost, path, body)
	if path == "/v1/topics" && resp.StatusCode == http.StatusCreated {
		var created topic.Topic
		decodeResp, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err := json.Unmarshal(decodeResp, &created); err == nil {
			if store, ok := e.ms.(*metastore.Store); ok {
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					assignments, err := store.ListAssignments(created.Name)
					if err == nil && len(assignments) == created.Partitions {
						break
					}
					time.Sleep(25 * time.Millisecond)
				}
			}
		}
		resp.Body = io.NopCloser(bytes.NewReader(decodeResp))
	}
	return resp
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
	_ = rf
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

// consume issues a single Consume against `path` (typically built from
// the topic name and any partition/wait params) and returns the parsed
// Message. Fails the test on non-200; use the raw `get` helper if you
// want to inspect a 204 / error.
func (e *env) consume(path string) topic.Message {
	e.t.Helper()
	resp := e.get(path)
	expectOK(e.t, resp)
	return readJSON[topic.Message](e.t, resp)
}

// ack posts a receipt handle against the standard ack endpoint and
// asserts 204. Tests that want to assert a specific error code should
// call env.post directly.
func (e *env) ack(topicName, handle string) {
	e.t.Helper()
	resp := e.post("/v1/topics/"+topicName+"/ack", map[string]any{
		"receipt_handle": handle,
	})
	expectStatus(e.t, resp, http.StatusNoContent)
}

func (e *env) produce(topicName, key, msg string) (offset int64, partition int) {
	e.t.Helper()
	before := topicNextOffsets(e.t, e, topicName)
	resp := e.post(fmt.Sprintf("/v1/topics/%s/produce", topicName), map[string]any{
		"key":     key,
		"message": json.RawMessage(msg),
	})
	expectStatus(e.t, resp, http.StatusAccepted)
	result := readJSON[produceResult](e.t, resp)
	validateAcceptedProduce(e.t, result, topicName, len(before))
	offset = waitForVisibleOffset(e.t, e, topicName, result.Partition, before[result.Partition])
	return offset, result.Partition
}
