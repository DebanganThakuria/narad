// Package e2e holds HTTP-level end-to-end tests against a real broker
// (SQLite metastore + temp partition logs) exposed via httptest.Server.
package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
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

// e2eMemberDeadTimeout is deliberately generous so the controller never
// declares the fake cluster members dead mid-test.
const e2eMemberDeadTimeout = 5 * time.Minute

// envOpts lets tests override broker policy values without a fat constructor.
type envOpts struct {
	defaultParts               int
	maxParts                   int
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
		if p.DefaultRetentionMs > 0 {
			o.defaultRetentionMs = p.DefaultRetentionMs
		}
	}
}

func withMaxConsumeWait(d time.Duration) envOption {
	return func(o *envOpts) { o.maxConsumeWait = d }
}

// env bundles a running server, its broker, and request helpers for a
// single test. Call env.close() to clean up.
type env struct {
	t      *testing.T
	Broker broker.Broker
	Server *httptest.Server
	ms     *metastore.Store

	dispatcherCancel context.CancelFunc
	dispatcherDone   chan struct{}
	controllerCancel context.CancelFunc
	controllerDone   chan struct{}
	closeOnce        sync.Once

	Registry *prometheus.Registry // non-nil only when metrics:true
	Metrics  *obsmetrics.Metrics  // non-nil only when metrics:true
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

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	dataDir := t.TempDir()
	ms := startMetastore(t, dataDir)
	controllerCancel, controllerDone := startController(t, ms)
	log := newTestLogger()

	var (
		reg *prometheus.Registry
		m   *obsmetrics.Metrics
	)
	if opts.metrics {
		reg = prometheus.NewRegistry()
		m = obsmetrics.New(reg)
	}

	logs := runtime.NewLogs(dataDir, opts.logOptions, ms, m)
	lifecycle := runtime.NewLifecycle(logs)
	ingressManager, err := ingress.OpenManager(dataDir, ingress.DefaultWALOptions())
	if err != nil {
		t.Fatalf("ingress: %v", err)
	}
	br, err := broker.New(broker.Deps{
		DataDir:        dataDir,
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
		ConsumerOffsets: consumer.NewInFlight(capsResolver(ms), nil),
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

	dispatcherCtx, dispatcherCancel := context.WithCancel(context.Background())
	dispatcherDone := make(chan struct{})
	go func() {
		defer close(dispatcherDone)
		cluster.NewProduceDispatcher(ingressManager, ms, "", br, nil, log, cluster.ProduceDispatcherConfig{
			PollInterval: 5 * time.Millisecond,
		}).Run(dispatcherCtx)
	}()

	router := httpserver.NewRouter(handlers.New(handlers.Deps{
		Broker:         br,
		Logs:           logs,
		Logger:         log,
		MaxConsumeWait: opts.maxConsumeWait,
	}), log, m, reg, nil)

	return &env{
		t:                t,
		Broker:           br,
		Server:           httptest.NewServer(router),
		ms:               ms,
		dispatcherCancel: dispatcherCancel,
		dispatcherDone:   dispatcherDone,
		controllerCancel: controllerCancel,
		controllerDone:   controllerDone,
		Registry:         reg,
		Metrics:          m,
	}
}

// startMetastore boots a single-node Raft metastore, waits for it to
// elect itself leader (topic operations fail on a leaderless store), and
// registers three alive members so replica placement has somewhere to go.
func startMetastore(t *testing.T, dataDir string) *metastore.Store {
	t.Helper()

	ms, err := metastore.New(metastore.Config{
		NodeID:   "test-0",
		DataDir:  filepath.Join(dataDir, "metastore"),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	t.Cleanup(func() { ms.Close() })

	deadline := time.Now().Add(5 * time.Second)
	for !ms.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("metastore: timed out waiting for Raft leader")
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i, addr := range []string{"127.0.0.1:0", "127.0.0.1:1", "127.0.0.1:2"} {
		member := metastore.Member{
			ID:            fmt.Sprintf("test-%d", i),
			Addr:          addr,
			Status:        metastore.MemberAlive,
			LastHeartbeat: time.Now().Unix(),
		}
		if err := ms.RegisterMember(context.Background(), member); err != nil {
			t.Fatalf("register member %s: %v", member.ID, err)
		}
	}
	return ms
}

// startController runs the cluster reconcile loop so freshly created
// topics get partition assignments. The t.Cleanup here is a backstop for
// tests that fatal before an env is fully constructed; env.close performs
// the same shutdown in the normal path.
func startController(t *testing.T, ms *metastore.Store) (context.CancelFunc, chan struct{}) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		waitDone(t, "controller", done)
	})
	go func() {
		defer close(done)
		controller.New(ms, controller.Config{
			ReconcileInterval: 50 * time.Millisecond,
			DeadTimeout:       e2eMemberDeadTimeout,
		}).Run(ctx)
	}()
	return cancel, done
}

// capsResolver adapts topic records in the metastore to consumer caps,
// substituting the broker default (1024) when a topic sets none.
func capsResolver(ms metastore.Metastore) func(context.Context, string) (consumer.Caps, error) {
	return func(_ context.Context, topicName string) (consumer.Caps, error) {
		rec, err := ms.GetTopic(context.Background(), topicName)
		if err != nil {
			return consumer.Caps{}, err
		}
		caps := consumer.Caps{
			MaxInFlight:   int(rec.MaxInFlightPerPartition),
			MaxAckedAhead: int(rec.MaxAckedAheadPerPartition),
		}
		if caps.MaxInFlight <= 0 {
			caps.MaxInFlight = 1024
		}
		if caps.MaxAckedAhead <= 0 {
			caps.MaxAckedAhead = 1024
		}
		return caps, nil
	}
}

func newTestLogger() *slog.Logger {
	w := io.Writer(io.Discard)
	if testing.Verbose() {
		w = os.Stdout
	}
	return slog.New(slog.NewTextHandler(w, nil))
}

func (e *env) close() {
	e.closeOnce.Do(func() {
		if e.Server != nil {
			e.Server.Close()
		}
		if e.dispatcherCancel != nil {
			e.dispatcherCancel()
			waitDone(e.t, "produce dispatcher", e.dispatcherDone)
		}
		if e.controllerCancel != nil {
			e.controllerCancel()
			waitDone(e.t, "controller", e.controllerDone)
		}
		if e.Broker != nil {
			_ = e.Broker.Close()
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
