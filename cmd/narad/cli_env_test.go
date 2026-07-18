package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

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
)

// cliTestEnv is a single-node broker behind a real HTTP server, so the
// client subcommands are exercised end to end.
type cliTestEnv struct {
	server *httptest.Server
	broker broker.Broker
	store  *metastore.Store
}

func newCLITestEnv(t *testing.T) *cliTestEnv {
	t.Helper()
	dataDir := t.TempDir()
	store, err := metastore.New(metastore.Config{
		NodeID:   "test-0",
		DataDir:  filepath.Join(dataDir, "metastore"),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore: %v", err)
	}
	waitForLeadership(t, store)
	for _, member := range []metastore.Member{
		{ID: "test-0", Addr: "127.0.0.1:0", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "test-1", Addr: "127.0.0.1:1", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
		{ID: "test-2", Addr: "127.0.0.1:2", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()},
	} {
		if err := store.RegisterMember(context.Background(), member); err != nil {
			t.Fatalf("register member %s: %v", member.ID, err)
		}
	}

	ctrlCtx, ctrlCancel := context.WithCancel(context.Background())
	go controller.New(store, controller.Config{ReconcileInterval: 50 * time.Millisecond, DeadTimeout: 5 * time.Second}).Run(ctrlCtx)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := prometheus.NewRegistry()
	metrics := obsmetrics.New(reg)
	storageOptions := storage.Options{
		Codec:         mustZstdCodec(t),
		FlushBytes:    1 << 20,
		FlushRecords:  100,
		FlushInterval: 100 * time.Millisecond,
		SegmentBytes:  64 << 20,
		Retention:     storage.RetentionConfig{CheckInterval: time.Hour},
	}
	logs := runtime.NewLogs(dataDir, storageOptions, store, metrics)
	lifecycle := runtime.NewLifecycle(logs)
	ingressManager, err := ingress.OpenManager(dataDir, ingress.DefaultWALOptions())
	if err != nil {
		ctrlCancel()
		store.Close()
		t.Fatalf("ingress: %v", err)
	}
	resolveCaps := func(_ context.Context, topicName string) (consumer.Caps, error) {
		topicCfg, err := store.GetTopic(context.Background(), topicName)
		if err != nil {
			return consumer.Caps{}, err
		}
		maxIF := int(topicCfg.MaxInFlightPerPartition)
		if maxIF <= 0 {
			maxIF = 1024
		}
		maxAA := int(topicCfg.MaxAckedAheadPerPartition)
		if maxAA <= 0 {
			maxAA = 1024
		}
		return consumer.Caps{MaxInFlight: maxIF, MaxAckedAhead: maxAA}, nil
	}
	br, err := broker.New(broker.Deps{
		DataDir:        dataDir,
		StorageOptions: storageOptions,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions:                3,
			MaxPartitions:                    1024,
			DefaultRetentionMs:               7 * 24 * 3600 * 1000,
			DefaultVisibilityTimeoutMs:       30_000,
			DefaultMaxInFlightPerPartition:   1024,
			DefaultMaxAckedAheadPerPartition: 1024,
		},
		Metastore:       store,
		Partitions:      partition.NewHashRoundRobin(),
		Schemas:         schema.NewJSONSchema(),
		ConsumerOffsets: consumer.NewInFlight(resolveCaps, nil),
		Logger:          log,
		Logs:            logs,
		Ingress:         ingressManager,
		Lifecycle:       lifecycle,
		Metrics:         metrics,
	})
	if err != nil {
		ctrlCancel()
		store.Close()
		t.Fatalf("broker: %v", err)
	}
	lifecycle.MarkReady()
	dispatchCtx, dispatchCancel := context.WithCancel(context.Background())
	go cluster.NewProduceDispatcher(ingressManager, store, "", br, nil, log, cluster.ProduceDispatcherConfig{
		PollInterval: 5 * time.Millisecond,
	}).Run(dispatchCtx)
	server := httptest.NewServer(httpserver.NewRouter(handlers.New(handlers.Deps{
		Broker:         br,
		Logs:           logs,
		Logger:         log,
		MaxConsumeWait: 5 * time.Second,
	}), log, metrics, reg, nil))

	t.Cleanup(func() {
		dispatchCancel()
		server.Close()
		_ = br.Close()
		ctrlCancel()
		_ = store.Close()
	})
	return &cliTestEnv{server: server, broker: br, store: store}
}

// waitForAssignments blocks until the controller has assigned every
// partition of the topic, so consume/produce paths have owners.
func waitForAssignments(store *metastore.Store, topicName string) error {
	cfg, err := store.GetTopic(context.Background(), topicName)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		assignments, err := store.ListAssignments(topicName)
		if err == nil && len(assignments) == cfg.Partitions {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func waitForAnyVisibleOffset(t *testing.T, br broker.Broker, topicName string, previousNext []int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		details, err := br.GetTopicDetails(context.Background(), topicName)
		if err != nil {
			t.Fatalf("get topic details %q: %v", topicName, err)
		}
		for partition, before := range previousNext {
			if partition < len(details.Partitions) &&
				details.Partitions[partition].HighWatermark > before {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for visible offset topic=%q", topicName)
}

// captureCLIOutput runs fn with stdin fed from the given string and
// returns what it wrote to stdout and stderr.
func captureCLIOutput(t *testing.T, fn func() error, stdin string) (string, string, error) {
	t.Helper()
	oldStdout, oldStderr, oldStdin := os.Stdout, os.Stderr, os.Stdin
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stderr pipe: %v", err)
	}
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	if _, err := io.WriteString(stdinW, stdin); err != nil {
		t.Fatalf("stdin write: %v", err)
	}
	stdinW.Close()
	os.Stdout, os.Stderr, os.Stdin = stdoutW, stderrW, stdinR
	callErr := fn()
	stdoutW.Close()
	stderrW.Close()
	os.Stdout, os.Stderr, os.Stdin = oldStdout, oldStderr, oldStdin

	var stdoutBuf, stderrBuf bytes.Buffer
	if _, err := io.Copy(&stdoutBuf, stdoutR); err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	if _, err := io.Copy(&stderrBuf, stderrR); err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	return stdoutBuf.String(), stderrBuf.String(), callErr
}

func decodeCLIJSON[T any](t *testing.T, out string) T {
	t.Helper()
	var v T
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatalf("decode CLI json %q: %v", out, err)
	}
	return v
}

func mustZstdCodec(t *testing.T) codec.Codec {
	t.Helper()
	c, err := codec.NewZstdCodec(zstd.SpeedBestCompression)
	if err != nil {
		t.Fatalf("zstd codec: %v", err)
	}
	return c
}
