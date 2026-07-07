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
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/broker/topics"
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

func TestClientTopicsCreateListGetAlterDeleteParity(t *testing.T) {
	env := newCLITestEnv(t)

	stdout, stderr, err := captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "create", "--addr", env.server.URL, "--partitions", "3", "--retention-ms", "3600000", "orders"})
	}, "")
	if err != nil {
		t.Fatalf("topics create: %v", err)
	}
	if stderr != "" {
		t.Fatalf("topics create stderr = %q, want empty", stderr)
	}
	created := decodeCLIJSON[topic.Topic](t, stdout)
	if created.Name != "orders" || created.Partitions != 3 || created.RetentionMs != 3600000 {
		t.Fatalf("created topic = %+v", created)
	}
	if err := waitForAssignments(env.store, "orders"); err != nil {
		t.Fatalf("wait assignments: %v", err)
	}

	stdout, stderr, err = captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "list", "--addr", env.server.URL})
	}, "")
	if err != nil {
		t.Fatalf("topics list: %v", err)
	}
	if stderr != "" {
		t.Fatalf("topics list stderr = %q, want empty", stderr)
	}
	listed := decodeCLIJSON[struct {
		Topics []topic.Topic `json:"topics"`
	}](t, stdout)
	if len(listed.Topics) != 1 || listed.Topics[0].Name != "orders" {
		t.Fatalf("listed topics = %+v", listed.Topics)
	}

	stdout, stderr, err = captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "get", "--addr", env.server.URL, "orders"})
	}, "")
	if err != nil {
		t.Fatalf("topics get: %v", err)
	}
	if stderr != "" {
		t.Fatalf("topics get stderr = %q, want empty", stderr)
	}
	details := decodeCLIJSON[topic.Details](t, stdout)
	if details.Topic.Name != "orders" || len(details.Partitions) != 3 {
		t.Fatalf("topic details = %+v", details)
	}

	stdout, stderr, err = captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "alter", "--addr", env.server.URL, "--retention-ms", "0", "orders"})
	}, "")
	if err != nil {
		t.Fatalf("topics alter: %v", err)
	}
	if stderr != "" {
		t.Fatalf("topics alter stderr = %q, want empty", stderr)
	}
	altered := decodeCLIJSON[topic.Topic](t, stdout)
	if altered.RetentionMs == 0 {
		t.Fatalf("altered retention = %d, want defaulted non-zero value", altered.RetentionMs)
	}

	stdout, stderr, err = captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "delete", "--addr", env.server.URL, "orders"})
	}, "")
	if err != nil {
		t.Fatalf("topics delete: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("topics delete stdout = %q, want empty", stdout)
	}
	if strings.TrimSpace(stderr) != "deleted" {
		t.Fatalf("topics delete stderr = %q, want deleted", stderr)
	}
}

func TestClientProduceConsumeAckParity(t *testing.T) {
	env := newCLITestEnv(t)
	if _, err := env.broker.CreateTopic(context.Background(), topics.CreateOpts{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := waitForAssignments(env.store, "orders"); err != nil {
		t.Fatalf("wait assignments: %v", err)
	}

	stdout, stderr, err := captureCLIOutput(t, func() error {
		return runClient([]string{"produce", "--addr", env.server.URL, "--key", "customer-1", "orders"})
	}, `{"id":1}`)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if stderr != "" {
		t.Fatalf("produce stderr = %q, want empty", stderr)
	}
	if stdout != "" {
		t.Fatalf("produce stdout = %q, want empty", stdout)
	}
	waitForAnyVisibleOffset(t, env.broker, "orders", []int64{0, 0, 0})

	stdout, stderr, err = captureCLIOutput(t, func() error {
		return runClient([]string{"consume", "--addr", env.server.URL, "orders"})
	}, "")
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if stderr != "" {
		t.Fatalf("consume stderr = %q, want empty", stderr)
	}
	msg := decodeCLIJSON[topic.Message](t, stdout)
	if msg.Topic != "orders" || strings.TrimSpace(string(msg.Payload)) != "{\n    \"id\": 1\n  }" || msg.ReceiptHandle == "" {
		t.Fatalf("consumed message = %+v", msg)
	}

	stdout, stderr, err = captureCLIOutput(t, func() error {
		return runClient([]string{"ack", "--addr", env.server.URL, "--handle", msg.ReceiptHandle, "orders"})
	}, "")
	if err != nil {
		t.Fatalf("ack: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("ack stdout = %q, want empty", stdout)
	}
	if strings.TrimSpace(stderr) != "acked" {
		t.Fatalf("ack stderr = %q, want acked", stderr)
	}
}

func TestClientTopicsFanoutAttachChildrenDetach(t *testing.T) {
	env := newCLITestEnv(t)

	for _, name := range []string{"parent", "child"} {
		_, _, err := captureCLIOutput(t, func() error {
			return runClient([]string{"topics", "create", "--addr", env.server.URL, "--partitions", "3", name})
		}, "")
		if err != nil {
			t.Fatalf("topics create %s: %v", name, err)
		}
		if err := waitForAssignments(env.store, name); err != nil {
			t.Fatalf("wait assignments %s: %v", name, err)
		}
	}

	stdout, _, err := captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "attach", "--addr", env.server.URL, "parent", "child"})
	}, "")
	if err != nil {
		t.Fatalf("topics attach: %v", err)
	}
	attached := decodeCLIJSON[topic.Topic](t, stdout)
	if attached.Role != topic.RoleParent || len(attached.Children) != 1 || attached.Children[0] != "child" {
		t.Fatalf("attach response = %+v, want parent with [child]", attached)
	}

	stdout, _, err = captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "children", "--addr", env.server.URL, "parent"})
	}, "")
	if err != nil {
		t.Fatalf("topics children: %v", err)
	}
	listed := decodeCLIJSON[struct {
		Parent   string `json:"parent"`
		Children []struct {
			Name string `json:"name"`
		} `json:"children"`
	}](t, stdout)
	if listed.Parent != "parent" || len(listed.Children) != 1 || listed.Children[0].Name != "child" {
		t.Fatalf("children listing = %+v", listed)
	}

	stdout, stderr, err := captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "detach", "--addr", env.server.URL, "parent", "child"})
	}, "")
	if err != nil {
		t.Fatalf("topics detach: %v", err)
	}
	if stdout != "" || stderr != "detached\n" {
		t.Fatalf("detach output = (%q, %q), want detached on stderr", stdout, stderr)
	}

	stdout, _, err = captureCLIOutput(t, func() error {
		return runClient([]string{"topics", "get", "--addr", env.server.URL, "child"})
	}, "")
	if err != nil {
		t.Fatalf("topics get child: %v", err)
	}
	details := decodeCLIJSON[topic.Details](t, stdout)
	if details.Role != topic.RoleStandalone || details.Parent != "" {
		t.Fatalf("child after detach = %+v, want standalone", details.Topic)
	}
}
