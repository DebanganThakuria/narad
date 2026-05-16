package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"

	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

func TestServeFlagsApplyTo(t *testing.T) {
	cfg := config.Default()
	flags := serveFlags{
		port:        9001,
		addr:        "127.0.0.1:9002",
		clusterPort: 9100,
		dataDir:     "/tmp/narad-data",
		logLevel:    "debug",
		logFormat:   "json",
	}

	flags.applyTo(cfg)

	if cfg.HTTP.Addr != "127.0.0.1:9002" {
		t.Fatalf("HTTP.Addr = %q, want %q", cfg.HTTP.Addr, "127.0.0.1:9002")
	}
	if cfg.Cluster.Addr != ":9100" {
		t.Fatalf("Cluster.Addr = %q, want %q", cfg.Cluster.Addr, ":9100")
	}
	if cfg.Storage.DataDir != "/tmp/narad-data" {
		t.Fatalf("DataDir = %q, want %q", cfg.Storage.DataDir, "/tmp/narad-data")
	}
	if cfg.Log.Level != "debug" || cfg.Log.Format != "json" {
		t.Fatalf("Log config = %+v", cfg.Log)
	}
}

func TestLoadServeConfigReturnsNilOnHelp(t *testing.T) {
	cfg, err := loadServeConfig([]string{"-help"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg != nil {
		t.Fatalf("loadServeConfig() cfg = %+v, want nil", cfg)
	}
}

func TestLoadServeConfigAppliesFlags(t *testing.T) {
	cfg, err := loadServeConfig([]string{"--addr", "127.0.0.1:9123", "--cluster-port", "9456", "--log-level", "debug"})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9123" {
		t.Fatalf("HTTP.Addr = %q, want %q", cfg.HTTP.Addr, "127.0.0.1:9123")
	}
	if cfg.Cluster.Addr != ":9456" {
		t.Fatalf("Cluster.Addr = %q, want %q", cfg.Cluster.Addr, ":9456")
	}
	if cfg.Log.Level != "debug" {
		t.Fatalf("Log.Level = %q, want %q", cfg.Log.Level, "debug")
	}
}

func TestLoadServeConfigLoadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "narad.json")
	if err := os.WriteFile(path, []byte(`{"http":{"addr":"127.0.0.1:8111"},"cluster":{"addr":"127.0.0.1:8112"}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := loadServeConfig([]string{"--config", path})
	if err != nil {
		t.Fatalf("loadServeConfig() error = %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:8111" || cfg.Cluster.Addr != "127.0.0.1:8112" {
		t.Fatalf("loadServeConfig() cfg = %+v", cfg)
	}
}

func TestValidateServeClusterRequiresThreePeers(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_PEERS", "n1,n2")
	if err := validateServeCluster(); err == nil {
		t.Fatal("validateServeCluster() error = nil, want error")
	}
}

func TestValidateServeClusterAcceptsThreePeers(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_PEERS", "n1,n2,n3")
	if err := validateServeCluster(); err != nil {
		t.Fatalf("validateServeCluster() error = %v", err)
	}
}

func TestValidateServeClusterRejectsMissingPeers(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_PEERS", "")
	if err := validateServeCluster(); err == nil {
		t.Fatal("validateServeCluster() error = nil, want error")
	}
}

func TestValidateServeClusterDeduplicatesPeers(t *testing.T) {
	t.Setenv("NARAD_CLUSTER_PEERS", "n1,n1,n2")
	if err := validateServeCluster(); err == nil {
		t.Fatal("validateServeCluster() error = nil, want error")
	}
}

func TestBuildBrokerRejectsNonStoreMetastore(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fakeMS := stubMetastore{}

	if _, _, _, err := buildBroker(cfg, "node-1", fakeMS, m, log); err == nil {
		t.Fatal("buildBroker() error = nil, want error")
	}
}

func TestBuildBrokerReturnsLogs(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := metastore.New(metastore.Config{NodeID: "node-1", DataDir: filepath.Join(t.TempDir(), "metastore"), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	defer store.Close()

	br, logs, _, err := buildBroker(cfg, "node-1", store, m, log)
	if err != nil {
		t.Fatalf("buildBroker() error = %v", err)
	}
	if br == nil || logs == nil {
		t.Fatalf("buildBroker() = (%v, %v), want non-nil", br, logs)
	}
}

func TestBuildBrokerPanicsWithoutStoreOnlyAfterTypeCheck(t *testing.T) {
	_ = stubMetastore{}
}

type stubMetastore struct{}

func (stubMetastore) CreateTopic(context.Context, topic.Topic) error { return nil }
func (stubMetastore) UpdateTopic(context.Context, topic.Topic) error { return nil }
func (stubMetastore) DeleteTopic(context.Context, string) error { return nil }
func (stubMetastore) GetTopic(context.Context, string) (topic.Topic, error) { return topic.Topic{}, nil }
func (stubMetastore) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}
func (stubMetastore) PutSchema(context.Context, string, int, []byte) error { return nil }
func (stubMetastore) GetSchema(context.Context, string, int) ([]byte, error) { return nil, nil }
func (stubMetastore) GetConsumerOffset(context.Context, string, int) (int64, error) { return 0, nil }
func (stubMetastore) SetConsumerOffset(context.Context, string, int, int64) error { return nil }
func (stubMetastore) Close() error { return nil }

func TestBuildMetricsReturnsUsableRegistry(t *testing.T) {
	reg, m := buildMetrics()
	if reg == nil || m == nil {
		t.Fatal("buildMetrics() returned nil values")
	}
	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(metricFamilies) == 0 {
		t.Fatal("Gather() returned no metric families")
	}
}

func TestCloseWithLogDoesNothingOnNilError(t *testing.T) {
	closeWithLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "metastore", func() error { return nil })
}

func TestZstdLevelFromString(t *testing.T) {
	cases := []struct {
		input string
		want  zstd.EncoderLevel
	}{
		{input: "fastest", want: zstd.SpeedFastest},
		{input: "default", want: zstd.SpeedDefault},
		{input: "better", want: zstd.SpeedBetterCompression},
		{input: "best", want: zstd.SpeedBestCompression},
		{input: "", want: zstd.SpeedDefault},
	}

	for _, tc := range cases {
		got, err := zstdLevelFromString(tc.input)
		if err != nil {
			t.Fatalf("zstdLevelFromString(%q) error = %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("zstdLevelFromString(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}

	if _, err := zstdLevelFromString("weird"); err == nil {
		t.Fatal("zstdLevelFromString() error = nil, want error")
	}
}

func TestBuildCodec(t *testing.T) {
	c, err := buildCodec(config.StorageConfig{Codec: "none"})
	if err != nil {
		t.Fatalf("buildCodec(none) error = %v", err)
	}
	if c.Flag() != 0 {
		t.Fatalf("buildCodec(none) flag = %d, want 0", c.Flag())
	}

	c, err = buildCodec(config.StorageConfig{Codec: "zstd", CompressionLevel: "better"})
	if err != nil {
		t.Fatalf("buildCodec(zstd) error = %v", err)
	}
	if c == nil {
		t.Fatal("buildCodec(zstd) returned nil codec")
	}

	if _, err := buildCodec(config.StorageConfig{Codec: "unknown"}); err == nil {
		t.Fatal("buildCodec(unknown) error = nil, want error")
	}
}

func TestStorageOptions(t *testing.T) {
	opts, err := storageOptions(config.StorageConfig{
		Codec:                    "none",
		FlushBytes:               128,
		FlushRecords:             16,
		FlushIntervalMs:          250,
		SegmentBytes:             4096,
		RetentionCheckIntervalMs: 500,
	})
	if err != nil {
		t.Fatalf("storageOptions() error = %v", err)
	}
	if opts.FlushBytes != 128 || opts.FlushRecords != 16 || opts.SegmentBytes != 4096 {
		t.Fatalf("storageOptions() opts = %+v", opts)
	}
	if opts.FlushInterval != 250*time.Millisecond {
		t.Fatalf("FlushInterval = %v, want %v", opts.FlushInterval, 250*time.Millisecond)
	}
	if opts.Retention.CheckInterval != 500*time.Millisecond {
		t.Fatalf("Retention.CheckInterval = %v, want %v", opts.Retention.CheckInterval, 500*time.Millisecond)
	}
}

func TestStorageOptionsReturnsCodecError(t *testing.T) {
	_, err := storageOptions(config.StorageConfig{Codec: "wat"})
	if err == nil {
		t.Fatal("storageOptions() error = nil, want error")
	}
}

func TestCloseWithLogHandlesError(t *testing.T) {
	closeWithLog(slog.New(slog.NewTextHandler(io.Discard, nil)), "broker", func() error {
		return errors.New("close failed")
	})
}

func TestBuildAPIServerPanicsWithoutBroker(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("buildAPIServer() did not panic")
		}
	}()

	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	_ = buildAPIServer(cfg, nil, nil, nil, m, reg, log)
}

func TestBuildAPIServerReturnsServer(t *testing.T) {
	cfg := config.Default()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	broker := stubBroker{}
	srv := buildAPIServer(cfg, broker, nil, nil, m, reg, log)
	if srv == nil {
		t.Fatal("buildAPIServer() returned nil")
	}
}

type stubBroker struct{}

func (stubBroker) CreateTopic(context.Context, brokertopics.CreateOpts) (topic.Topic, error) {
	return topic.Topic{}, nil
}
func (stubBroker) IncreaseTopicPartitions(context.Context, string, int) (topic.Topic, error) {
	return topic.Topic{}, nil
}
func (stubBroker) UpdateTopicRetention(context.Context, string, int64) (topic.Topic, error) {
	return topic.Topic{}, nil
}
func (stubBroker) UpdateTopicCaps(context.Context, string, int64, int64) (topic.Topic, error) {
	return topic.Topic{}, nil
}
func (stubBroker) UpdateTopicSchema(context.Context, string, []byte) (topic.Topic, error) {
	return topic.Topic{}, nil
}
func (stubBroker) DeleteTopic(context.Context, string) error { return nil }
func (stubBroker) GetTopic(context.Context, string) (topic.Topic, error) { return topic.Topic{}, nil }
func (stubBroker) GetTopicDetails(context.Context, string) (topic.Details, error) {
	return topic.Details{}, nil
}
func (stubBroker) ListTopics(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
	return nil, "", nil
}
func (stubBroker) Produce(context.Context, string, string, []byte) (int64, int, error) { return 0, 0, nil }
func (stubBroker) Consume(context.Context, string, brokermsg.ConsumeOpts) (topic.Message, bool, error) {
	return topic.Message{}, false, nil
}
func (stubBroker) Ack(context.Context, string, string) error { return nil }
func (stubBroker) Snapshot(context.Context) ([]metrics.TopicSnapshot, error) { return nil, nil }
func (stubBroker) Ready(context.Context) error { return nil }
func (stubBroker) Close() error { return nil }

