package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/logger"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/replication"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	"github.com/debanganthakuria/narad/internal/transport/httpserver"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func runServe(args []string) error {
	bootStart := time.Now()

	cfg, err := loadServeConfig(args)
	if err != nil || cfg == nil {
		return err
	}
	if err := validateServeCluster(); err != nil {
		return err
	}

	log, err := logger.New(cfg.Log.Format, cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}

	reg, m := buildMetrics()

	if err = os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}

	nodeID, _ := os.Hostname()
	ms, err := metastore.New(metastore.Config{
		NodeID:   nodeID,
		DataDir:  filepath.Join(cfg.Storage.DataDir, "metastore"),
		BindAddr: cfg.Cluster.Addr,
	})
	if err != nil {
		return fmt.Errorf("metastore: %w", err)
	}
	defer closeWithLog(log, "metastore", ms.Close)

	br, logs, offsets, err := buildBroker(cfg, nodeID, ms, m, log)
	if err != nil {
		return err
	}
	defer closeWithLog(log, "broker", br.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	recoveryClient := &http.Client{Timeout: 5 * time.Second}
	recovery := replication.NewStoreRecovery(nodeID, ms, logs, recoveryClient)

	hb := controller.NewHeartbeater(ms, metastore.Member{
		ID:     nodeID,
		Addr:   cfg.HTTP.Addr,
		Status: metastore.MemberAlive,
	}, 5*time.Second)
	ctrl := controller.New(ms, controller.Config{})
	router := cluster.NewRouter(ms, nodeID, partition.NewHashRoundRobin())

	var wg sync.WaitGroup
	wg.Go(func() { hb.Run(ctx) })
	wg.Go(func() { ctrl.Run(ctx) })
	wg.Go(func() { offsets.RunPurger(ctx, time.Second) })
	wg.Go(func() {
		if pprofErr := http.ListenAndServe(":6060", nil); pprofErr != nil {
			log.Error("failed to start pprof", "err", pprofErr)
		}
	})

	poller := metrics.NewPoller(m, br, log)
	wg.Go(func() { poller.Run(ctx) })

	srv := buildAPIServer(cfg, br, logs, router, m, reg, log)
	wg.Go(func() {
		if repairErr := recovery.RepairOwnedPartitions(ctx); repairErr != nil {
			log.Error("repair owned partitions", "err", repairErr)
			stop()
		}
	})
	m.BootDurationSeconds.Set(time.Since(bootStart).Seconds())

	if err = srv.Run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}

	log.Info("narad serve started",
		"addr", cfg.HTTP.Addr,
		"cluster_addr", cfg.Cluster.Addr,
		"data_dir", cfg.Storage.DataDir,
		"version", versionString())

	wg.Wait()
	log.Info("narad serve stopped")
	return nil
}

func buildMetrics() (*prometheus.Registry, *metrics.Metrics) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg, metrics.New(reg)
}

type serveFlags struct {
	configPath  string
	port        int
	addr        string
	clusterPort int
	dataDir     string
	logLevel    string
	logFormat   string
	pprofAddr   string
}

func (f *serveFlags) applyTo(cfg *config.Config) {
	if f.port != 0 {
		cfg.HTTP.Addr = ":" + strconv.Itoa(f.port)
	}
	if f.addr != "" {
		cfg.HTTP.Addr = f.addr
	}
	if f.clusterPort != 0 {
		cfg.Cluster.Addr = ":" + strconv.Itoa(f.clusterPort)
	}
	if f.dataDir != "" {
		cfg.Storage.DataDir = f.dataDir
	}
	if f.logLevel != "" {
		cfg.Log.Level = f.logLevel
	}
	if f.logFormat != "" {
		cfg.Log.Format = f.logFormat
	}
}

func loadServeConfig(args []string) (*config.Config, error) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintln(out, "Usage: narad serve [flags]")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Run the Narad HTTP API server. Default port: 7942.")
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Flags:")
		fs.PrintDefaults()
	}

	var f serveFlags
	fs.StringVar(&f.configPath, "config", "", "path to JSON config file (optional)")
	fs.IntVar(&f.port, "port", 0, "API listen port (overrides http.addr; e.g. --port 7942)")
	fs.StringVar(&f.addr, "addr", "", "API listen address (overrides http.addr; e.g. --addr 0.0.0.0:7942)")
	fs.IntVar(&f.clusterPort, "cluster-port", 0, "cluster listen port (overrides cluster.addr)")
	fs.StringVar(&f.dataDir, "data-dir", "", "storage directory (overrides storage.data_dir)")
	fs.StringVar(&f.logLevel, "log-level", "", "log level: debug|info|warn|error (overrides log.level)")
	fs.StringVar(&f.logFormat, "log-format", "", "log format: json|text (overrides log.format)")
	fs.StringVar(&f.pprofAddr, "pprof-addr", "", "enable pprof on this address (e.g. 127.0.0.1:6060); empty disables")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, nil
		}
		return nil, err
	}

	cfg, err := config.Load(f.configPath)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	f.applyTo(cfg)
	if err = cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func validateServeCluster() error {
	peers := strings.TrimSpace(os.Getenv("NARAD_CLUSTER_PEERS"))
	if peers == "" {
		return errors.New("serve: NARAD_CLUSTER_PEERS must list exactly 3 cluster voters")
	}

	seen := make(map[string]struct{}, 3)
	for _, peer := range strings.Split(peers, ",") {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		seen[peer] = struct{}{}
	}
	if len(seen) != 3 {
		return fmt.Errorf("serve: NARAD_CLUSTER_PEERS must list exactly 3 cluster voters, got %d", len(seen))
	}
	return nil
}

func buildBroker(cfg *config.Config, nodeID string, ms metastore.Metastore, m *metrics.Metrics, log *slog.Logger) (broker.Broker, *runtime.Logs, *consumer.InFlight, error) {
	storageOpts, err := storageOptions(cfg.Storage)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("storage options: %w", err)
	}

	resolveCaps := func(ctx context.Context, topicName string) (consumer.Caps, error) {
		t, err := ms.GetTopic(ctx, topicName)
		if err != nil {
			return consumer.Caps{}, err
		}
		maxIF := int(t.MaxInFlightPerPartition)
		if maxIF <= 0 {
			maxIF = int(cfg.Topic.DefaultMaxInFlightPerPartition)
		}
		maxAA := int(t.MaxAckedAheadPerPartition)
		if maxAA <= 0 {
			maxAA = int(cfg.Topic.DefaultMaxAckedAheadPerPartition)
		}
		return consumer.Caps{MaxInFlight: maxIF, MaxAckedAhead: maxAA}, nil
	}

	offsets := consumer.NewInFlight(resolveCaps, func(topic string, partition int, offset int64) {
		partitionDir := filepath.Join(cfg.Storage.DataDir, "topics", topic, fmt.Sprintf("p%05d", partition))
		if err := storage.WriteConsumerOffset(partitionDir, offset); err != nil {
			log.Error("consumer offset write failed", "topic", topic, "partition", partition, "offset", offset, "err", err)
		}
	})
	logs := runtime.NewLogs(cfg.Storage.DataDir, storageOpts, ms, m)
	if err := initializeConsumerOffsets(context.Background(), cfg.Storage.DataDir, ms, offsets, log); err != nil {
		return nil, nil, nil, fmt.Errorf("initialize consumer offsets: %w", err)
	}
	client := &http.Client{Timeout: 5 * time.Second}

	store, ok := ms.(*metastore.Store)
	if !ok {
		return nil, nil, nil, errors.New("broker: cluster replication requires metastore.Store")
	}

	br, err := broker.New(broker.Deps{
		DataDir:        cfg.Storage.DataDir,
		StorageOptions: storageOpts,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions:                cfg.Topic.DefaultPartitions,
			MaxPartitions:                    cfg.Topic.MaxPartitions,
			DefaultReplicationFactor:         cfg.Topic.DefaultReplicationFactor,
			DefaultRetentionMs:               cfg.Topic.DefaultRetentionAgeMs,
			DefaultVisibilityTimeoutMs:       cfg.Topic.DefaultVisibilityTimeoutMs,
			DefaultMaxInFlightPerPartition:   cfg.Topic.DefaultMaxInFlightPerPartition,
			DefaultMaxAckedAheadPerPartition: cfg.Topic.DefaultMaxAckedAheadPerPartition,
		},
		Metastore:       ms,
		Partitions:      partition.NewHashRoundRobin(),
		Schemas:         schema.NewJSONSchema(),
		ConsumerOffsets: offsets,
		Replicator:      replication.NewCluster(nodeID, store, client),
		Logs:            logs,
		Logger:          log,
		Metrics:         m,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("broker: %w", err)
	}
	return br, logs, offsets, nil
}

func buildAPIServer(cfg *config.Config, br broker.Broker, logs *runtime.Logs, router handlers.Router, m *metrics.Metrics, reg *prometheus.Registry, log *slog.Logger) *httpserver.Server {
	handlerSet := handlers.New(handlers.Deps{
		Broker:         br,
		Logs:           logs,
		Logger:         log,
		MaxConsumeWait: cfg.HTTP.MaxConsumeWait.D(),
		Router:         router,
	})
	return httpserver.New(cfg.HTTP, httpserver.NewRouter(handlerSet, log, m, reg), log)
}

func initializeConsumerOffsets(ctx context.Context, dataDir string, ms metastore.Metastore, offsets *consumer.InFlight, log *slog.Logger) error {
	topics, _, err := ms.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return err
	}
	for _, topicCfg := range topics {
		for partition := 0; partition < topicCfg.Partitions; partition++ {
			partitionDir := filepath.Join(dataDir, "topics", topicCfg.Name, fmt.Sprintf("p%05d", partition))
			committed, ok, err := storage.ReadConsumerOffset(partitionDir)
			if err != nil {
				log.Error("consumer offset recovery skipped", "topic", topicCfg.Name, "partition", partition, "err", err)
				continue
			}
			if !ok {
				continue
			}
			if err := offsets.Init(ctx, topicCfg.Name, partition, committed); err != nil {
				return err
			}
		}
	}
	return nil
}

func closeWithLog(log *slog.Logger, what string, close func() error) {
	if err := close(); err != nil {
		log.Error(what+" close", "err", err)
	}
}

func storageOptions(sc config.StorageConfig) (storage.Options, error) {
	storageCodec, err := buildCodec(sc)
	if err != nil {
		return storage.Options{}, err
	}
	return storage.Options{
		Codec:         storageCodec,
		FlushBytes:    sc.FlushBytes,
		FlushRecords:  sc.FlushRecords,
		FlushInterval: time.Duration(sc.FlushIntervalMs) * time.Millisecond,
		SegmentBytes:  sc.SegmentBytes,
		Retention: storage.RetentionConfig{
			CheckInterval: time.Duration(sc.RetentionCheckIntervalMs) * time.Millisecond,
		},
	}, nil
}

func buildCodec(sc config.StorageConfig) (codec.Codec, error) {
	switch strings.ToLower(sc.Codec) {
	case "none":
		return codec.NewNoopCodec(), nil
	case "zstd", "":
		level, err := zstdLevelFromString(sc.CompressionLevel)
		if err != nil {
			return nil, err
		}
		return codec.NewZstdCodec(level)
	default:
		return nil, fmt.Errorf("unknown codec %q", sc.Codec)
	}
}

func zstdLevelFromString(s string) (zstd.EncoderLevel, error) {
	switch strings.ToLower(s) {
	case "fastest":
		return zstd.SpeedFastest, nil
	case "default", "":
		return zstd.SpeedDefault, nil
	case "better":
		return zstd.SpeedBetterCompression, nil
	case "best":
		return zstd.SpeedBestCompression, nil
	default:
		return 0, fmt.Errorf("unknown compression level %q", s)
	}
}
