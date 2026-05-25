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

	// Load the app config
	cfg, err := loadServeConfig(args)
	if err != nil || cfg == nil {
		return err
	}

	// Observability Logger
	log, err := logger.New(cfg.Log.Format, cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}

	// Observability metrics
	reg, m := buildMetrics()

	if err = os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}

	// Init metastore
	nodeID, err := resolveNodeID(cfg)
	if err != nil {
		return err
	}
	ms, err := metastore.New(metastore.Config{
		NodeID:        nodeID,
		DataDir:       filepath.Join(cfg.Storage.DataDir, "metastore"),
		BindAddr:      cfg.Cluster.Addr,
		AdvertiseAddr: advertisedClusterAddr(nodeID, cfg.Cluster.Addr, cfg.Cluster.Peers),
		Peers:         configPeersToMetastore(nodeID, cfg.Cluster.Addr, cfg.Cluster.Peers),
	})
	if err != nil {
		return fmt.Errorf("metastore: %w", err)
	}
	defer closeWithLog(log, "metastore", ms.Close)

	// Build the main broker which is responsible for all client actions
	br, logs, offsets, err := buildBroker(cfg, nodeID, ms, m, log)
	if err != nil {
		return err
	}
	defer closeWithLog(log, "broker", br.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Recovery client, controller and cluster router
	recoveryClient := &http.Client{Timeout: 5 * time.Second}
	recovery := replication.NewStoreRecovery(nodeID, ms, logs, recoveryClient)

	hb := controller.NewHeartbeater(ms, metastore.Member{
		ID:     nodeID,
		Addr:   advertisedClusterAddr(nodeID, cfg.Cluster.Addr, cfg.Cluster.Peers),
		Status: metastore.MemberAlive,
	}, 5*time.Second)
	ctrl := controller.New(ms, controller.Config{})
	router := cluster.NewRouter(ms, nodeID, partition.NewHashRoundRobin(), br)

	// Start background processes
	var wg sync.WaitGroup
	wg.Go(func() { hb.Run(ctx) })
	wg.Go(func() { ctrl.Run(ctx) })
	wg.Go(func() { offsets.RunPurger(ctx, time.Second) })

	poller := metrics.NewPoller(m, br, log)
	wg.Go(func() { poller.Run(ctx) })

	wg.Go(func() {
		if repairErr := recovery.RepairOwnedPartitions(ctx); repairErr != nil {
			log.Error("repair owned partitions", "err", repairErr)
			stop()
		}
	})

	// Finally build the API server
	srv := buildAPIServer(cfg, br, logs, router, m, reg, log)

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
	nodeID      string
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
	if f.nodeID != "" {
		cfg.Cluster.NodeID = f.nodeID
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
	fs.StringVar(&f.nodeID, "node-id", "", "stable cluster node ID (overrides cluster.node_id)")
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

func resolveNodeID(cfg *config.Config) (string, error) {
	if cfg != nil && cfg.Cluster.NodeID != "" {
		return cfg.Cluster.NodeID, nil
	}
	nodeID, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("resolve node id: %w", err)
	}
	return nodeID, nil
}

func configPeersToMetastore(nodeID, clusterAddr string, peers []config.ClusterPeer) []metastore.Peer {
	if len(peers) == 0 {
		return nil
	}
	out := make([]metastore.Peer, 0, len(peers)-1)
	for _, peer := range peers {
		if peer.ID == nodeID && clusterAddrMatchesPeer(clusterAddr, peer.Addr) {
			continue
		}
		out = append(out, metastore.Peer{ID: peer.ID, Addr: peer.Addr})
	}
	return out
}

func advertisedClusterAddr(nodeID, clusterAddr string, peers []config.ClusterPeer) string {
	for _, peer := range peers {
		if peer.ID != nodeID || !clusterAddrMatchesPeer(clusterAddr, peer.Addr) {
			continue
		}
		if strings.HasPrefix(clusterAddr, ":") && !strings.HasPrefix(strings.TrimSpace(peer.Addr), ":") {
			return peer.Addr
		}
		return clusterAddr
	}
	return clusterAddr
}

func clusterAddrMatchesPeer(clusterAddr, peerAddr string) bool {
	clusterAddr = strings.TrimSpace(clusterAddr)
	peerAddr = strings.TrimSpace(peerAddr)
	if clusterAddr == "" || peerAddr == "" {
		return false
	}
	if clusterAddr == peerAddr {
		return true
	}
	if strings.HasPrefix(clusterAddr, ":") {
		return strings.HasSuffix(peerAddr, clusterAddr)
	}
	if strings.HasPrefix(peerAddr, ":") {
		return strings.HasSuffix(clusterAddr, peerAddr)
	}
	return false
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

	onCommit := func(topic string, partition int, offset int64) {
		partitionDir := filepath.Join(cfg.Storage.DataDir, "topics", topic, fmt.Sprintf("p%05d", partition))
		if err := storage.WriteConsumerOffset(partitionDir, offset); err != nil {
			log.Error("consumer offset write failed", "topic", topic, "partition", partition, "offset", offset, "err", err)
		}
	}

	offsets := consumer.NewInFlight(resolveCaps, onCommit)
	logs := runtime.NewLogs(cfg.Storage.DataDir, storageOpts, ms, m)
	if err = initializeConsumerOffsets(context.Background(), cfg.Storage.DataDir, ms, offsets, log); err != nil {
		return nil, nil, nil, fmt.Errorf("initialize consumer offsets: %w", err)
	}

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
		Replicator:      replication.NewCluster(nodeID, store, &http.Client{Timeout: 5 * time.Second}),
		Logs:            logs,
		Logger:          log,
		SelfID:          nodeID,
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
	// TODO Only list topics this is a leader of and own it
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
