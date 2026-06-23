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
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/logger"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
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
		Metrics:       m,
	})
	if err != nil {
		return fmt.Errorf("metastore: %w", err)
	}
	defer closeWithLog(log, "metastore", ms.Close)

	// Build the main broker which is responsible for all client actions
	schemas := schema.NewJSONSchema()
	if err = initializeSchemas(context.Background(), ms, schemas); err != nil {
		return fmt.Errorf("initialize schemas: %w", err)
	}

	br, logs, offsets, lifecycle, ingressManager, err := buildBroker(cfg, nodeID, ms, schemas, m, log)
	if err != nil {
		return err
	}
	defer closeWithLog(log, "broker", br.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Controller and cluster router
	member := metastore.Member{
		ID:          nodeID,
		Addr:        advertisedMemberAddr(nodeID, cfg.HTTP.Addr, cfg.Cluster.Addr, cfg.Cluster.Peers),
		ClusterAddr: advertisedClusterAddr(nodeID, cfg.Cluster.Addr, cfg.Cluster.Peers),
		Status:      metastore.MemberAlive,
	}
	ctrl := controller.New(ms, controller.Config{})
	router := cluster.NewRouter(ms, nodeID, partition.NewHashRoundRobin(), br, m)
	peerRPC := cluster.NewPeerClient(5*time.Second, m)
	rpcServer := cluster.NewRPCServer(br, ms, log, m)
	produceDispatcher := cluster.NewProduceDispatcher(ingressManager, ms, nodeID, br, peerRPC, log, cluster.ProduceDispatcherConfig{}, m)

	// Start background processes
	var wg sync.WaitGroup
	wg.Go(func() { runMemberHeartbeater(ctx, ms, member, 5*time.Second, peerRPC, log) })
	wg.Go(func() { ctrl.Run(ctx) })
	wg.Go(func() { offsets.RunPurger(ctx, time.Second) })
	wg.Go(func() { produceDispatcher.Run(ctx) })
	startPprofServer(ctx, &wg, cfg.HTTP.PprofAddr, log)
	wg.Go(func() {
		if err := clusterrpc.ServeQUIC(ctx, cfg.HTTP.Addr, log, rpcServer); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("cluster rpc server", "addr", cfg.HTTP.Addr, "err", err)
		}
	})

	poller := metrics.NewPoller(m, br, log, cfg.Storage.DataDir)
	wg.Go(func() { poller.Run(ctx) })

	// Startup reconciliation: once this node's metastore replica is caught
	// up, reclaim orphaned topic dirs (crash safety) and open owned
	// partition logs so their retention reapers run even for topics that
	// are idle after a restart.
	wg.Go(func() { runStartupReconcile(ctx, ms, logs, cfg.Storage.DataDir, nodeID, log) })

	// Finally build the API server
	srv := buildAPIServer(ctx, cfg, br, logs, ms, router, m, reg, log)
	lifecycle.MarkReady()
	defer lifecycle.MarkNotReady()

	m.BootDurationSeconds.Set(time.Since(bootStart).Seconds())
	log.Info("narad serve starting",
		"addr", cfg.HTTP.Addr,
		"cluster_addr", cfg.Cluster.Addr,
		"data_dir", cfg.Storage.DataDir,
		"version", versionString())

	if err = srv.Run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}

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
	if f.pprofAddr != "" {
		cfg.HTTP.PprofAddr = f.pprofAddr
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

func buildBroker(
	cfg *config.Config,
	nodeID string,
	ms metastore.Metastore,
	schemas schema.Registry,
	m *metrics.Metrics,
	log *slog.Logger,
) (broker.Broker, *runtime.Logs, *consumer.InFlight, *runtime.Lifecycle, *ingress.Manager, error) {
	storageOpts, err := storageOptions(cfg.Storage)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("storage options: %w", err)
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

	offsetCommitter := runtime.NewConsumerOffsetCommitter(cfg.Storage.DataDir, time.Duration(cfg.Storage.FlushIntervalMs)*time.Millisecond, log)
	onCommit := func(topic string, partition int, offset int64) {
		offsetCommitter.Commit(topic, partition, offset)
	}

	offsets := consumer.NewInFlight(resolveCaps, onCommit)
	logs := runtime.NewLogs(cfg.Storage.DataDir, storageOpts, ms, m)
	lifecycle := runtime.NewLifecycle(logs, offsetCommitter.Close)
	if err = initializeConsumerOffsets(context.Background(), cfg.Storage.DataDir, ms, offsets, log, nodeID); err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("initialize consumer offsets: %w", err)
	}

	if _, ok := ms.(*metastore.Store); !ok {
		return nil, nil, nil, nil, nil, errors.New("broker: cluster coordination requires metastore.Store")
	}

	ingressManager, err := ingress.OpenManagerWithOptions(cfg.Storage.DataDir, ingressWALOptions(cfg.Storage, m))
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("ingress: %w", err)
	}

	br, err := broker.New(broker.Deps{
		DataDir:        cfg.Storage.DataDir,
		StorageOptions: storageOpts,
		TopicConfig: broker.TopicConfig{
			DefaultPartitions:                cfg.Topic.DefaultPartitions,
			MaxPartitions:                    cfg.Topic.MaxPartitions,
			DefaultRetentionMs:               cfg.Topic.DefaultRetentionAgeMs,
			DefaultVisibilityTimeoutMs:       cfg.Topic.DefaultVisibilityTimeoutMs,
			DefaultMaxInFlightPerPartition:   cfg.Topic.DefaultMaxInFlightPerPartition,
			DefaultMaxAckedAheadPerPartition: cfg.Topic.DefaultMaxAckedAheadPerPartition,
		},
		Metastore:       ms,
		Partitions:      partition.NewHashRoundRobin(),
		Schemas:         schemas,
		ConsumerOffsets: offsets,
		Logs:            logs,
		Ingress:         ingressManager,
		Logger:          log,
		SelfID:          nodeID,
		Lifecycle:       lifecycle,
		Metrics:         m,
	})
	if err != nil {
		_ = ingressManager.Close()
		return nil, nil, nil, nil, nil, fmt.Errorf("broker: %w", err)
	}
	return br, logs, offsets, lifecycle, ingressManager, nil
}

func buildAPIServer(ctx context.Context, cfg *config.Config, br broker.Broker, logs *runtime.Logs, ms *metastore.Store, router handlers.Router, m *metrics.Metrics, reg *prometheus.Registry, log *slog.Logger) *httpserver.Server {
	handlerSet := handlers.New(handlers.Deps{
		Broker:         br,
		Logs:           logs,
		Metastore:      ms,
		Logger:         log,
		MaxConsumeWait: cfg.HTTP.MaxConsumeWait.D(),
		ShutdownCtx:    ctx,
		Router:         router,
	})
	return httpserver.New(cfg.HTTP, httpserver.NewRouter(handlerSet, log, m, reg), log)
}

func initializeSchemas(ctx context.Context, ms metastore.Metastore, schemas schema.Registry) error {
	topics, _, err := ms.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return err
	}
	for _, topicCfg := range topics {
		for version := 1; ; version++ {
			raw, err := ms.GetSchema(ctx, topicCfg.Name, version)
			if err != nil {
				if errors.Is(err, metastore.ErrNotFound) {
					break
				}
				return err
			}
			if err := schemas.Load(ctx, topicCfg.Name, version, raw); err != nil {
				return err
			}
		}
	}
	return nil
}

// startupReconcileCaughtUpTimeout bounds how long startup reconciliation
// waits for the local metastore replica to catch up before giving up on
// the (destructive) orphan sweep.
const startupReconcileCaughtUpTimeout = 60 * time.Second

// runStartupReconcile waits for the local metastore replica to catch up,
// then (1) removes orphaned topic directories left by a crash between a
// topic's metastore delete and its file purge, and (2) opens this node's
// owned partition logs so retention reapers run for topics that are idle
// after a restart. The sweep is skipped if the replica never catches up,
// since acting on a stale topic set could delete live data.
func runStartupReconcile(ctx context.Context, store *metastore.Store, logs *runtime.Logs, dataDir, nodeID string, log *slog.Logger) {
	if waitMetastoreCaughtUp(ctx, store, startupReconcileCaughtUpTimeout) {
		removed, err := runtime.SweepOrphanTopicDirs(dataDir, func(name string) bool {
			_, getErr := store.GetTopic(ctx, name)
			return !errors.Is(getErr, errs.ErrNotFound)
		}, log)
		if err != nil {
			log.Warn("startup orphan sweep encountered errors", "err", err)
		}
		if len(removed) > 0 {
			log.Info("startup orphan sweep reclaimed topic directories", "count", len(removed))
		}
	} else if ctx.Err() == nil {
		log.Warn("skipping startup orphan sweep: metastore not caught up within timeout")
	}
	openOwnedPartitionLogs(ctx, store, logs, nodeID, log)
}

// waitMetastoreCaughtUp polls until the local replica has applied all
// committed entries (with a leader present), ctx is cancelled, or timeout.
func waitMetastoreCaughtUp(ctx context.Context, store *metastore.Store, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if store.AppliedCaughtUp() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// openOwnedPartitionLogs opens the partition logs this node owns so their
// retention reapers run regardless of produce/consume activity. Logs.Get
// refuses topics absent from the metastore, so deleted topics are never
// reopened here.
func openOwnedPartitionLogs(ctx context.Context, store *metastore.Store, logs *runtime.Logs, nodeID string, log *slog.Logger) {
	topics, _, err := store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		log.Warn("retention warmup: list topics failed", "err", err)
		return
	}
	opened := 0
	for _, t := range topics {
		assignments, err := store.ListAssignments(t.Name)
		if err != nil {
			continue
		}
		for _, a := range assignments {
			if a.OwnerID != nodeID {
				continue
			}
			if _, err := logs.Get(t.Name, a.Partition); err != nil {
				log.Debug("retention warmup: open owned partition failed", "topic", t.Name, "partition", a.Partition, "err", err)
				continue
			}
			opened++
		}
	}
	if opened > 0 {
		log.Info("retention warmup: opened owned partition logs", "count", opened)
	}
}

func initializeConsumerOffsets(ctx context.Context, dataDir string, ms metastore.Metastore, offsets *consumer.InFlight, log *slog.Logger, nodeID string) error {
	topics, _, err := ms.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return err
	}
	store, ok := ms.(*metastore.Store)
	if !ok {
		return fmt.Errorf("metastore does not support assignment listing")
	}
	for _, topicCfg := range topics {
		assignments, err := store.ListAssignments(topicCfg.Name)
		if err != nil {
			return err
		}
		owned := make(map[int]struct{}, len(assignments))
		for _, assignment := range assignments {
			if assignment.OwnerID == nodeID {
				owned[assignment.Partition] = struct{}{}
			}
		}
		for partition := range owned {
			partitionDir := storage.TopicPartitionDir(dataDir, topicCfg.Name, partition)
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
		Codec:           storageCodec,
		FlushBytes:      sc.FlushBytes,
		FlushRecords:    sc.FlushRecords,
		FlushInterval:   time.Duration(sc.FlushIntervalMs) * time.Millisecond,
		SyncMode:        storageSyncMode(sc.Fsync),
		SyncInterval:    time.Duration(sc.SyncIntervalMs) * time.Millisecond,
		SyncBytes:       sc.SyncBytes,
		HWMSyncInterval: time.Duration(sc.HighWatermarkSyncIntervalMs) * time.Millisecond,
		SegmentBytes:    sc.SegmentBytes,
		Retention: storage.RetentionConfig{
			CheckInterval: time.Duration(sc.RetentionCheckIntervalMs) * time.Millisecond,
		},
	}, nil
}

func ingressWALOptions(sc config.StorageConfig, m *metrics.Metrics) ingress.Options {
	opts := ingress.DefaultWALOptions(m)
	opts.SyncInterval = time.Duration(sc.IngressWALSyncIntervalMs) * time.Millisecond
	opts.SyncBytes = sc.IngressWALSyncBytes
	return ingress.Options{
		WAL:    opts,
		Shards: sc.IngressWALShards,
	}
}

func storageSyncMode(mode config.FsyncMode) storage.SyncMode {
	if mode == config.FsyncPerWrite {
		return storage.SyncPerWrite
	}
	return storage.SyncBatched
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
