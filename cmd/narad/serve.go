package main

import (
	"context"
	"crypto/rand"
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
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
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

	log, err := logger.New(cfg.Log.Format, cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}

	reg, m := buildMetrics()

	if err = os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil { // idempotent dir creation
		return fmt.Errorf("data dir: %w", err)
	}

	ms, err := metastore.NewSQLiteStore(filepath.Join(cfg.Storage.DataDir, "metastore/metadata.db"))
	if err != nil {
		return fmt.Errorf("metastore: %w", err)
	}
	defer closeWithLog(log, "metastore", ms.Close)

	br, err := buildBroker(cfg, ms, m, log)
	if err != nil {
		return err
	}
	defer closeWithLog(log, "broker", br.Close)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Go(func() {
		if pprofErr := http.ListenAndServe(":6060", nil); pprofErr != nil {
			log.Error("failed to start pprof", "err", pprofErr)
		}
	})

	poller := metrics.NewPoller(m, br, log)
	wg.Go(func() { poller.Run(ctx) })

	srv := buildAPIServer(cfg, br, m, reg, log)

	// Capture boot time
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

// buildMetrics constructs the Prometheus registry plus runtime
// collectors and returns both. The registry is what /metrics scrapes;
// the *Metrics struct is what every instrumented call site reads
// from. Splitting them this way means tests can swap a fresh registry
// without touching the rest of the wiring.
func buildMetrics() (*prometheus.Registry, *metrics.Metrics) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg, metrics.New(reg)
}

// serveFlags holds the values parsed from the `narad serve` flag set
// before they are overlaid onto the loaded config. Keeping them in one
// struct lets us pass parsed flags around without long parameter lists.
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

// applyTo overlays CLI-flag values onto cfg. Only non-zero fields take
// effect, preserving values from the config file and environment.
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

// loadServeConfig parses CLI flags, loads the config (file + env), and
// applies CLI overlays. Returns (nil, nil) when --help was printed so
// the caller can exit cleanly without doing further work.
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

func buildBroker(cfg *config.Config, ms *metastore.SQLiteStore, m *metrics.Metrics, log *slog.Logger) (broker.Broker, error) {
	storageOpts, err := storageOptions(cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("storage options: %w", err)
	}

	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate handle secret: %w", err)
	}

	// Caps resolver consults the metastore so altered caps are honored
	// at the next ReserveNext / Commit call without restarting the
	// broker. Falls back to TopicConfig defaults for topics whose
	// per-record value is zero (legacy rows pre-migration).
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
		ConsumerOffsets: consumer.NewInFlight(consumer.NewMetastoreBacked(ms), resolveCaps),
		Replicator:      replication.NewLocal(),
		Logger:          log,
		HandleSecret:    secret,
		Metrics:         m,
	})
	if err != nil {
		return nil, fmt.Errorf("broker: %w", err)
	}
	return br, nil
}

func buildAPIServer(cfg *config.Config, br broker.Broker, m *metrics.Metrics, reg *prometheus.Registry, log *slog.Logger) *httpserver.Server {
	handlerSet := handlers.New(handlers.Deps{
		Broker:         br,
		Logger:         log,
		MaxConsumeWait: cfg.HTTP.MaxConsumeWait.D(),
	})
	return httpserver.New(cfg.HTTP, httpserver.NewRouter(handlerSet, log, m, reg), log)
}

// closeWithLog invokes close and logs any error under the given label.
// Useful as a defer where we don't want to drop the error but also
// don't want it to mask the function's primary return.
func closeWithLog(log *slog.Logger, what string, close func() error) {
	if err := close(); err != nil {
		log.Error(what+" close", "err", err)
	}
}

func storageOptions(sc config.StorageConfig) (storage.Options, error) {
	codec, err := buildCodec(sc)
	if err != nil {
		return storage.Options{}, err
	}
	return storage.Options{
		Codec:         codec,
		FlushBytes:    sc.FlushBytes,
		FlushRecords:  sc.FlushRecords,
		FlushInterval: time.Duration(sc.FlushIntervalMs) * time.Millisecond,
		SegmentBytes:  sc.SegmentBytes,
		// Per-topic MaxAge/MaxBytes get filled in by the broker per
		// partition (see broker/partition_log.go). Only the
		// operational dial comes from storage config.
		Retention: storage.RetentionConfig{
			CheckInterval: time.Duration(sc.RetentionCheckIntervalMs) * time.Millisecond,
		},
	}, nil
}

func buildCodec(sc config.StorageConfig) (storage.Codec, error) {
	switch strings.ToLower(sc.Codec) {
	case "none":
		return storage.NewNoopCodec(), nil
	case "zstd", "":
		level, err := zstdLevelFromString(sc.CompressionLevel)
		if err != nil {
			return nil, err
		}
		return storage.NewZstdCodec(level)
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
