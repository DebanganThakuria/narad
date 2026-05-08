package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/config"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/httpserver"
	"github.com/debanganthakuria/narad/internal/httpserver/handlers"
	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/observability/logger"
	"github.com/debanganthakuria/narad/internal/partition"
	"github.com/debanganthakuria/narad/internal/replication"
	"github.com/debanganthakuria/narad/internal/schema"
	"github.com/debanganthakuria/narad/internal/storage"
	"github.com/debanganthakuria/narad/internal/topic"
)

func runServe(args []string) error {
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

	configPath := fs.String("config", "", "path to JSON config file (optional)")
	port := fs.Int("port", 0, "API listen port (overrides http.addr; e.g. --port 7942)")
	addr := fs.String("addr", "", "API listen address (overrides http.addr; e.g. --addr 0.0.0.0:7942)")
	clusterPort := fs.Int("cluster-port", 0, "cluster listen port (overrides cluster.addr)")
	dataDir := fs.String("data-dir", "", "storage directory (overrides storage.data_dir)")
	logLevel := fs.String("log-level", "", "log level: debug|info|warn|error (overrides log.level)")
	logFormat := fs.String("log-format", "", "log format: json|text (overrides log.format)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	// CLI flags overlay LAST — highest precedence.
	if *port != 0 {
		cfg.HTTP.Addr = ":" + strconv.Itoa(*port)
	}
	if *addr != "" {
		cfg.HTTP.Addr = *addr
	}
	if *clusterPort != 0 {
		cfg.Cluster.Addr = ":" + strconv.Itoa(*clusterPort)
	}
	if *dataDir != "" {
		cfg.Storage.DataDir = *dataDir
	}
	if *logLevel != "" {
		cfg.Log.Level = *logLevel
	}
	if *logFormat != "" {
		cfg.Log.Format = *logFormat
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	log, err := logger.New(cfg.Log.Format, cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}

	if err := os.MkdirAll(cfg.Storage.DataDir, 0o755); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}

	ms, err := metastore.NewSQLiteStore(filepath.Join(cfg.Storage.DataDir, "metadata.db"))
	if err != nil {
		return fmt.Errorf("metastore: %w", err)
	}
	defer func() {
		if err := ms.Close(); err != nil {
			log.Error("metastore close", "err", err)
		}
	}()

	logOpts, err := storageOptions(cfg.Storage)
	if err != nil {
		return fmt.Errorf("storage options: %w", err)
	}

	br, err := broker.New(broker.Deps{
		DataDir:    cfg.Storage.DataDir,
		LogOptions: logOpts,
		TopicPolicy: broker.TopicPolicy{
			DefaultPartitions:        cfg.Topic.DefaultPartitions,
			MaxPartitions:            cfg.Topic.MaxPartitions,
			DefaultReplicationFactor: cfg.Topic.DefaultReplicationFactor,
			DefaultRetention: topic.Retention{
				MaxAgeMs: cfg.Topic.DefaultRetentionAgeMs,
				MaxBytes: cfg.Topic.DefaultRetentionBytes,
			},
		},
		Metastore:  ms,
		Partitions: partition.NewHashRoundRobin(),
		Schemas:    schema.NewJSONSchema(),
		Offsets:    consumer.NewMetastoreBacked(ms),
		Replicator: replication.NewLocal(),
		Logger:     log,
	})
	if err != nil {
		return fmt.Errorf("broker: %w", err)
	}
	defer func() {
		if err := br.Close(); err != nil {
			log.Error("broker close", "err", err)
		}
	}()

	handlerSet := handlers.New(handlers.Deps{
		Broker:         br,
		Logger:         log,
		MaxConsumeWait: cfg.HTTP.MaxConsumeWait.D(),
	})
	router := httpserver.NewRouter(handlerSet, log)
	srv := httpserver.New(cfg.HTTP, router, log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("narad serve starting",
		"addr", cfg.HTTP.Addr,
		"cluster_addr", cfg.Cluster.Addr,
		"data_dir", cfg.Storage.DataDir,
		"version", versionString())

	if err := srv.Run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server: %w", err)
	}
	log.Info("narad serve stopped")
	return nil
}

func storageOptions(sc config.StorageConfig) (storage.Options, error) {
	var codec storage.Codec
	switch strings.ToLower(sc.Codec) {
	case "none":
		codec = storage.NewNoopCodec()
	case "zstd", "":
		level, err := zstdLevelFromString(sc.CompressionLevel)
		if err != nil {
			return storage.Options{}, err
		}
		codec, err = storage.NewZstdCodec(level)
		if err != nil {
			return storage.Options{}, err
		}
	default:
		return storage.Options{}, fmt.Errorf("unknown codec %q", sc.Codec)
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
