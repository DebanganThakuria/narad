package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/config"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func buildMetrics() (*prometheus.Registry, *metrics.Metrics) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg, metrics.New(reg)
}

// brokerComponents bundles the broker with the collaborators runServe
// wires into background workers and the HTTP layer.
type brokerComponents struct {
	broker     broker.Broker
	createGate broker.CreateGater
	logs       *runtime.Logs
	offsets    *consumer.InFlight
	lifecycle  *runtime.Lifecycle
	ingress    *ingress.Manager
	metrics    *metrics.Metrics
}

func buildBroker(
	cfg *config.Config,
	nodeID string,
	ms metastore.Metastore,
	schemas schema.Registry,
	m *metrics.Metrics,
	log *slog.Logger,
) (*brokerComponents, error) {
	storageOpts, err := storageOptions(cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("storage options: %w", err)
	}

	offsetCommitter := runtime.NewConsumerOffsetCommitter(cfg.Storage.DataDir, time.Duration(cfg.Storage.FlushIntervalMs)*time.Millisecond, log)
	offsets := consumer.NewInFlight(capsResolver(ms, cfg.Topic), offsetCommitter.Commit)
	// Committed consumer offsets recover lazily from the per-partition
	// file when a shard is first touched. Recovering from DISK — not from
	// a startup scan over metastore assignments — matters: at boot the
	// local replica can be stale (old Raft snapshot), and an
	// assignment-driven scan would skip partitions this node owns,
	// leaving their shards to start at -1 and re-deliver whole logs.
	offsets.SetCommittedRecovery(func(topicName string, partition int) (int64, bool) {
		dir := storage.TopicPartitionDir(cfg.Storage.DataDir, topicName, partition)
		committed, ok, err := storage.ReadConsumerOffset(dir)
		if err != nil {
			log.Error("consumer offset recovery failed; starting from log start", "topic", topicName, "partition", partition, "err", err)
			return 0, false
		}
		return committed, ok
	})
	logs := runtime.NewLogs(cfg.Storage.DataDir, storageOpts, ms, m)
	lifecycle := runtime.NewLifecycle(logs, offsetCommitter.Close)

	if _, ok := ms.(*metastore.Store); !ok {
		return nil, errors.New("broker: cluster coordination requires metastore.Store")
	}

	ingressManager, err := ingress.OpenManager(cfg.Storage.DataDir, ingressWALOptions(cfg.Storage))
	if err != nil {
		return nil, fmt.Errorf("ingress: %w", err)
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
		return nil, fmt.Errorf("broker: %w", err)
	}

	// The startup create gate is load-bearing: without it a peer-forwarded
	// create can race the startup orphan sweep and have its live topic
	// directory removed as an orphan. Brokers built by broker.New always
	// implement CreateGater (compile-time checked there); surface any
	// future wrapper that drops the interface as a loud startup failure
	// rather than silently running ungated.
	createGate, ok := br.(broker.CreateGater)
	if !ok {
		_ = br.Close()
		return nil, fmt.Errorf("broker: %T does not implement broker.CreateGater; startup create gate cannot be armed", br)
	}

	return &brokerComponents{
		broker:     br,
		createGate: createGate,
		logs:       logs,
		offsets:    offsets,
		lifecycle:  lifecycle,
		ingress:    ingressManager,
		metrics:    m,
	}, nil
}

// capsResolver returns a per-topic consumer caps lookup that falls back to
// the configured defaults when the topic leaves a cap unset.
func capsResolver(ms metastore.Metastore, defaults config.TopicConfig) consumer.CapsResolver {
	return func(ctx context.Context, topicName string) (consumer.Caps, error) {
		t, err := ms.GetTopic(ctx, topicName)
		if err != nil {
			return consumer.Caps{}, err
		}
		caps := consumer.Caps{
			MaxInFlight:   int(t.MaxInFlightPerPartition),
			MaxAckedAhead: int(t.MaxAckedAheadPerPartition),
		}
		if caps.MaxInFlight <= 0 {
			caps.MaxInFlight = int(defaults.DefaultMaxInFlightPerPartition)
		}
		if caps.MaxAckedAhead <= 0 {
			caps.MaxAckedAhead = int(defaults.DefaultMaxAckedAheadPerPartition)
		}
		return caps, nil
	}
}

func buildAPIServer(ctx context.Context, cfg *config.Config, br broker.Broker, logs *runtime.Logs, ms *metastore.Store, router handlers.Router, m *metrics.Metrics, reg *prometheus.Registry, auth *security.Authenticator, log *slog.Logger) *httpserver.Server {
	handlerSet := handlers.New(handlers.Deps{
		Broker:         br,
		Logs:           logs,
		Metastore:      ms,
		Logger:         log,
		MaxConsumeWait: cfg.HTTP.MaxConsumeWait.D(),
		ShutdownCtx:    ctx,
		Router:         router,
	})
	return httpserver.New(cfg.HTTP, httpserver.NewRouter(handlerSet, log, m, reg, auth), log)
}

// initializeSchemas loads every persisted schema version into the registry
// so validation works from the first request after a restart.
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

// closeWithLog invokes closeFn and logs (rather than returns) its error;
// it exists for defers where a close failure must not mask the real error.
func closeWithLog(log *slog.Logger, what string, closeFn func() error) {
	if err := closeFn(); err != nil {
		log.Error(what+" close", "err", err)
	}
}
