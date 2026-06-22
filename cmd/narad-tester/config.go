package main

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

type config struct {
	Nodes                     []string
	Topics                    int
	TopicPrefix               string
	RunID                     string
	MessagesPerSecond         int
	MaxMessagesPerSecond      int
	RateRampStep              int
	RateRampInterval          time.Duration
	DispatchInterval          time.Duration
	ProducerConcurrency       int
	ConsumerConcurrency       int
	PayloadBytes              int
	Partitions                int
	ReplicationFactor         int
	MaxInFlightPerPartition   int64
	MaxAckedAheadPerPartition int64
	Retention                 time.Duration
	VisibilityTimeout         time.Duration
	ConsumeWait               time.Duration
	MissingAfter              time.Duration
	ConsumedMarkerTTL         time.Duration
	MaxConsumedMarkers        int
	MaxOutstandingMessages    int64
	LedgerScanInterval        time.Duration
	RequestTimeout            time.Duration
	ReadyTimeout              time.Duration
	MetricsAddr               string
	Duration                  time.Duration
	MaxMessages               int64
	DrainTimeout              time.Duration
	CreateTopics              bool
	CleanupTopics             bool
}

func parseConfig(args []string) (config, error) {
	runID := defaultRunID()
	cfg := config{
		RunID:                     runID,
		Nodes:                     []string{"http://127.0.0.1:18081", "http://127.0.0.1:18082", "http://127.0.0.1:18083"},
		Topics:                    10,
		TopicPrefix:               "narad-soak",
		MessagesPerSecond:         1000,
		RateRampInterval:          10 * time.Minute,
		DispatchInterval:          time.Millisecond,
		ProducerConcurrency:       16,
		ConsumerConcurrency:       32,
		PayloadBytes:              512,
		Partitions:                12,
		ReplicationFactor:         2,
		MaxInFlightPerPartition:   1024,
		MaxAckedAheadPerPartition: 1024,
		Retention:                 24 * time.Hour,
		VisibilityTimeout:         30 * time.Second,
		ConsumeWait:               time.Second,
		MissingAfter:              5 * time.Minute,
		ConsumedMarkerTTL:         time.Hour,
		MaxConsumedMarkers:        1_000_000,
		MaxOutstandingMessages:    1_000_000,
		LedgerScanInterval:        10 * time.Second,
		RequestTimeout:            15 * time.Second,
		ReadyTimeout:              time.Minute,
		MetricsAddr:               "127.0.0.1:9095",
		DrainTimeout:              2 * time.Minute,
		CreateTopics:              true,
	}

	fs := flag.NewFlagSet("narad-tester", flag.ContinueOnError)
	nodes := strings.Join(cfg.Nodes, ",")
	fs.StringVar(&nodes, "nodes", nodes, "comma-separated Narad HTTP API nodes")
	fs.IntVar(&cfg.Topics, "topics", cfg.Topics, "number of topics to create and exercise")
	fs.StringVar(&cfg.TopicPrefix, "topic-prefix", cfg.TopicPrefix, "topic name prefix")
	fs.StringVar(&cfg.RunID, "run-id", cfg.RunID, "stable run id used in topic names and message ids")
	fs.IntVar(&cfg.MessagesPerSecond, "messages-per-second", cfg.MessagesPerSecond, "target aggregate produce rate")
	fs.IntVar(&cfg.MaxMessagesPerSecond, "max-messages-per-second", cfg.MaxMessagesPerSecond, "maximum aggregate produce rate to ramp up to and hold; 0 means no cap")
	fs.IntVar(&cfg.RateRampStep, "rate-ramp-step", cfg.RateRampStep, "messages-per-second to add every rate-ramp-interval; 0 disables ramping")
	fs.DurationVar(&cfg.RateRampInterval, "rate-ramp-interval", cfg.RateRampInterval, "interval between produce rate ramp steps")
	fs.DurationVar(&cfg.DispatchInterval, "dispatch-interval", cfg.DispatchInterval, "interval between producer dispatch ticks")
	fs.IntVar(&cfg.ProducerConcurrency, "producer-concurrency", cfg.ProducerConcurrency, "number of producer workers")
	fs.IntVar(&cfg.ConsumerConcurrency, "consumer-concurrency", cfg.ConsumerConcurrency, "number of consumer workers")
	fs.IntVar(&cfg.PayloadBytes, "payload-bytes", cfg.PayloadBytes, "bytes to put in the payload field of each message")
	fs.IntVar(&cfg.Partitions, "partitions", cfg.Partitions, "partitions per created topic")
	fs.IntVar(&cfg.ReplicationFactor, "replication-factor", cfg.ReplicationFactor, "replication factor per created topic")
	fs.Int64Var(&cfg.MaxInFlightPerPartition, "max-in-flight-per-partition", cfg.MaxInFlightPerPartition, "topic max in-flight reservations per partition")
	fs.Int64Var(&cfg.MaxAckedAheadPerPartition, "max-acked-ahead-per-partition", cfg.MaxAckedAheadPerPartition, "topic max acked-ahead offsets per partition")
	fs.DurationVar(&cfg.Retention, "retention", cfg.Retention, "topic retention age")
	fs.DurationVar(&cfg.VisibilityTimeout, "visibility-timeout", cfg.VisibilityTimeout, "topic visibility timeout")
	fs.DurationVar(&cfg.ConsumeWait, "consume-wait", cfg.ConsumeWait, "long-poll wait used by consume requests")
	fs.DurationVar(&cfg.MissingAfter, "missing-after", cfg.MissingAfter, "age after which an unconsumed produced message is counted as missing")
	fs.DurationVar(&cfg.ConsumedMarkerTTL, "consumed-marker-ttl", cfg.ConsumedMarkerTTL, "deprecated; ignored because duplicate detection uses exact sequence tracking")
	fs.IntVar(&cfg.MaxConsumedMarkers, "max-consumed-markers", cfg.MaxConsumedMarkers, "deprecated; ignored because duplicate detection uses exact sequence tracking")
	fs.Int64Var(&cfg.MaxOutstandingMessages, "max-outstanding-messages", cfg.MaxOutstandingMessages, "maximum produced-but-not-consumed messages retained before throttling new produces; 0 disables the cap")
	fs.DurationVar(&cfg.LedgerScanInterval, "ledger-scan-interval", cfg.LedgerScanInterval, "interval for ledger compaction and lag gauges")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "per-request HTTP timeout")
	fs.DurationVar(&cfg.ReadyTimeout, "ready-timeout", cfg.ReadyTimeout, "maximum time to wait for all configured nodes to become ready")
	fs.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "tester Prometheus listen address")
	fs.DurationVar(&cfg.Duration, "duration", 0, "run duration; 0 means run until interrupted")
	fs.Int64Var(&cfg.MaxMessages, "max-messages", cfg.MaxMessages, "maximum produce attempts to dispatch; 0 means run until interrupted or duration expires")
	fs.DurationVar(&cfg.DrainTimeout, "drain-timeout", cfg.DrainTimeout, "after max-messages is dispatched, wait this long for outstanding messages to be consumed; 0 disables drain wait")
	fs.BoolVar(&cfg.CreateTopics, "create-topics", cfg.CreateTopics, "create the run topics before producing")
	fs.BoolVar(&cfg.CleanupTopics, "cleanup-topics", cfg.CleanupTopics, "delete created topics on shutdown")
	ignoredLedgerPath := ""
	ignoredPendingTTL := 10 * time.Minute
	fs.StringVar(&ignoredLedgerPath, "ledger-path", ignoredLedgerPath, "deprecated; ignored because the tester ledger is in-memory")
	fs.DurationVar(&ignoredPendingTTL, "pending-ttl", ignoredPendingTTL, "deprecated; ignored because the tester records only successful produces")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	parsedNodes, err := parseNodes(nodes)
	if err != nil {
		return config{}, err
	}
	cfg.Nodes = parsedNodes
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (c config) validate() error {
	checks := []struct {
		ok  bool
		msg string
	}{
		{len(c.Nodes) > 0, "at least one node is required"},
		{c.Topics > 0, "topics must be > 0"},
		{strings.TrimSpace(c.TopicPrefix) != "", "topic-prefix is required"},
		{strings.TrimSpace(c.RunID) != "", "run-id is required"},
		{c.MessagesPerSecond > 0, "messages-per-second must be > 0"},
		{c.MaxMessagesPerSecond >= 0, "max-messages-per-second must be >= 0"},
		{c.RateRampStep >= 0, "rate-ramp-step must be >= 0"},
		{c.RateRampInterval > 0, "rate-ramp-interval must be > 0"},
		{c.DispatchInterval > 0, "dispatch-interval must be > 0"},
		{c.ProducerConcurrency > 0, "producer-concurrency must be > 0"},
		{c.ConsumerConcurrency >= 0, "consumer-concurrency must be >= 0"},
		{c.PayloadBytes >= 0, "payload-bytes must be >= 0"},
		{c.Partitions >= 3, "partitions must be >= 3"},
		{c.ReplicationFactor >= 2, "replication-factor must be >= 2"},
		{c.MaxInFlightPerPartition >= 0, "max-in-flight-per-partition must be >= 0"},
		{c.MaxAckedAheadPerPartition >= 0, "max-acked-ahead-per-partition must be >= 0"},
		{c.Retention >= 0, "retention must be >= 0"},
		{c.VisibilityTimeout > 0, "visibility-timeout must be > 0"},
		{c.ConsumeWait >= 0, "consume-wait must be >= 0"},
		{c.MissingAfter > 0, "missing-after must be > 0"},
		{c.ConsumedMarkerTTL >= 0, "consumed-marker-ttl must be >= 0"},
		{c.MaxConsumedMarkers >= 0, "max-consumed-markers must be >= 0"},
		{c.MaxOutstandingMessages >= 0, "max-outstanding-messages must be >= 0"},
		{c.LedgerScanInterval > 0, "ledger-scan-interval must be > 0"},
		{c.RequestTimeout > 0, "request-timeout must be > 0"},
		{c.ReadyTimeout > 0, "ready-timeout must be > 0"},
		{strings.TrimSpace(c.MetricsAddr) != "", "metrics-addr is required"},
		{c.MaxMessages >= 0, "max-messages must be >= 0"},
		{c.DrainTimeout >= 0, "drain-timeout must be >= 0"},
	}
	for _, check := range checks {
		if !check.ok {
			return errors.New(check.msg)
		}
	}
	if c.MaxMessagesPerSecond > 0 && c.MaxMessagesPerSecond < c.MessagesPerSecond {
		return errors.New("max-messages-per-second must be >= messages-per-second")
	}
	return nil
}

func (c config) topicNames() []string {
	topics := make([]string, c.Topics)
	for i := range topics {
		topics[i] = fmt.Sprintf("%s-%s-%02d", c.TopicPrefix, c.RunID, i+1)
	}
	return topics
}

func (c config) payloadPadding() string {
	if c.PayloadBytes == 0 {
		return ""
	}
	return strings.Repeat("x", c.PayloadBytes)
}

func parseNodes(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	nodes := make([]string, 0, len(parts))
	for _, part := range parts {
		node := strings.TrimSpace(part)
		if node == "" {
			continue
		}
		if !strings.HasPrefix(node, "http://") && !strings.HasPrefix(node, "https://") {
			node = "http://" + node
		}
		parsed, err := url.Parse(node)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", part, err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("node %q: expected http(s)://host", part)
		}
		nodes = append(nodes, strings.TrimRight(node, "/"))
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("nodes is empty")
	}
	return nodes, nil
}

func defaultRunID() string {
	return fmt.Sprintf("%s-%d", time.Now().UTC().Format("20060102T150405Z"), os.Getpid())
}
