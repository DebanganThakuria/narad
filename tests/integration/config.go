package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"
)

func parseConfig(args []string) (config, error) {
	var nodesCSV string
	cfg := config{mode: modeLoad}
	flagSet := flag.NewFlagSet("local-cluster-driver", flag.ContinueOnError)
	flagSet.StringVar(&cfg.mode, "mode", modeLoad, "driver mode: load, chaos")
	flagSet.StringVar(&nodesCSV, "nodes", "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083", "comma-separated Narad node base URLs")
	flagSet.IntVar(&cfg.topics, "topics", 10, "number of topics to create")
	flagSet.IntVar(&cfg.messages, "messages", 1000, "total messages to produce and consume")
	flagSet.IntVar(&cfg.partitions, "partitions", 6, "partitions per topic")
	flagSet.IntVar(&cfg.produceConcurrency, "produce-concurrency", 32, "concurrent producer workers")
	flagSet.IntVar(&cfg.consumeConcurrency, "consume-concurrency", 32, "concurrent consumer workers")
	flagSet.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "overall driver timeout")
	flagSet.DurationVar(&cfg.assignmentTimeout, "assignment-timeout", 20*time.Second, "maximum time to wait for topic assignments to become visible")
	flagSet.DurationVar(&cfg.visibilityTimeout, "visibility-timeout", 30*time.Second, "topic visibility timeout")
	flagSet.StringVar(&cfg.runID, "run-id", "", "topic/message run id; defaults to timestamp")
	flagSet.BoolVar(&cfg.cleanup, "cleanup", true, "delete created topics at the end")
	flagSet.StringVar(&cfg.username, "username", "", "HTTP Basic auth username (empty = no auth)")
	flagSet.StringVar(&cfg.password, "password", "", "HTTP Basic auth password")
	if err := flagSet.Parse(args); err != nil {
		return cfg, err
	}

	if !validMode(cfg.mode) {
		return cfg, fmt.Errorf("invalid --mode %q", cfg.mode)
	}
	cfg.nodes = splitNodes(nodesCSV)
	if len(cfg.nodes) == 0 {
		return cfg, errors.New("at least one node is required")
	}
	if cfg.topics <= 0 {
		return cfg, errors.New("--topics must be > 0")
	}
	if cfg.messages <= 0 {
		return cfg, errors.New("--messages must be > 0")
	}
	if cfg.partitions < 3 {
		return cfg, errors.New("--partitions must be >= 3")
	}
	if cfg.produceConcurrency <= 0 || cfg.consumeConcurrency <= 0 {
		return cfg, errors.New("concurrency values must be > 0")
	}
	if cfg.timeout <= 0 {
		return cfg, errors.New("--timeout must be > 0")
	}
	if cfg.assignmentTimeout <= 0 {
		return cfg, errors.New("--assignment-timeout must be > 0")
	}
	if cfg.visibilityTimeout <= 0 {
		return cfg, errors.New("--visibility-timeout must be > 0")
	}
	if cfg.runID == "" {
		cfg.runID = fmt.Sprintf("lc-%d", time.Now().UnixNano())
	}
	return cfg, nil
}

func splitNodes(nodesCSV string) []string {
	parts := strings.Split(nodesCSV, ",")
	nodes := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimRight(strings.TrimSpace(part), "/")
		if part == "" {
			continue
		}
		if !strings.HasPrefix(part, "http://") && !strings.HasPrefix(part, "https://") {
			part = "http://" + part
		}
		nodes = append(nodes, part)
	}
	return nodes
}
