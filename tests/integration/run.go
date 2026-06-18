package main

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"
)

const (
	modeLoad                  = "load"
	modeChaos                 = "chaos"
	modePrepareOwnerRepair    = "prepare-owner-repair"
	modeVerifyOwnerRepair     = "verify-owner-repair"
	modePrepareFollowerRepair = "prepare-follower-repair"
	modeVerifyFollowerRepair  = "verify-follower-repair"
)

func validMode(mode string) bool {
	switch mode {
	case modeLoad, modeChaos, modePrepareOwnerRepair, modeVerifyOwnerRepair, modePrepareFollowerRepair, modeVerifyFollowerRepair:
		return true
	default:
		return false
	}
}

func modeRequiresPlan(mode string) bool {
	switch mode {
	case modePrepareOwnerRepair, modeVerifyOwnerRepair, modePrepareFollowerRepair, modeVerifyFollowerRepair:
		return true
	default:
		return false
	}
}

func run(cfg config) error {
	if cfg.mode == modeChaos {
		return runChaos(cfg)
	}
	if cfg.mode != modeLoad {
		return runRecoveryMode(cfg)
	}
	return runLoad(cfg)
}

func runLoad(cfg config) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	lb := &roundRobinClient{
		nodes:  cfg.nodes,
		client: &http.Client{Timeout: 15 * time.Second},
	}

	loadTopics := topicNames(cfg)
	recoveryTopic := cfg.runID + "-recovery"
	allTopics := append(append([]string(nil), loadTopics...), recoveryTopic)
	jobs := messageJobs(cfg, loadTopics)
	expected := make(map[string]messageJob, len(jobs))
	for _, job := range jobs {
		expected[job.Body.ID] = job
	}

	fmt.Printf("nodes: %s\n", strings.Join(cfg.nodes, ", "))
	fmt.Printf("run_id: %s\n", cfg.runID)
	fmt.Printf("creating topics: %d load topics + 1 recovery topic x %d partitions rf=%d\n", len(loadTopics), cfg.partitions, cfg.replicationFactor)
	if err := verifyReady(ctx, lb); err != nil {
		return err
	}
	if err := createTopics(ctx, lb, cfg, allTopics); err != nil {
		return err
	}
	fmt.Printf("verifying topic assignments: timeout=%s\n", cfg.assignmentTimeout)
	if err := verifyTopicsReady(ctx, lb, cfg, allTopics); err != nil {
		return err
	}
	fmt.Printf("verifying schema rejection\n")
	if err := verifySchemaRejection(ctx, lb, loadTopics[0]); err != nil {
		return err
	}
	fmt.Printf("verifying committed replica recovery reads\n")
	if err := verifyCommittedReplicaRead(ctx, lb, cfg, recoveryTopic); err != nil {
		return err
	}

	stats := &runStats{}
	fmt.Printf("producing messages: %d messages concurrency=%d\n", len(jobs), cfg.produceConcurrency)
	if err := produceMessages(ctx, lb, jobs, cfg.produceConcurrency, stats); err != nil {
		return err
	}

	fmt.Printf("consuming messages: target=%d concurrency=%d\n", len(jobs), cfg.consumeConcurrency)
	if err := consumeAndAck(ctx, lb, loadTopics, expected, cfg.consumeConcurrency, stats); err != nil {
		return err
	}
	if err := verifyDrained(ctx, lb, loadTopics); err != nil {
		return err
	}
	if cfg.cleanup {
		if err := deleteTopics(ctx, lb, allTopics); err != nil {
			return err
		}
		if err := verifyDeleted(ctx, lb, cfg.nodes, allTopics); err != nil {
			return err
		}
	}

	elapsed := time.Since(start)
	throughput := float64(stats.acked.Load()) / math.Max(elapsed.Seconds(), 0.001)
	fmt.Printf("PASS topics=%d produced=%d consumed=%d acked=%d duration=%s acked_per_sec=%.1f\n",
		len(loadTopics), stats.produced.Load(), stats.consumed.Load(), stats.acked.Load(), elapsed.Round(time.Millisecond), throughput)
	return nil
}
