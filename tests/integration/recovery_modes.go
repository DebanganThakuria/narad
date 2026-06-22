package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func runRecoveryMode(cfg config) error {
	return fmt.Errorf("%s is unsupported by the WAL-first no-follower produce design", cfg.mode)
}

func prepareRecoveryPlan(ctx context.Context, lb *roundRobinClient, cfg config, topicName string) error {
	if err := verifyReady(ctx, lb); err != nil {
		return err
	}
	if err := createTopics(ctx, lb, cfg, []string{topicName}); err != nil {
		return err
	}
	if err := verifyTopicsReady(ctx, lb, cfg, []string{topicName}); err != nil {
		return err
	}

	plan, err := createRecoveryPlan(ctx, lb, cfg, topicName, 0)
	if err != nil {
		return err
	}
	if err := writeRecoveryPlan(cfg.planPath, plan); err != nil {
		return err
	}
	fmt.Printf("recovery plan: topic=%s partition=%d owner=%s follower=%s last_offset=%d\n",
		plan.Topic, plan.Partition, plan.OwnerNode, plan.FollowerNode, plan.LastOffset)
	return nil
}

func createRecoveryPlan(ctx context.Context, lb *roundRobinClient, cfg config, topicName string, partition int) (recoveryPlan, error) {
	ownerIdx, first, err := producePinnedToOwner(ctx, lb, cfg, topicName, partition, 0)
	if err != nil {
		return recoveryPlan{}, err
	}
	second, err := producePinnedToNode(ctx, lb, lb.nodes[ownerIdx], cfg, topicName, partition, 1)
	if err != nil {
		return recoveryPlan{}, fmt.Errorf("produce second recovery record on owner: %w", err)
	}
	if second.Offset != first.Offset+1 {
		return recoveryPlan{}, fmt.Errorf("second recovery offset = %d, want %d", second.Offset, first.Offset+1)
	}

	payload := recoveryPayload(cfg, topicName, 1)
	copyIndexes, err := waitCommittedReplicaCopies(ctx, lb, payload, second, min(cfg.replicationFactor, len(lb.nodes)))
	if err != nil {
		return recoveryPlan{}, err
	}
	followerIdx := -1
	for _, idx := range copyIndexes {
		if idx != ownerIdx {
			followerIdx = idx
			break
		}
	}
	if followerIdx < 0 {
		return recoveryPlan{}, fmt.Errorf("could not identify follower copy: owner=%d copies=%v", ownerIdx, copyIndexes)
	}

	return recoveryPlan{
		Topic:         topicName,
		Partition:     partition,
		OwnerIndex:    ownerIdx,
		FollowerIndex: followerIdx,
		OwnerNode:     lb.nodes[ownerIdx],
		FollowerNode:  lb.nodes[followerIdx],
		LastOffset:    second.Offset,
		Payload:       payload,
	}, nil
}

func producePinnedToOwner(ctx context.Context, lb *roundRobinClient, cfg config, topicName string, partition int, sequence int) (int, produceResponse, error) {
	var last error
	var ownerIdx int
	var produced produceResponse
	err := retry(ctx, 30, 250*time.Millisecond, func() error {
		for idx, node := range lb.nodes {
			out, err := producePinnedToNode(ctx, lb, node, cfg, topicName, partition, sequence)
			if err == nil {
				ownerIdx = idx
				produced = out
				return nil
			}
			last = err
		}
		return fmt.Errorf("no owner accepted pinned produce for partition %d: %w", partition, last)
	})
	return ownerIdx, produced, err
}

func producePinnedToNode(ctx context.Context, lb *roundRobinClient, node string, cfg config, topicName string, partition int, sequence int) (produceResponse, error) {
	path := "/v1/topics/" + url.PathEscape(topicName) + "/produce?partition=" + strconv.Itoa(partition)
	req := produceRequest{
		Key:     "recovery-partition-" + strconv.Itoa(partition),
		Message: recoveryPayload(cfg, topicName, sequence),
	}
	out := produceResponse{Offset: -1}
	status, _, err := lb.doTo(ctx, node, http.MethodPost, path, req, &out, http.StatusAccepted, http.StatusMisdirectedRequest, http.StatusServiceUnavailable)
	if err != nil {
		return produceResponse{}, err
	}
	if status != http.StatusAccepted {
		return produceResponse{}, fmt.Errorf("%s pinned produce returned status %d", node, status)
	}
	if out.Status != "accepted" || out.MessageID == "" || out.Topic != topicName {
		return produceResponse{}, fmt.Errorf("%s pinned produce returned invalid accepted response: %+v", node, out)
	}
	if out.Partition != partition {
		return produceResponse{}, fmt.Errorf("%s pinned produce partition = %d, want %d", node, out.Partition, partition)
	}
	return out, nil
}

func recoveryPayload(cfg config, topicName string, sequence int) messageRecord {
	return messageRecord{
		ID:       fmt.Sprintf("%s/%s/recovery/%06d", cfg.runID, topicName, sequence),
		Topic:    topicName,
		Sequence: sequence,
		Key:      "recovery-partition-0",
		RunID:    cfg.runID,
	}
}

func waitCommittedReplicaCopies(ctx context.Context, lb *roundRobinClient, want messageRecord, produced produceResponse, wantCopies int) ([]int, error) {
	var copyIndexes []int
	err := retry(ctx, 20, 100*time.Millisecond, func() error {
		var err error
		copyIndexes, err = committedReplicaCopyIndexes(ctx, lb, want, produced)
		if err != nil {
			return err
		}
		if len(copyIndexes) < wantCopies {
			return fmt.Errorf("committed replica copies = %d, want >= %d", len(copyIndexes), wantCopies)
		}
		return nil
	})
	return copyIndexes, err
}

func committedReplicaCopyIndexes(ctx context.Context, lb *roundRobinClient, want messageRecord, produced produceResponse) ([]int, error) {
	path := "/internal/v1/replicate?topic=" + url.QueryEscape(want.Topic) +
		"&partition=" + strconv.Itoa(produced.Partition) +
		"&offset=" + strconv.FormatInt(produced.Offset, 10) +
		"&committed=true"

	var copyIndexes []int
	for idx, node := range lb.nodes {
		var out replicaReadResponse
		status, _, err := lb.doTo(ctx, node, http.MethodGet, path, nil, &out, http.StatusOK, http.StatusNotFound)
		if err != nil {
			return nil, fmt.Errorf("read committed replica from %s: %w", node, err)
		}
		if status == http.StatusNotFound {
			continue
		}
		var got messageRecord
		if err := json.Unmarshal(out.Payload, &got); err != nil {
			return nil, fmt.Errorf("decode committed replica payload from %s: %w", node, err)
		}
		if got != want {
			return nil, fmt.Errorf("committed replica payload mismatch from %s: got %+v want %+v", node, got, want)
		}
		copyIndexes = append(copyIndexes, idx)
	}
	return copyIndexes, nil
}

func writeRecoveryPlan(path string, plan recoveryPlan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func readRecoveryPlan(path string) (recoveryPlan, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return recoveryPlan{}, err
	}
	var plan recoveryPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return recoveryPlan{}, err
	}
	return plan, nil
}

func verifyRecoveryPlan(ctx context.Context, lb *roundRobinClient, path string, target string) error {
	plan, err := readRecoveryPlan(path)
	if err != nil {
		return fmt.Errorf("read recovery plan: %w", err)
	}
	targetNode := plan.OwnerNode
	if target == "follower" {
		targetNode = plan.FollowerNode
	}
	produced := produceResponse{Offset: plan.LastOffset, Partition: plan.Partition}
	return retry(ctx, 30, 250*time.Millisecond, func() error {
		if err := verifyCommittedReplicaOnNode(ctx, lb, targetNode, plan.Payload, produced); err != nil {
			return fmt.Errorf("verify %s repair on %s: %w", target, targetNode, err)
		}
		return nil
	})
}

func verifyCommittedReplicaOnNode(ctx context.Context, lb *roundRobinClient, node string, want messageRecord, produced produceResponse) error {
	path := "/internal/v1/replicate?topic=" + url.QueryEscape(want.Topic) +
		"&partition=" + strconv.Itoa(produced.Partition) +
		"&offset=" + strconv.FormatInt(produced.Offset, 10) +
		"&committed=true"
	var out replicaReadResponse
	_, _, err := lb.doTo(ctx, node, http.MethodGet, path, nil, &out, http.StatusOK)
	if err != nil {
		return err
	}
	var got messageRecord
	if err := json.Unmarshal(out.Payload, &got); err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("payload = %+v, want %+v", got, want)
	}
	return nil
}
