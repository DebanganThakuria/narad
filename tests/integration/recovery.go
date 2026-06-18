package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func verifyCommittedReplicaRead(ctx context.Context, lb *roundRobinClient, cfg config, topicName string) error {
	job := messageJob{
		Topic: topicName,
		Key:   "recovery-probe",
		Body: messageRecord{
			ID:       cfg.runID + "/" + topicName + "/recovery-probe",
			Topic:    topicName,
			Sequence: 0,
			Key:      "recovery-probe",
			RunID:    cfg.runID,
		},
	}

	produced, err := produceOne(ctx, lb, job)
	if err != nil {
		return fmt.Errorf("produce recovery probe: %w", err)
	}

	wantCopies := min(cfg.replicationFactor, len(lb.nodes))
	return retry(ctx, 20, 100*time.Millisecond, func() error {
		copies, err := committedReplicaCopies(ctx, lb, job.Body, produced)
		if err != nil {
			return err
		}
		if copies < wantCopies {
			return fmt.Errorf("committed replica copies = %d, want >= %d", copies, wantCopies)
		}
		return nil
	})
}

func committedReplicaCopies(ctx context.Context, lb *roundRobinClient, want messageRecord, produced produceResponse) (int, error) {
	path := "/internal/v1/replicate?topic=" + url.QueryEscape(want.Topic) +
		"&partition=" + strconv.Itoa(produced.Partition) +
		"&offset=" + strconv.FormatInt(produced.Offset, 10) +
		"&committed=true"

	copies := 0
	for _, node := range lb.nodes {
		var out replicaReadResponse
		status, _, err := lb.doTo(ctx, node, http.MethodGet, path, nil, &out, http.StatusOK, http.StatusNotFound)
		if err != nil {
			return 0, fmt.Errorf("read committed replica from %s: %w", node, err)
		}
		if status == http.StatusNotFound {
			continue
		}

		var got messageRecord
		if err := json.Unmarshal(out.Payload, &got); err != nil {
			return 0, fmt.Errorf("decode committed replica payload from %s: %w", node, err)
		}
		if got != want {
			return 0, fmt.Errorf("committed replica payload mismatch from %s: got %+v want %+v", node, got, want)
		}
		copies++
	}
	return copies, nil
}
