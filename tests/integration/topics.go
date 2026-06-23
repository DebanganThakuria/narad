package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

func topicNames(cfg config) []string {
	topics := make([]string, cfg.topics)
	for i := range topics {
		topics[i] = fmt.Sprintf("%s-%02d", cfg.runID, i)
	}
	return topics
}

func verifyReady(ctx context.Context, lb *roundRobinClient) error {
	for _, node := range lb.nodes {
		if err := retry(ctx, 30, 250*time.Millisecond, func() error {
			status, _, err := lb.doTo(ctx, node, http.MethodGet, "/readyz", nil, nil, http.StatusOK)
			if err != nil {
				return err
			}
			if status != http.StatusOK {
				return fmt.Errorf("%s /readyz status %d", node, status)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("ready check %s: %w", node, err)
		}
	}
	return nil
}

func createTopics(ctx context.Context, lb *roundRobinClient, cfg config, topics []string) error {
	for _, topicName := range topics {
		req := topicRecord{
			Name:                      topicName,
			Partitions:                cfg.partitions,
			RetentionMs:               int64((1 * time.Hour) / time.Millisecond),
			VisibilityTimeoutMs:       int64(cfg.visibilityTimeout / time.Millisecond),
			MaxInFlightPerPartition:   4096,
			MaxAckedAheadPerPartition: 4096,
			Schema:                    json.RawMessage(messageSchema),
		}
		var created topicRecord
		if err := retry(ctx, 10, 200*time.Millisecond, func() error {
			_, _, err := lb.do(ctx, http.MethodPost, "/v1/topics", req, &created, http.StatusCreated, http.StatusConflict)
			return err
		}); err != nil {
			return fmt.Errorf("create topic %s: %w", topicName, err)
		}
	}
	return nil
}

func verifySchemaRejection(ctx context.Context, lb *roundRobinClient, topicName string) error {
	path := "/v1/topics/" + url.PathEscape(topicName) + "/produce"
	req := map[string]any{
		"key": "schema-reject",
		"message": map[string]any{
			"id":       123,
			"topic":    topicName,
			"sequence": "invalid",
			"key":      "schema-reject",
			"run_id":   "schema-reject",
		},
	}
	_, _, err := lb.do(ctx, http.MethodPost, path, req, nil, http.StatusBadRequest)
	return err
}

func verifyTopicsReady(ctx context.Context, lb *roundRobinClient, cfg config, topics []string) error {
	attempts := max(int(cfg.assignmentTimeout/(250*time.Millisecond)), 1)
	for _, node := range cfg.nodes {
		for _, topicName := range topics {
			var got topicDetailsResponse
			path := "/v1/topics/" + url.PathEscape(topicName)
			if err := retry(ctx, attempts, 250*time.Millisecond, func() error {
				_, _, err := lb.doTo(ctx, node, http.MethodGet, path, nil, &got, http.StatusOK)
				if err != nil {
					return err
				}
				if got.Name != topicName {
					return fmt.Errorf("got topic %q, want %q", got.Name, topicName)
				}
				if got.Partitions != cfg.partitions {
					return fmt.Errorf("topic %s partitions = %d, want %d", topicName, got.Partitions, cfg.partitions)
				}
				if len(got.PartitionStats) != cfg.partitions {
					return fmt.Errorf("topic %s has %d assigned partition stats on %s, want %d", topicName, len(got.PartitionStats), node, cfg.partitions)
				}
				return nil
			}); err != nil {
				return fmt.Errorf("topic %s not ready on %s: %w", topicName, node, err)
			}
		}
	}
	return nil
}

func verifyDrained(ctx context.Context, lb *roundRobinClient, topics []string) error {
	for _, topicName := range topics {
		path := "/v1/topics/" + url.PathEscape(topicName) + "/consume?wait=100ms"
		var notDrained error
		if err := retry(ctx, 20, 100*time.Millisecond, func() error {
			status, _, err := lb.do(ctx, http.MethodGet, path, nil, nil, http.StatusNoContent, http.StatusOK)
			if err != nil {
				return err
			}
			if status != http.StatusNoContent {
				notDrained = fmt.Errorf("drain check %s returned status %d, want 204", topicName, status)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("drain check %s: %w", topicName, err)
		}
		if notDrained != nil {
			return notDrained
		}
	}
	return nil
}

func deleteTopics(ctx context.Context, lb *roundRobinClient, topics []string) error {
	for _, topicName := range topics {
		path := "/v1/topics/" + url.PathEscape(topicName)
		if err := retry(ctx, 10, 200*time.Millisecond, func() error {
			_, _, err := lb.do(ctx, http.MethodDelete, path, nil, nil, http.StatusNoContent, http.StatusNotFound)
			return err
		}); err != nil {
			return fmt.Errorf("delete topic %s: %w", topicName, err)
		}
	}
	return nil
}

func verifyDeleted(ctx context.Context, lb *roundRobinClient, nodes []string, topics []string) error {
	for _, node := range nodes {
		if err := retry(ctx, 40, 250*time.Millisecond, func() error {
			var listed listTopicsResponse
			if _, _, err := lb.doTo(ctx, node, http.MethodGet, "/v1/topics?limit=1000", nil, &listed, http.StatusOK); err != nil {
				return err
			}
			left := make(map[string]struct{}, len(listed.Topics))
			for _, t := range listed.Topics {
				left[t.Name] = struct{}{}
			}
			for _, topicName := range topics {
				if _, ok := left[topicName]; ok {
					return fmt.Errorf("topic %s still listed", topicName)
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("verify deleted on %s: %w", node, err)
		}
	}
	return nil
}
