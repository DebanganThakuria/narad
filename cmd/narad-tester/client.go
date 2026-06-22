package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/httpclient"
)

type apiClient struct {
	nodes   []string
	client  *http.Client
	metrics *testerMetrics
	next    atomic.Uint64
}

type apiStatusError struct {
	Method     string
	URL        string
	StatusCode int
	Body       []byte
}

func (e *apiStatusError) Error() string {
	return fmt.Sprintf("%s %s returned status %d: %s", e.Method, e.URL, e.StatusCode, strings.TrimSpace(string(e.Body)))
}

func newAPIClient(nodes []string, timeout time.Duration, metrics *testerMetrics) *apiClient {
	return &apiClient{
		nodes:   nodes,
		client:  httpclient.New(timeout),
		metrics: metrics,
	}
}

func (c *apiClient) waitReady(ctx context.Context, timeout time.Duration) error {
	for _, node := range c.nodes {
		deadline := time.Now().Add(timeout)
		for {
			if time.Now().After(deadline) {
				return fmt.Errorf("node %s did not become ready within %s", node, timeout)
			}
			status, _, err := c.doTo(ctx, node, http.MethodGet, "/readyz", "readyz", nil, nil, http.StatusOK)
			if err == nil && status == http.StatusOK {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	return nil
}

func (c *apiClient) createTopic(ctx context.Context, topic string, cfg config) error {
	req := createTopicRequest{
		Name:                      topic,
		Partitions:                cfg.Partitions,
		ReplicationFactor:         cfg.ReplicationFactor,
		RetentionMs:               cfg.Retention.Milliseconds(),
		VisibilityTimeoutMs:       cfg.VisibilityTimeout.Milliseconds(),
		MaxInFlightPerPartition:   cfg.MaxInFlightPerPartition,
		MaxAckedAheadPerPartition: cfg.MaxAckedAheadPerPartition,
		Schema:                    json.RawMessage(testerMessageSchema),
	}
	_, _, err := c.do(ctx, http.MethodPost, "/v1/topics", "create_topic", req, nil, http.StatusCreated, http.StatusConflict)
	return err
}

func (c *apiClient) deleteTopic(ctx context.Context, topic string) error {
	_, _, err := c.do(ctx, http.MethodDelete, "/v1/topics/"+url.PathEscape(topic), "delete_topic", nil, nil, http.StatusNoContent, http.StatusNotFound)
	return err
}

func (c *apiClient) produce(ctx context.Context, topic string, req produceRequest) (produceResponse, error) {
	out := produceResponse{Offset: -1}
	_, _, err := c.do(ctx, http.MethodPost, "/v1/topics/"+url.PathEscape(topic)+"/produce", "produce", req, &out, http.StatusAccepted)
	if err == nil {
		if out.Status != "accepted" || out.MessageID == "" || out.Topic != topic || out.Partition < 0 {
			err = fmt.Errorf("invalid produce response: %+v", out)
		}
	}
	return out, err
}

func (c *apiClient) consume(ctx context.Context, topic string, wait time.Duration) (consumeResponse, bool, error) {
	var out consumeResponse
	path := "/v1/topics/" + url.PathEscape(topic) + "/consume"
	if wait > 0 {
		path += "?wait=" + url.QueryEscape(wait.String())
	}
	status, _, err := c.do(ctx, http.MethodGet, path, "consume", nil, &out, http.StatusOK, http.StatusNoContent)
	if err != nil {
		return consumeResponse{}, false, err
	}
	if status == http.StatusNoContent {
		return consumeResponse{}, false, nil
	}
	return out, true, nil
}

func (c *apiClient) ack(ctx context.Context, topic, receiptHandle string) error {
	req := ackRequest{ReceiptHandle: receiptHandle}
	_, _, err := c.do(ctx, http.MethodPost, "/v1/topics/"+url.PathEscape(topic)+"/ack", "ack", req, nil, http.StatusNoContent)
	return err
}

func (c *apiClient) do(ctx context.Context, method, path, endpoint string, body any, out any, want ...int) (int, []byte, error) {
	node := c.nodes[int(c.next.Add(1)-1)%len(c.nodes)]
	return c.doTo(ctx, node, method, path, endpoint, body, out, want...)
}

func (c *apiClient) doTo(ctx context.Context, node, method, path, endpoint string, body any, out any, want ...int) (int, []byte, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reader = bytes.NewReader(encoded)
	}

	req, err := http.NewRequestWithContext(ctx, method, node+path, reader)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	if err != nil {
		c.metrics.observeNodeRequest(node, method, endpoint, "network", time.Since(start))
		return 0, nil, err
	}
	defer resp.Body.Close()
	statusLabel := strconv.Itoa(resp.StatusCode)
	c.metrics.observeNodeRequest(node, method, endpoint, statusLabel, time.Since(start))

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if !wantedStatus(resp.StatusCode, want) {
		return resp.StatusCode, respBody, &apiStatusError{
			Method:     method,
			URL:        node + path,
			StatusCode: resp.StatusCode,
			Body:       respBody,
		}
	}
	if out != nil && resp.StatusCode != http.StatusNoContent && len(bytes.TrimSpace(respBody)) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return resp.StatusCode, respBody, fmt.Errorf("decode %s %s: %w: %s", method, path, err, string(respBody))
		}
	}
	return resp.StatusCode, respBody, nil
}

func wantedStatus(status int, want []int) bool {
	return slices.Contains(want, status)
}
