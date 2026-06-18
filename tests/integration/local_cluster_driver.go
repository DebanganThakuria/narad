package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	nodes              []string
	topics             int
	messages           int
	partitions         int
	replicationFactor  int
	produceConcurrency int
	consumeConcurrency int
	timeout            time.Duration
	assignmentTimeout  time.Duration
	runID              string
	cleanup            bool
}

type roundRobinClient struct {
	nodes  []string
	client *http.Client
	next   atomic.Uint64
}

type topicRecord struct {
	Name                      string          `json:"name"`
	Partitions                int             `json:"partitions"`
	ReplicationFactor         int             `json:"replication_factor"`
	RetentionMs               int64           `json:"retention_ms"`
	VisibilityTimeoutMs       int64           `json:"visibility_timeout_ms"`
	MaxInFlightPerPartition   int64           `json:"max_in_flight_per_partition"`
	MaxAckedAheadPerPartition int64           `json:"max_acked_ahead_per_partition"`
	Schema                    json.RawMessage `json:"schema,omitempty"`
}

type listTopicsResponse struct {
	Topics []topicRecord `json:"topics"`
}

type topicDetailsResponse struct {
	topicRecord
	PartitionStats []partitionStats `json:"partition_stats"`
}

type partitionStats struct {
	Index int `json:"index"`
}

type produceRequest struct {
	Key     string        `json:"key"`
	Message messageRecord `json:"message"`
}

type messageRecord struct {
	ID       string `json:"id"`
	Topic    string `json:"topic"`
	Sequence int    `json:"sequence"`
	Key      string `json:"key"`
	RunID    string `json:"run_id"`
}

type produceResponse struct {
	Offset    int64 `json:"offset"`
	Partition int   `json:"partition"`
}

type consumeResponse struct {
	Topic         string        `json:"topic"`
	Partition     int           `json:"partition"`
	Offset        int64         `json:"offset"`
	Payload       messageRecord `json:"payload"`
	ReceiptHandle string        `json:"receipt_handle"`
}

type messageJob struct {
	Topic string
	Key   string
	Body  messageRecord
}

type runStats struct {
	produced atomic.Int64
	consumed atomic.Int64
	acked    atomic.Int64
}

const messageSchema = `{
  "type": "object",
  "properties": {
    "id":       { "type": "string" },
    "topic":    { "type": "string" },
    "sequence": { "type": "integer" },
    "key":      { "type": "string" },
    "run_id":   { "type": "string" }
  },
  "required": ["id", "topic", "sequence", "key", "run_id"],
  "additionalProperties": false
}`

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "local cluster integration failed:", err)
		os.Exit(1)
	}
}

func parseConfig(args []string) (config, error) {
	var nodesCSV string
	cfg := config{}
	flagSet := flag.NewFlagSet("local-cluster-driver", flag.ContinueOnError)
	flagSet.StringVar(&nodesCSV, "nodes", "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083", "comma-separated Narad node base URLs")
	flagSet.IntVar(&cfg.topics, "topics", 10, "number of topics to create")
	flagSet.IntVar(&cfg.messages, "messages", 1000, "total messages to produce and consume")
	flagSet.IntVar(&cfg.partitions, "partitions", 6, "partitions per topic")
	flagSet.IntVar(&cfg.replicationFactor, "replication-factor", 2, "replication factor per topic")
	flagSet.IntVar(&cfg.produceConcurrency, "produce-concurrency", 32, "concurrent producer workers")
	flagSet.IntVar(&cfg.consumeConcurrency, "consume-concurrency", 32, "concurrent consumer workers")
	flagSet.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "overall driver timeout")
	flagSet.DurationVar(&cfg.assignmentTimeout, "assignment-timeout", 20*time.Second, "maximum time to wait for topic assignments to become visible")
	flagSet.StringVar(&cfg.runID, "run-id", "", "topic/message run id; defaults to timestamp")
	flagSet.BoolVar(&cfg.cleanup, "cleanup", true, "delete created topics at the end")
	if err := flagSet.Parse(args); err != nil {
		return cfg, err
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
	if cfg.replicationFactor < 2 {
		return cfg, errors.New("--replication-factor must be >= 2")
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

func run(cfg config) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	lb := &roundRobinClient{
		nodes:  cfg.nodes,
		client: &http.Client{Timeout: 15 * time.Second},
	}

	topics := topicNames(cfg)
	jobs := messageJobs(cfg, topics)
	expected := make(map[string]messageJob, len(jobs))
	for _, job := range jobs {
		expected[job.Body.ID] = job
	}

	fmt.Printf("nodes: %s\n", strings.Join(cfg.nodes, ", "))
	fmt.Printf("run_id: %s\n", cfg.runID)
	fmt.Printf("creating topics: %d topics x %d partitions rf=%d\n", len(topics), cfg.partitions, cfg.replicationFactor)
	if err := verifyReady(ctx, lb); err != nil {
		return err
	}
	if err := createTopics(ctx, lb, cfg, topics); err != nil {
		return err
	}
	fmt.Printf("verifying topic assignments: timeout=%s\n", cfg.assignmentTimeout)
	if err := verifyTopicsReady(ctx, lb, cfg, topics); err != nil {
		return err
	}
	fmt.Printf("verifying schema rejection\n")
	if err := verifySchemaRejection(ctx, lb, topics[0]); err != nil {
		return err
	}

	stats := &runStats{}
	fmt.Printf("producing messages: %d messages concurrency=%d\n", len(jobs), cfg.produceConcurrency)
	if err := produceMessages(ctx, lb, jobs, cfg.produceConcurrency, stats); err != nil {
		return err
	}

	fmt.Printf("consuming messages: target=%d concurrency=%d\n", len(jobs), cfg.consumeConcurrency)
	if err := consumeAndAck(ctx, lb, topics, expected, cfg.consumeConcurrency, stats); err != nil {
		return err
	}
	if err := verifyDrained(ctx, lb, topics); err != nil {
		return err
	}
	if cfg.cleanup {
		if err := deleteTopics(ctx, lb, topics); err != nil {
			return err
		}
		if err := verifyDeleted(ctx, lb, cfg.nodes, topics); err != nil {
			return err
		}
	}

	elapsed := time.Since(start)
	throughput := float64(stats.acked.Load()) / math.Max(elapsed.Seconds(), 0.001)
	fmt.Printf("PASS topics=%d produced=%d consumed=%d acked=%d duration=%s acked_per_sec=%.1f\n",
		len(topics), stats.produced.Load(), stats.consumed.Load(), stats.acked.Load(), elapsed.Round(time.Millisecond), throughput)
	return nil
}

func topicNames(cfg config) []string {
	topics := make([]string, cfg.topics)
	for i := range topics {
		topics[i] = fmt.Sprintf("%s-%02d", cfg.runID, i)
	}
	return topics
}

func messageJobs(cfg config, topics []string) []messageJob {
	jobs := make([]messageJob, 0, cfg.messages)
	seqByTopic := make(map[string]int, len(topics))
	for i := 0; i < cfg.messages; i++ {
		topicName := topics[i%len(topics)]
		seq := seqByTopic[topicName]
		seqByTopic[topicName]++
		key := fmt.Sprintf("key-%04d", i%max(cfg.partitions, 1))
		id := fmt.Sprintf("%s/%s/%06d", cfg.runID, topicName, seq)
		jobs = append(jobs, messageJob{
			Topic: topicName,
			Key:   key,
			Body: messageRecord{
				ID:       id,
				Topic:    topicName,
				Sequence: seq,
				Key:      key,
				RunID:    cfg.runID,
			},
		})
	}
	return jobs
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
			ReplicationFactor:         cfg.replicationFactor,
			RetentionMs:               int64((1 * time.Hour) / time.Millisecond),
			VisibilityTimeoutMs:       int64((30 * time.Second) / time.Millisecond),
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

func produceMessages(ctx context.Context, lb *roundRobinClient, jobs []messageJob, concurrency int, stats *runStats) error {
	jobCh := make(chan messageJob)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for range concurrency {
		wg.Go(func() {
			for job := range jobCh {
				if err := produceOne(ctx, lb, job); err != nil {
					sendErr(errCh, err)
					return
				}
				stats.produced.Add(1)
			}
		})
	}

	go func() {
		defer close(jobCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobCh <- job:
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return firstErr(errCh)
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func produceOne(ctx context.Context, lb *roundRobinClient, job messageJob) error {
	path := "/v1/topics/" + url.PathEscape(job.Topic) + "/produce"
	req := produceRequest{Key: job.Key, Message: job.Body}
	var out produceResponse
	return retry(ctx, 20, 100*time.Millisecond, func() error {
		_, _, err := lb.do(ctx, http.MethodPost, path, req, &out, http.StatusOK)
		if err != nil {
			return err
		}
		if out.Partition < 0 {
			return fmt.Errorf("produce %s returned invalid partition %d", job.Body.ID, out.Partition)
		}
		return nil
	})
}

func consumeAndAck(ctx context.Context, lb *roundRobinClient, topics []string, expected map[string]messageJob, concurrency int, stats *runStats) error {
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var topicCursor atomic.Uint64
	var mu sync.Mutex
	seen := make(map[string]consumeResponse, len(expected))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for range concurrency {
		wg.Go(func() {
			for consumeCtx.Err() == nil {
				if int(stats.consumed.Load()) >= len(expected) {
					return
				}
				topicName := topics[int(topicCursor.Add(1)-1)%len(topics)]
				msg, found, err := consumeOne(consumeCtx, lb, topicName)
				if err != nil {
					if consumeCtx.Err() != nil && int(stats.consumed.Load()) >= len(expected) {
						return
					}
					sendErr(errCh, err)
					cancel()
					return
				}
				if !found {
					continue
				}
				job, ok := expected[msg.Payload.ID]
				if !ok {
					sendErr(errCh, fmt.Errorf("unexpected message id %q from topic %s", msg.Payload.ID, msg.Topic))
					cancel()
					return
				}
				if msg.Topic != job.Topic || msg.Payload.Topic != job.Topic || msg.Payload.Sequence != job.Body.Sequence {
					sendErr(errCh, fmt.Errorf("message mismatch for id %s: got topic=%s payload_topic=%s seq=%d", msg.Payload.ID, msg.Topic, msg.Payload.Topic, msg.Payload.Sequence))
					cancel()
					return
				}
				if msg.ReceiptHandle == "" {
					sendErr(errCh, fmt.Errorf("message %s missing receipt handle", msg.Payload.ID))
					cancel()
					return
				}

				mu.Lock()
				if _, exists := seen[msg.Payload.ID]; exists {
					mu.Unlock()
					sendErr(errCh, fmt.Errorf("duplicate delivery before completion: %s", msg.Payload.ID))
					cancel()
					return
				}
				seen[msg.Payload.ID] = msg
				mu.Unlock()

				if err := ackOne(consumeCtx, lb, msg.Topic, msg.ReceiptHandle); err != nil {
					sendErr(errCh, err)
					cancel()
					return
				}
				stats.consumed.Add(1)
				stats.acked.Add(1)
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			if err := firstErr(errCh); err != nil {
				return err
			}
			if len(seen) != len(expected) {
				return fmt.Errorf("consumed %d unique messages, want %d", len(seen), len(expected))
			}
			return nil
		case err := <-errCh:
			return err
		case <-ticker.C:
			if int(stats.consumed.Load()) >= len(expected) {
				cancel()
			}
		case <-ctx.Done():
			return fmt.Errorf("%w while consuming: consumed=%d want=%d", ctx.Err(), stats.consumed.Load(), len(expected))
		}
	}
}

func consumeOne(ctx context.Context, lb *roundRobinClient, topicName string) (consumeResponse, bool, error) {
	var out consumeResponse
	path := "/v1/topics/" + url.PathEscape(topicName) + "/consume?wait=500ms"
	status, _, err := lb.do(ctx, http.MethodGet, path, nil, &out, http.StatusOK, http.StatusNoContent)
	if err != nil {
		return out, false, err
	}
	if status == http.StatusNoContent {
		return out, false, nil
	}
	return out, true, nil
}

func ackOne(ctx context.Context, lb *roundRobinClient, topicName, receiptHandle string) error {
	path := "/v1/topics/" + url.PathEscape(topicName) + "/ack"
	body := map[string]string{"receipt_handle": receiptHandle}
	return retry(ctx, 5, 100*time.Millisecond, func() error {
		_, _, err := lb.do(ctx, http.MethodPost, path, body, nil, http.StatusNoContent)
		return err
	})
}

func verifyDrained(ctx context.Context, lb *roundRobinClient, topics []string) error {
	for _, topicName := range topics {
		path := "/v1/topics/" + url.PathEscape(topicName) + "/consume?wait=100ms"
		status, _, err := lb.do(ctx, http.MethodGet, path, nil, nil, http.StatusNoContent)
		if err != nil {
			return fmt.Errorf("drain check %s: %w", topicName, err)
		}
		if status != http.StatusNoContent {
			return fmt.Errorf("drain check %s returned status %d, want 204", topicName, status)
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

func (lb *roundRobinClient) do(ctx context.Context, method, path string, body any, out any, want ...int) (int, []byte, error) {
	node := lb.nodes[int(lb.next.Add(1)-1)%len(lb.nodes)]
	return lb.doTo(ctx, node, method, path, body, out, want...)
}

func (lb *roundRobinClient) doTo(ctx context.Context, node, method, path string, body any, out any, want ...int) (int, []byte, error) {
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
	resp, err := lb.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if !wantedStatus(resp.StatusCode, want) {
		return resp.StatusCode, respBody, fmt.Errorf("%s %s returned status %d: %s", method, node+path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if out != nil && len(bytes.TrimSpace(respBody)) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return resp.StatusCode, respBody, fmt.Errorf("decode %s %s: %w: %s", method, path, err, string(respBody))
		}
	}
	return resp.StatusCode, respBody, nil
}

func wantedStatus(status int, want []int) bool {
	return slices.Contains(want, status)
}

func retry(ctx context.Context, attempts int, baseDelay time.Duration, fn func() error) error {
	var last error
	for attempt := range attempts {
		if err := fn(); err != nil {
			last = err
		} else {
			return nil
		}
		delay := baseDelay * time.Duration(1<<min(attempt, 5))
		if err := sleepContext(ctx, delay); err != nil {
			if last != nil {
				return last
			}
			return err
		}
	}
	return last
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sendErr(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func firstErr(errCh <-chan error) error {
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
