package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

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

func produceMessages(ctx context.Context, lb *roundRobinClient, jobs []messageJob, concurrency int, stats *runStats) error {
	jobCh := make(chan messageJob)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for range concurrency {
		wg.Go(func() {
			for job := range jobCh {
				if _, err := produceOne(ctx, lb, job); err != nil {
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

func produceOne(ctx context.Context, lb *roundRobinClient, job messageJob) (produceResponse, error) {
	path := "/v1/topics/" + url.PathEscape(job.Topic) + "/produce"
	req := produceRequest{Key: job.Key, Message: job.Body}
	out := produceResponse{Offset: -1}
	err := retry(ctx, 20, 100*time.Millisecond, func() error {
		_, _, err := lb.do(ctx, http.MethodPost, path, req, &out, http.StatusAccepted)
		if err != nil {
			return err
		}
		if out.Status != "accepted" || out.MessageID == "" || out.Topic != job.Topic {
			return fmt.Errorf("produce %s returned invalid accepted response: %+v", job.Body.ID, out)
		}
		if out.Partition < 0 {
			return fmt.Errorf("produce %s returned invalid partition %d", job.Body.ID, out.Partition)
		}
		return nil
	})
	return out, err
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
	var status int
	err := retry(ctx, 40, 100*time.Millisecond, func() error {
		gotStatus, body, err := lb.do(ctx, http.MethodGet, path, nil, nil, http.StatusOK, http.StatusNoContent, http.StatusMisdirectedRequest)
		if err != nil {
			return err
		}
		if gotStatus == http.StatusMisdirectedRequest {
			return fmt.Errorf("consume %s misdirected: %s", topicName, string(body))
		}
		status = gotStatus
		if gotStatus == http.StatusOK {
			var attempt consumeResponse
			if err := json.Unmarshal(body, &attempt); err != nil {
				return fmt.Errorf("decode consume %s: %w: %s", topicName, err, string(body))
			}
			out = attempt
		}
		return nil
	})
	if err != nil {
		return out, false, err
	}
	if status == http.StatusNoContent {
		return out, false, nil
	}
	return out, true, nil
}

func ackOne(ctx context.Context, lb *roundRobinClient, topicName, receiptHandle string) error {
	_, err := ackOneStatus(ctx, lb, topicName, receiptHandle, 5, http.StatusNoContent)
	return err
}

func ackOneStatus(ctx context.Context, lb *roundRobinClient, topicName, receiptHandle string, attempts int, want ...int) (int, error) {
	path := "/v1/topics/" + url.PathEscape(topicName) + "/ack"
	body := map[string]string{"receipt_handle": receiptHandle}
	var status int
	err := retry(ctx, attempts, 100*time.Millisecond, func() error {
		got, _, err := lb.do(ctx, http.MethodPost, path, body, nil, want...)
		if err != nil {
			return err
		}
		status = got
		return err
	})
	return status, err
}
