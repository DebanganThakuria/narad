package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	chaosStateInFlight = "in-flight"
	chaosStateAcked    = "acked"
)

// runChaos produces and consumes a full message set while
// scripts/local-cluster-chaos.sh restarts nodes underneath it. Unlike the
// load run, duplicate deliveries are expected (at-least-once during
// failover) and are counted rather than failed on; the invariant is that
// every message is eventually acked exactly once in the claim table.
func runChaos(cfg config) error {
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

	fmt.Printf("chaos nodes: %s\n", strings.Join(cfg.nodes, ", "))
	fmt.Printf("run_id: %s\n", cfg.runID)
	fmt.Printf("creating chaos topics: %d topics x %d partitions\n", len(topics), cfg.partitions)
	if err := verifyReady(ctx, lb); err != nil {
		return err
	}
	if err := createTopics(ctx, lb, cfg, topics); err != nil {
		return err
	}
	if err := verifyTopicsReady(ctx, lb, cfg, topics); err != nil {
		return err
	}

	stats := &runStats{}
	chaosCtx, stop := context.WithCancel(ctx)
	defer stop()

	producerDone := make(chan error, 1)
	go func() {
		producerDone <- produceMessages(chaosCtx, lb, jobs, cfg.produceConcurrency, stats)
	}()

	consumerDone := make(chan error, 1)
	go func() {
		consumerDone <- consumeAndAckChaos(chaosCtx, lb, topics, expected, cfg.consumeConcurrency, stats)
	}()

	producerComplete := false
	consumerComplete := false
	for !producerComplete || !consumerComplete {
		select {
		case err := <-producerDone:
			if err != nil {
				stop()
				return fmt.Errorf("chaos produce: %w", err)
			}
			producerComplete = true
		case err := <-consumerDone:
			if err != nil {
				stop()
				return fmt.Errorf("chaos consume: %w", err)
			}
			consumerComplete = true
		case <-ctx.Done():
			stop()
			return fmt.Errorf("%w during chaos: produced=%d acked=%d want=%d", ctx.Err(), stats.produced.Load(), stats.acked.Load(), len(expected))
		}
	}

	if err := drainChaosDuplicates(ctx, lb, topics, expected, cfg.visibilityTimeout, stats); err != nil {
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
	fmt.Printf("PASS chaos topics=%d produced=%d deliveries=%d duplicates=%d acked=%d duration=%s\n",
		len(topics), stats.produced.Load(), stats.consumed.Load(), stats.duplicates.Load(), stats.acked.Load(), elapsed.Round(time.Millisecond))
	return nil
}

// claimTable tracks each message's delivery state so concurrent chaos
// consumers can count duplicate deliveries without double-counting acks.
type claimTable struct {
	mu     sync.Mutex
	states map[string]string
}

func newClaimTable(capacity int) *claimTable {
	return &claimTable{states: make(map[string]string, capacity)}
}

// claim marks id as in-flight and reports whether it was already claimed
// by another delivery (a duplicate).
func (c *claimTable) claim(id string) (duplicate bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.states[id] {
	case chaosStateAcked, chaosStateInFlight:
		return true
	default:
		c.states[id] = chaosStateInFlight
		return false
	}
}

// release gives back an in-flight claim after a failed ack so a future
// redelivery can own the message.
func (c *claimTable) release(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states[id] == chaosStateInFlight {
		delete(c.states, id)
	}
}

// markAcked records a successful ack and reports whether id newly
// transitioned to acked.
func (c *claimTable) markAcked(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.states[id] == chaosStateAcked {
		return false
	}
	c.states[id] = chaosStateAcked
	return true
}

func (c *claimTable) ackedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, state := range c.states {
		if state == chaosStateAcked {
			count++
		}
	}
	return count
}

func consumeAndAckChaos(ctx context.Context, lb *roundRobinClient, topics []string, expected map[string]messageJob, concurrency int, stats *runStats) error {
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var topicCursor atomic.Uint64
	claims := newClaimTable(len(expected))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for range concurrency {
		wg.Go(func() {
			for consumeCtx.Err() == nil {
				if int(stats.acked.Load()) >= len(expected) {
					return
				}
				topicName := topics[int(topicCursor.Add(1)-1)%len(topics)]
				msg, found, err := consumeOne(consumeCtx, lb, topicName)
				if err != nil {
					if consumeCtx.Err() != nil && int(stats.acked.Load()) >= len(expected) {
						return
					}
					sendErr(errCh, err)
					cancel()
					return
				}
				if !found {
					continue
				}
				stats.consumed.Add(1)
				if err := validateChaosMessage(msg, expected); err != nil {
					sendErr(errCh, err)
					cancel()
					return
				}

				duplicate := claims.claim(msg.Payload.ID)
				if duplicate {
					stats.duplicates.Add(1)
				}

				status, err := ackOneStatus(consumeCtx, lb, msg.Topic, msg.ReceiptHandle, 8, http.StatusNoContent, http.StatusGone)
				if err != nil || status == http.StatusGone {
					// The ack didn't land (or the handle was already stale);
					// return the claim so a redelivery can complete it.
					if !duplicate {
						claims.release(msg.Payload.ID)
					}
					continue
				}

				if claims.markAcked(msg.Payload.ID) {
					stats.acked.Add(1)
				}
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
			if got := claims.ackedCount(); got != len(expected) {
				return fmt.Errorf("acked %d messages, want %d", got, len(expected))
			}
			return nil
		case err := <-errCh:
			return err
		case <-ticker.C:
			if int(stats.acked.Load()) >= len(expected) {
				cancel()
			}
		case <-ctx.Done():
			return fmt.Errorf("%w while chaos consuming: acked=%d want=%d", ctx.Err(), stats.acked.Load(), len(expected))
		}
	}
}

// drainChaosDuplicates keeps consuming and acking until every topic has
// been quiet for quietFor — duplicates redelivered after a visibility
// timeout may still be trickling in when the main consume loop finishes.
func drainChaosDuplicates(ctx context.Context, lb *roundRobinClient, topics []string, expected map[string]messageJob, quietFor time.Duration, stats *runStats) error {
	quietUntil := time.Now().Add(quietFor)
	for time.Now().Before(quietUntil) {
		foundAny := false
		for _, topicName := range topics {
			msg, found, err := consumeOne(ctx, lb, topicName)
			if err != nil {
				return fmt.Errorf("drain duplicate from %s: %w", topicName, err)
			}
			if !found {
				continue
			}
			foundAny = true
			stats.consumed.Add(1)
			stats.duplicates.Add(1)
			if err := validateChaosMessage(msg, expected); err != nil {
				return err
			}
			if _, err := ackOneStatus(ctx, lb, msg.Topic, msg.ReceiptHandle, 8, http.StatusNoContent, http.StatusGone); err != nil {
				return fmt.Errorf("ack drained duplicate %s: %w", msg.Payload.ID, err)
			}
		}
		if foundAny {
			quietUntil = time.Now().Add(quietFor)
		}
	}
	return nil
}

func validateChaosMessage(msg consumeResponse, expected map[string]messageJob) error {
	job, ok := expected[msg.Payload.ID]
	if !ok {
		return fmt.Errorf("unexpected payload id %q from topic %s", msg.Payload.ID, msg.Topic)
	}
	if msg.Topic != job.Topic || msg.Payload.Topic != job.Topic || msg.Payload.Sequence != job.Body.Sequence {
		return fmt.Errorf("message mismatch for id %s: got topic=%s payload_topic=%s seq=%d", msg.Payload.ID, msg.Topic, msg.Payload.Topic, msg.Payload.Sequence)
	}
	if msg.ReceiptHandle == "" {
		return fmt.Errorf("message %s missing receipt handle", msg.Payload.ID)
	}
	return nil
}
