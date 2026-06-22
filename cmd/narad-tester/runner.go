package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const createTopicRetryInterval = time.Second

type runner struct {
	cfg     config
	client  *apiClient
	ledger  *ledger
	metrics *testerMetrics
	log     *slog.Logger
	topics  []string
	padding string

	sequence         atomic.Int64
	nextConsumeTopic atomic.Uint64
}

func newRunner(cfg config, client *apiClient, ledger *ledger, metrics *testerMetrics, log *slog.Logger) *runner {
	return &runner{
		cfg:     cfg,
		client:  client,
		ledger:  ledger,
		metrics: metrics,
		log:     log,
		topics:  cfg.topicNames(),
		padding: cfg.payloadPadding(),
	}
}

func (r *runner) Run(ctx context.Context) error {
	if err := r.client.waitReady(ctx, r.cfg.ReadyTimeout); err != nil {
		return err
	}
	if r.cfg.CreateTopics {
		if err := r.createTopics(ctx); err != nil {
			return err
		}
	}
	if r.cfg.CleanupTopics {
		defer r.cleanupTopics(context.Background())
	}

	runCtx, cancel := context.WithCancel(ctx)
	if r.cfg.Duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, r.cfg.Duration)
	}
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Go(func() {
		if err := r.metrics.serve(runCtx, r.cfg.MetricsAddr); err != nil {
			select {
			case errCh <- fmt.Errorf("metrics server: %w", err):
			default:
			}
		}
	})
	wg.Go(func() {
		r.scanLedger(runCtx)
	})

	jobs := make(chan messageJob, max(1024, r.cfg.ProducerConcurrency*64))
	r.metrics.producerQueueCapacity.Set(float64(cap(jobs)))
	var produceDone <-chan struct{}
	if r.cfg.MaxMessages > 0 {
		done := make(chan struct{})
		produceDone = done
		wg.Go(func() {
			defer close(done)
			r.dispatchProduces(runCtx, jobs)
		})
	} else {
		wg.Go(func() {
			r.dispatchProduces(runCtx, jobs)
		})
	}

	var producerWG sync.WaitGroup
	for i := 0; i < r.cfg.ProducerConcurrency; i++ {
		producerWG.Add(1)
		wg.Go(func() {
			defer producerWG.Done()
			r.produceWorker(runCtx, jobs)
		})
	}
	producersDone := make(chan struct{})
	wg.Go(func() {
		producerWG.Wait()
		close(producersDone)
	})
	for i := 0; i < r.cfg.ConsumerConcurrency; i++ {
		wg.Go(func() {
			r.consumeWorker(runCtx)
		})
	}

	r.log.Info("narad tester running",
		"nodes", strings.Join(r.cfg.Nodes, ","),
		"topics", len(r.topics),
		"messages_per_second", r.cfg.MessagesPerSecond,
		"max_messages_per_second", r.cfg.MaxMessagesPerSecond,
		"rate_ramp_step", r.cfg.RateRampStep,
		"rate_ramp_interval", r.cfg.RateRampInterval,
		"dispatch_interval", r.cfg.DispatchInterval,
		"metrics_addr", r.cfg.MetricsAddr,
		"consume_wait", r.cfg.ConsumeWait,
		"consumed_sequence_tracking", "exact",
		"max_outstanding_messages", r.cfg.MaxOutstandingMessages,
		"max_messages", r.cfg.MaxMessages,
		"drain_timeout", r.cfg.DrainTimeout)

	select {
	case <-runCtx.Done():
	case <-produceDone:
		if err := r.waitForProducersAndDrain(runCtx, producersDone); err != nil {
			r.log.Warn("one-shot drain incomplete", "err", err)
		}
		cancel()
	case err := <-errCh:
		cancel()
		wg.Wait()
		return err
	}
	cancel()
	wg.Wait()

	stats := r.publishLedgerStats()
	r.log.Info("narad tester stopped",
		"messages_attempted", r.sequence.Load(),
		"produced_outstanding", stats.ProducedOutstanding,
		"pending", stats.Pending,
		"missing", stats.Missing,
		"consumed_sequences", stats.ConsumedSequences)
	return nil
}

func (r *runner) createTopics(ctx context.Context) error {
	for _, topic := range r.topics {
		if err := r.createTopic(ctx, topic); err != nil {
			return fmt.Errorf("create topic %s: %w", topic, err)
		}
		r.log.Info("topic ready", "topic", topic)
	}
	return nil
}

func (r *runner) createTopic(ctx context.Context, topic string) error {
	deadline := time.Now().Add(r.cfg.ReadyTimeout)
	for {
		err := r.client.createTopic(ctx, topic, r.cfg)
		if err == nil {
			return nil
		}
		if !retryableSetupError(err) || time.Now().After(deadline) {
			return err
		}

		r.log.Warn("create topic failed; retrying",
			"topic", topic,
			"err", err,
			"retry_after", createTopicRetryInterval)
		timer := time.NewTimer(createTopicRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func retryableSetupError(err error) bool {
	var statusErr *apiStatusError
	if !errors.As(err, &statusErr) {
		return true
	}
	return statusErr.StatusCode == http.StatusRequestTimeout ||
		statusErr.StatusCode == http.StatusTooManyRequests ||
		statusErr.StatusCode >= http.StatusInternalServerError
}

func (r *runner) cleanupTopics(ctx context.Context) {
	for _, topic := range r.topics {
		if err := r.client.deleteTopic(ctx, topic); err != nil {
			r.log.Warn("delete topic", "topic", topic, "err", err)
		}
	}
}

func (r *runner) dispatchProduces(ctx context.Context, jobs chan<- messageJob) {
	defer close(jobs)
	rates := newRateSchedule(r.cfg)
	r.metrics.targetRate.Set(float64(rates.Current()))

	ticker := time.NewTicker(rates.DispatchInterval())
	defer ticker.Stop()
	rampTicker := time.NewTicker(r.cfg.RateRampInterval)
	defer rampTicker.Stop()

	var carry float64
	for {
		select {
		case <-ctx.Done():
			return
		case <-rampTicker.C:
			if rates.Advance() {
				r.metrics.targetRate.Set(float64(rates.Current()))
				r.log.Info("produce rate adjusted",
					"messages_per_second", rates.Current(),
					"direction", rates.Direction())
			}
		case <-ticker.C:
			count, nextCarry := rates.MessagesForElapsed(rates.DispatchInterval(), carry)
			carry = nextCarry
			for i := range count {
				if r.maxMessagesDispatched() {
					return
				}
				if r.produceBackpressured() {
					r.metrics.produceThrottled.Add(float64(count - i))
					break
				}
				if !r.dispatchProduceJob(ctx, jobs) {
					return
				}
			}
		}
	}
}

func (r *runner) produceBackpressured() bool {
	return r.cfg.MaxOutstandingMessages > 0 && r.ledger.outstandingCount() >= r.cfg.MaxOutstandingMessages
}

func (r *runner) maxMessagesDispatched() bool {
	return r.cfg.MaxMessages > 0 && r.sequence.Load() >= r.cfg.MaxMessages
}

func (r *runner) dispatchProduceJob(ctx context.Context, jobs chan<- messageJob) bool {
	seq := r.sequence.Add(1)
	topic := r.topics[int(seq-1)%len(r.topics)]
	id := fmt.Sprintf("%s-%012d", r.cfg.RunID, seq)
	job := messageJob{
		ID:       id,
		Topic:    topic,
		Sequence: seq,
		Key:      fmt.Sprintf("%s-key-%012d", r.cfg.RunID, seq),
	}
	queueFull := len(jobs) == cap(jobs)
	var blockedAt time.Time
	if queueFull {
		blockedAt = time.Now()
	}
	select {
	case jobs <- job:
		if queueFull {
			r.metrics.dispatchBlockLatency.Observe(time.Since(blockedAt).Seconds())
		}
		if queueFull || seq%1024 == 0 {
			r.metrics.producerQueueDepth.Set(float64(len(jobs)))
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *runner) waitForProducersAndDrain(ctx context.Context, producersDone <-chan struct{}) error {
	select {
	case <-producersDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	if r.cfg.DrainTimeout == 0 {
		return nil
	}

	drainCtx, cancel := context.WithTimeout(ctx, r.cfg.DrainTimeout)
	defer cancel()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		stats := r.publishLedgerStats()
		if stats.ProducedOutstanding == 0 && stats.Pending == 0 {
			return nil
		}
		select {
		case <-drainCtx.Done():
			return drainCtx.Err()
		case <-ticker.C:
		}
	}
}

func (r *runner) produceWorker(ctx context.Context, jobs <-chan messageJob) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			if job.Sequence%1024 == 0 {
				r.metrics.producerQueueDepth.Set(float64(len(jobs)))
			}
			r.produceOne(ctx, job)
		}
	}
}

func (r *runner) produceOne(ctx context.Context, job messageJob) {
	producedAt := time.Now()
	msg := testerMessage{
		ID:               job.ID,
		RunID:            r.cfg.RunID,
		Topic:            job.Topic,
		Sequence:         job.Sequence,
		Key:              job.Key,
		ProducedAtUnixMs: producedAt.UnixMilli(),
		Payload:          r.padding,
	}

	if err := r.ledger.recordProduced(ledgerRecord{
		ID:               job.ID,
		RunID:            r.cfg.RunID,
		Topic:            job.Topic,
		Sequence:         job.Sequence,
		Key:              job.Key,
		ProducedAtUnixMs: producedAt.UnixMilli(),
		Partition:        -1,
		Offset:           -1,
	}); err != nil {
		r.metrics.ledgerErrors.WithLabelValues("record_produced").Inc()
		r.log.Warn("record produced", "id", job.ID, "err", err)
		return
	}

	start := time.Now()
	resp, err := r.client.produce(ctx, job.Topic, produceRequest{
		Key:     job.Key,
		Message: msg,
	})
	r.metrics.produceLatency.Observe(time.Since(start).Seconds())
	if err != nil {
		r.ledger.deleteProduced(job.ID)
		reason, status := classifyHTTPError(err)
		r.metrics.produceErrors.WithLabelValues(reason, status).Inc()
		return
	}
	r.ledger.updateProducedLocation(job.ID, resp.Partition, resp.Offset)
	r.metrics.messagesProduced.Inc()
}

func (r *runner) consumeWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		topic := r.topics[int(r.nextConsumeTopic.Add(1)-1)%len(r.topics)]
		start := time.Now()
		msg, found, err := r.client.consume(ctx, topic, r.cfg.ConsumeWait)
		r.metrics.consumeRequestLatency.Observe(time.Since(start).Seconds())
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				reason, status := classifyHTTPError(err)
				r.metrics.consumeErrors.WithLabelValues(reason, status).Inc()
			}
			sleepAfterRequestError(ctx)
			continue
		}
		if !found {
			continue
		}
		r.handleConsumed(ctx, msg)
	}
}

func (r *runner) handleConsumed(ctx context.Context, msg consumeResponse) {
	if msg.Payload.RunID != r.cfg.RunID {
		r.metrics.messagesConsumed.WithLabelValues(consumeOutcomeUnknown).Inc()
		r.metrics.messagesUnknown.Inc()
		r.ackConsumed(ctx, msg)
		return
	}

	result, err := r.ledger.markConsumed(msg.Payload, msg.Topic, time.Now())
	if err != nil {
		r.metrics.ledgerErrors.WithLabelValues("mark_consumed").Inc()
		r.log.Warn("mark consumed", "id", msg.Payload.ID, "err", err)
		return
	}

	r.metrics.messagesConsumed.WithLabelValues(result.Outcome).Inc()
	switch result.Outcome {
	case consumeOutcomeValid:
		if result.ProducedAtUnixMs > 0 {
			latency := float64(time.Now().UnixMilli()-result.ProducedAtUnixMs) / 1000
			if !math.IsNaN(latency) && latency >= 0 {
				r.metrics.produceToConsumeLatency.Observe(latency)
			}
		}
	case consumeOutcomeDuplicate:
		r.metrics.messagesDuplicate.Inc()
	default:
		r.metrics.messagesUnknown.Inc()
	}

	r.ackConsumed(ctx, msg)
}

func (r *runner) ackConsumed(ctx context.Context, msg consumeResponse) {
	if msg.ReceiptHandle == "" {
		r.metrics.ackErrors.WithLabelValues("missing_receipt_handle", "0").Inc()
		return
	}
	start := time.Now()
	err := r.client.ack(ctx, msg.Topic, msg.ReceiptHandle)
	r.metrics.ackLatency.Observe(time.Since(start).Seconds())
	if err != nil {
		reason, status := classifyHTTPError(err)
		r.metrics.ackErrors.WithLabelValues(reason, status).Inc()
		return
	}
	r.metrics.messagesAcked.Inc()
}

func (r *runner) scanLedger(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.LedgerScanInterval)
	defer ticker.Stop()
	for {
		r.publishLedgerStats()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *runner) publishLedgerStats() ledgerStats {
	stats := r.ledger.statsAndCompact(time.Now(), r.cfg.MissingAfter)
	r.metrics.observeLedgerStats(stats)
	return stats
}

func sleepAfterRequestError(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
	}
}

func classifyHTTPError(err error) (string, string) {
	if err == nil {
		return "", ""
	}
	msg := err.Error()
	if strings.Contains(msg, "returned status ") {
		fields := strings.Fields(msg)
		for i, field := range fields {
			if field == "status" && i+1 < len(fields) {
				return "http_status", strings.TrimSuffix(fields[i+1], ":")
			}
		}
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled", "0"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", "0"
	}
	return "request_error", "0"
}
