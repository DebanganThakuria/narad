package messaging

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/replication"
)

const (
	replicationOperationTimeout = 5 * time.Second
	replicationBatchMaxRecords  = 256
	replicationBatchMaxBytes    = 512 << 10
	replicationBatchLinger      = time.Millisecond
)

type replicationLane struct {
	engine    *Engine
	topic     string
	partition int

	batchLinger time.Duration
	mu          sync.Mutex
	queue       []*replicationJob
	replicated  map[int64]struct{}
	inflight    int
	failed      bool
	failErr     error
	generation  uint64
	signal      chan struct{}
}

type replicationJob struct {
	log      *storage.Log
	offset   int64
	payload  []byte
	queuedAt time.Time
	gen      uint64
	done     chan error
}

func newReplicationLane(engine *Engine, topic string, partition int) *replicationLane {
	return newReplicationLaneWithBatchLinger(engine, topic, partition, replicationBatchLinger)
}

func newReplicationLaneWithBatchLinger(engine *Engine, topic string, partition int, batchLinger time.Duration) *replicationLane {
	lane := &replicationLane{
		engine:      engine,
		topic:       topic,
		partition:   partition,
		batchLinger: batchLinger,
		replicated:  make(map[int64]struct{}),
		signal:      make(chan struct{}, 1),
	}
	go lane.run()
	return lane
}

func (l *replicationLane) needsRepair(log *storage.Log) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failed {
		return true
	}
	return l.inflight == 0 && len(l.queue) == 0 && len(l.replicated) == 0 && log.HighWatermark() < log.NextOffset()
}

func (l *replicationLane) markRepaired() {
	l.mu.Lock()
	l.failed = false
	l.failErr = nil
	l.mu.Unlock()
}

func (l *replicationLane) enqueue(job *replicationJob) error {
	l.mu.Lock()
	if l.failed {
		l.mu.Unlock()
		return fmt.Errorf("replication lane requires repair")
	}
	if job.queuedAt.IsZero() {
		job.queuedAt = time.Now()
	}
	job.gen = l.generation
	l.inflight++
	l.queue = append(l.queue, job)
	l.mu.Unlock()
	l.notify()
	return nil
}

func (l *replicationLane) process(job *replicationJob) {
	l.processBatch([]*replicationJob{job})
}

func (l *replicationLane) notify() {
	select {
	case l.signal <- struct{}{}:
	default:
	}
}

func (l *replicationLane) run() {
	for range l.signal {
		for {
			batch := l.takeBatch()
			if len(batch) == 0 {
				break
			}
			l.processBatch(batch)
		}
	}
}

func (l *replicationLane) takeBatch() []*replicationJob {
	timer := time.NewTimer(l.batchLinger)
	defer stopReplicationBatchTimer(timer)

	for {
		if l.readyToFlush() {
			return l.popBatch()
		}
		select {
		case <-l.signal:
		case <-timer.C:
			return l.popBatch()
		}
	}
}

func (l *replicationLane) readyToFlush() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.queue) == 0 {
		return true
	}
	totalBytes := 0
	for i, job := range l.queue {
		if i > 0 && job.offset != l.queue[i-1].offset+1 {
			return true
		}
		totalBytes += len(job.payload)
		if i+1 >= replicationBatchMaxRecords || totalBytes >= replicationBatchMaxBytes {
			return true
		}
	}
	return false
}

func (l *replicationLane) popBatch() []*replicationJob {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.queue) == 0 {
		return nil
	}

	n := 0
	totalBytes := 0
	for n < len(l.queue) && n < replicationBatchMaxRecords {
		if n > 0 && l.queue[n].offset != l.queue[n-1].offset+1 {
			break
		}
		nextBytes := totalBytes + len(l.queue[n].payload)
		if n > 0 && nextBytes > replicationBatchMaxBytes {
			break
		}
		totalBytes = nextBytes
		n++
	}
	if n == 0 {
		n = 1
	}

	batch := append([]*replicationJob(nil), l.queue[:n]...)
	copy(l.queue, l.queue[n:])
	l.queue = l.queue[:len(l.queue)-n]
	return batch
}

func (l *replicationLane) processBatch(batch []*replicationJob) {
	if len(batch) == 0 {
		return
	}
	l.observeQueueWait(batch, "ok")
	err := l.engine.replicateJobBatch(l.topic, l.partition, batch)
	if err == nil {
		l.markBatchReplicated(batch)
		return
	}
	l.failBatch(batch, err)
}

func (l *replicationLane) markBatchReplicated(batch []*replicationJob) {
	if len(batch) == 0 {
		return
	}

	l.mu.Lock()
	completions := make([]replicationJobCompletion, 0, len(batch))
	for _, job := range batch {
		l.inflight--
		if job.gen != l.generation {
			completions = append(completions, replicationJobCompletion{job: job, err: fmt.Errorf("replication lane generation superseded")})
			continue
		}
		if l.failed {
			completions = append(completions, replicationJobCompletion{job: job, err: l.failErr})
			continue
		}
		l.replicated[job.offset] = struct{}{}
		completions = append(completions, replicationJobCompletion{job: job})
	}
	if l.failed {
		l.mu.Unlock()
		completeReplicationJobs(completions, nil)
		return
	}

	if err := l.advanceReadyLocked(batch[0].log); err != nil {
		for _, job := range l.failLocked(err) {
			completions = append(completions, replicationJobCompletion{job: job, err: err})
		}
		l.mu.Unlock()
		completeReplicationJobs(completions, err)
		return
	}
	l.mu.Unlock()
	completeReplicationJobs(completions, nil)
}

func (l *replicationLane) failBatch(batch []*replicationJob, err error) {
	l.mu.Lock()
	completions := make([]replicationJobCompletion, 0, len(batch)+len(l.queue))
	for _, job := range batch {
		l.inflight--
		completions = append(completions, replicationJobCompletion{job: job, err: err})
	}
	for _, job := range l.failLocked(err) {
		completions = append(completions, replicationJobCompletion{job: job, err: err})
	}
	l.mu.Unlock()
	completeReplicationJobs(completions, err)
}

func (l *replicationLane) failLocked(err error) []*replicationJob {
	l.failed = true
	l.failErr = err
	l.generation++
	for offset := range l.replicated {
		delete(l.replicated, offset)
	}
	queued := append([]*replicationJob(nil), l.queue...)
	l.inflight -= len(queued)
	l.queue = nil
	return queued
}

func (l *replicationLane) advanceReadyLocked(log *storage.Log) error {
	next := max(log.HighWatermark(), 0)
	for offset := range l.replicated {
		if offset < next {
			delete(l.replicated, offset)
		}
	}
	for {
		if _, ok := l.replicated[next]; !ok {
			break
		}
		delete(l.replicated, next)
		next++
	}
	if next <= log.HighWatermark() {
		return nil
	}
	if err := l.engine.advanceBatchHighWatermark(log, next); err != nil {
		return err
	}
	return nil
}

type replicationJobCompletion struct {
	job *replicationJob
	err error
}

func completeReplicationJobs(completions []replicationJobCompletion, defaultErr error) {
	for _, completion := range completions {
		if completion.job == nil {
			continue
		}
		err := completion.err
		if err == nil {
			err = defaultErr
		}
		completion.job.complete(err)
	}
}

func (l *replicationLane) observeQueueWait(batch []*replicationJob, outcome string) {
	now := time.Now()
	for _, job := range batch {
		if job.queuedAt.IsZero() {
			continue
		}
		l.engine.observe("produce", "replication_lane_queue", outcome, now.Sub(job.queuedAt))
	}
}

func stopReplicationBatchTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (j *replicationJob) complete(err error) {
	select {
	case j.done <- err:
	default:
	}
}

func (j *replicationJob) wait(ctx context.Context) error {
	select {
	case err := <-j.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) replicationLane(topicName string, partition int) *replicationLane {
	key := fmt.Sprintf("%s/%d", topicName, partition)

	e.replicationMu.Lock()
	defer e.replicationMu.Unlock()
	if lane, ok := e.replicationLanes[key]; ok {
		return lane
	}
	lane := newReplicationLane(e, topicName, partition)
	e.replicationLanes[key] = lane
	return lane
}

func (e *Engine) replicateJob(topicName string, partIdx int, job *replicationJob) error {
	return e.replicateJobBatch(topicName, partIdx, []*replicationJob{job})
}

func (e *Engine) replicateJobBatch(topicName string, partIdx int, batch []*replicationJob) error {
	if len(batch) == 0 {
		return nil
	}

	ctx, cancel := e.replicationOperationContext()
	defer cancel()

	err := e.replicateBatch(ctx, topicName, partIdx, batch)
	if err == nil {
		return nil
	}

	repaired, catchUpErr := e.catchUpReplication(ctx, topicName, partIdx, batch[0].log, err)
	if catchUpErr != nil {
		return produceStageError{
			stage: produceStageReplicate,
			err:   fmt.Errorf("%w; replication catch-up: %w", err, catchUpErr),
		}
	}
	last := batch[len(batch)-1]
	if repaired && last.log.HighWatermark() >= last.offset+1 {
		return nil
	}
	return produceStageError{stage: produceStageReplicate, err: err}
}

func (e *Engine) replicateBatch(ctx context.Context, topicName string, partIdx int, batch []*replicationJob) error {
	if batcher, ok := e.replicator.(replication.BatchReplicator); ok {
		records := make([]replication.Record, len(batch))
		for i, job := range batch {
			records[i] = replication.Record{Offset: job.offset, Payload: job.payload}
		}
		return batcher.ReplicateBatch(ctx, topicName, partIdx, records)
	}

	for _, job := range batch {
		if err := e.replicator.Replicate(ctx, topicName, partIdx, job.offset, job.payload); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) replicationOperationContext() (context.Context, context.CancelFunc) {
	parent := e.replicationCtx
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, replicationOperationTimeout)
}

func (e *Engine) advanceBatchHighWatermark(log *storage.Log, next int64) error {
	stageStart := time.Now()
	if err := log.AdvanceHighWatermark(next); err != nil {
		e.observe("produce", "advance_high_watermark", "error", time.Since(stageStart))
		return produceStageError{stage: produceStageCommit, err: err}
	}
	e.observe("produce", "advance_high_watermark", "ok", time.Since(stageStart))
	return nil
}
