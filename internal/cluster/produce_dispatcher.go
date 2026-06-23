package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const (
	defaultProduceDispatchInterval  = 10 * time.Millisecond
	defaultProduceDispatchBatchSize = 4096
)

var errProduceReplayBoundary = errors.New("produce replay reached durable boundary")

type produceCommitter interface {
	CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error)
}

type produceBatchCommitter interface {
	CommitAcceptedProduceBatch(context.Context, []ingress.ProduceRecord) ([]int64, error)
}

type produceDispatchTarget struct {
	local     bool
	addr      string
	topic     string
	partition int
}

type ProduceDispatcherConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

type ProduceDispatcher struct {
	ingress   *ingress.Manager
	store     *metastore.Store
	selfID    string
	committer produceCommitter
	peer      peerClient
	logger    *slog.Logger
	metrics   stageObserver
	interval  time.Duration
	batchSize int

	shards    []produceDispatchShard
	nextShard int
}

type produceDispatchShard struct {
	nextSeq uint64
	cursor  wal.Cursor
}

func NewProduceDispatcher(
	ingressManager *ingress.Manager,
	store *metastore.Store,
	selfID string,
	committer produceCommitter,
	peer peerClient,
	logger *slog.Logger,
	cfg ProduceDispatcherConfig,
	observers ...stageObserver,
) *ProduceDispatcher {
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = defaultProduceDispatchInterval
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultProduceDispatchBatchSize
	}
	var observer stageObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &ProduceDispatcher{
		ingress:   ingressManager,
		store:     store,
		selfID:    selfID,
		committer: committer,
		peer:      peer,
		logger:    logger,
		metrics:   observer,
		interval:  interval,
		batchSize: batchSize,
	}
}

func (d *ProduceDispatcher) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		processed, err := d.DispatchAvailable(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			if d.logger != nil {
				d.logger.Error("produce dispatcher", "err", err)
			}
		}
		if err := ctx.Err(); err != nil {
			return
		}
		if processed > 0 && err == nil {
			continue
		}

		timer := time.NewTimer(d.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (d *ProduceDispatcher) DispatchAvailable(ctx context.Context) (int, error) {
	start := time.Now()
	outcome := "ok"
	defer func() {
		d.observe("dispatch_available", outcome, time.Since(start))
	}()

	if d == nil {
		return 0, errors.New("produce dispatcher is nil")
	}
	if d.ingress == nil {
		return 0, errors.New("produce dispatcher ingress manager is nil")
	}

	if err := d.loadCursor(); err != nil {
		outcome = "error"
		return 0, err
	}
	if len(d.shards) == 0 {
		outcome = "empty"
		return 0, nil
	}

	processed := 0
	startShard := d.nextShard % len(d.shards)
	for visited := 0; visited < len(d.shards); visited++ {
		shard := (startShard + visited) % len(d.shards)
		n, err := d.dispatchShard(ctx, shard, &d.shards[shard], d.batchSize)
		processed += n
		if n > 0 {
			d.nextShard = (shard + 1) % len(d.shards)
		}
		if err != nil {
			outcome = "error"
			return processed, err
		}
	}

	if processed == 0 {
		outcome = "empty"
	}
	return processed, nil
}

func (d *ProduceDispatcher) dispatchShard(ctx context.Context, shard int, state *produceDispatchShard, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	durableNext, err := d.ingress.DurableProduceNextForShard(shard)
	if err != nil {
		return 0, err
	}
	if state.nextSeq >= durableNext {
		return 0, nil
	}

	processed := 0
	checkpointSeq := state.nextSeq
	nextCursor := state.cursor
	var pending []ingress.ProduceRecord
	var pendingTarget produceDispatchTarget
	var pendingCursor wal.Cursor

	flushPending := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := d.commitBatch(ctx, pendingTarget, pending); err != nil {
			return err
		}
		last := pending[len(pending)-1]
		checkpointSeq = last.WAL.Seq + 1
		nextCursor = pendingCursor
		processed += len(pending)
		pending = nil
		pendingTarget = produceDispatchTarget{}
		pendingCursor = wal.Cursor{}
		return nil
	}

	err = d.ingress.ReplayProduceShardFromCursor(shard, state.cursor, func(record ingress.ProduceRecord, cursor wal.Cursor) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.WAL.Seq < state.nextSeq {
			return nil
		}
		if record.WAL.Seq >= durableNext {
			if err := flushPending(); err != nil {
				return err
			}
			return errProduceReplayBoundary
		}
		if processed+len(pending) >= limit {
			if err := flushPending(); err != nil {
				return err
			}
			return errProduceReplayBoundary
		}
		target, err := d.dispatchTarget(record)
		if err != nil {
			if flushErr := flushPending(); flushErr != nil {
				return flushErr
			}
			// A deleted topic loses its assignment too (applyDeleteTopic
			// removes topic + assignments atomically), so dispatchTarget
			// fails here for its leftover WAL records. Discard them
			// (advance past) rather than head-of-line-blocking the shard
			// — gated on the local replica confirming the topic is gone,
			// never on the lookup error alone.
			if d.topicDeletedLocally(record.Topic) {
				d.logger.Warn("discarding undispatched record for deleted topic",
					"topic", record.Topic, "partition", record.TargetPartition,
					"seq", record.WAL.Seq, "err", err)
				checkpointSeq = record.WAL.Seq + 1
				nextCursor = cursor
				processed++
				return nil
			}
			return err
		}
		if !d.canBatchTarget(target) {
			if err := flushPending(); err != nil {
				return err
			}
			if err := d.commitBatch(ctx, target, []ingress.ProduceRecord{record}); err != nil {
				return err
			}
			checkpointSeq = record.WAL.Seq + 1
			nextCursor = cursor
			processed++
			return nil
		}
		if len(pending) > 0 && pendingTarget != target {
			if err := flushPending(); err != nil {
				return err
			}
		}
		pendingTarget = target
		pending = append(pending, record)
		pendingCursor = cursor
		return nil
	})
	if errors.Is(err, errProduceReplayBoundary) {
		err = nil
	}
	if err == nil {
		err = flushPending()
	}
	if processed > 0 {
		if checkpointErr := d.ingress.StoreProduceCheckpointForShard(shard, checkpointSeq); checkpointErr != nil {
			return processed, errors.Join(err, checkpointErr)
		}
		state.nextSeq = checkpointSeq
		state.cursor = nextCursor
		if compactErr := d.ingress.CompactProduceShardBefore(shard, checkpointSeq); compactErr != nil {
			return processed, errors.Join(err, compactErr)
		}
	}
	if err != nil {
		return processed, err
	}
	return processed, nil
}

func (d *ProduceDispatcher) canBatchTarget(target produceDispatchTarget) bool {
	if !target.local {
		return true
	}
	if d.committer == nil {
		return false
	}
	_, ok := d.committer.(produceBatchCommitter)
	return ok
}

func (d *ProduceDispatcher) loadCursor() error {
	if d.shards != nil {
		return nil
	}
	shardCount := d.ingress.ShardCount()
	if shardCount <= 0 {
		return errors.New("produce dispatcher ingress manager has no WAL shards")
	}
	shards := make([]produceDispatchShard, shardCount)
	for shard := range shards {
		nextSeq, err := d.ingress.LoadProduceCheckpointForShard(shard)
		if err != nil {
			return err
		}
		shards[shard] = produceDispatchShard{
			nextSeq: nextSeq,
			cursor:  wal.Cursor{Seq: nextSeq},
		}
	}
	d.shards = shards
	return nil
}

func (d *ProduceDispatcher) dispatchTarget(record ingress.ProduceRecord) (produceDispatchTarget, error) {
	if d.store == nil {
		return produceDispatchTarget{}, errors.New("produce dispatcher metastore is nil")
	}
	assignment, err := d.store.GetAssignment(record.Topic, record.TargetPartition)
	if err != nil {
		return produceDispatchTarget{}, fmt.Errorf("lookup assignment: %w", err)
	}
	if d.selfID == "" || assignment.OwnerID == d.selfID {
		return produceDispatchTarget{local: true, topic: record.Topic, partition: record.TargetPartition}, nil
	}

	member, err := d.store.GetMember(assignment.OwnerID)
	if err != nil {
		return produceDispatchTarget{}, fmt.Errorf("lookup owner member: %w", err)
	}
	if member.Status == metastore.MemberDead || member.Addr == "" {
		return produceDispatchTarget{}, fmt.Errorf("owner %q is unavailable", assignment.OwnerID)
	}
	return produceDispatchTarget{
		addr:      member.Addr,
		topic:     record.Topic,
		partition: record.TargetPartition,
	}, nil
}

// commitBatch dispatches a batch. If the commit fails AND the topic is
// genuinely gone from this node's metastore replica, the records are
// DISCARDED (returns nil so the caller advances the WAL checkpoint past
// them) — a topic deleted while it still had undispatched WAL records is
// the motivating case; without this the shard would head-of-line-block on
// records that can never commit.
//
// The discard decision keys off this node's own replica, never the commit
// error itself. That is the safe signal: a record only reached this WAL
// because AcceptProduce saw the topic in this replica, and Raft replicas
// only move forward — so if the topic is now absent here, a delete was
// truly applied (it cannot be create-replication lag). Any other failure
// (transient network, a lagging remote owner returning 404 for a live
// topic, owner moved, malformed record) is returned so the caller retries
// rather than silently dropping data.
func (d *ProduceDispatcher) commitBatch(ctx context.Context, target produceDispatchTarget, records []ingress.ProduceRecord) error {
	err := d.dispatchRecordBatch(ctx, target, records)
	if err == nil || ctx.Err() != nil {
		return err
	}
	if d.topicDeletedLocally(target.topic) {
		d.logger.Warn("discarding undispatched produce records for deleted topic",
			"topic", target.topic, "partition", target.partition,
			"records", len(records), "err", err)
		return nil
	}
	return err
}

// topicDeletedLocally reports whether the topic is absent from this node's
// local metastore replica.
func (d *ProduceDispatcher) topicDeletedLocally(topicName string) bool {
	if d.store == nil {
		return false
	}
	_, err := d.store.GetTopic(context.Background(), topicName)
	return errors.Is(err, errs.ErrNotFound)
}

func (d *ProduceDispatcher) dispatchRecordBatch(ctx context.Context, target produceDispatchTarget, records []ingress.ProduceRecord) error {
	start := time.Now()
	outcome := "ok"
	defer func() {
		d.observe("dispatch_record_batch", outcome, time.Since(start))
	}()

	if len(records) == 0 {
		return nil
	}
	if target.local {
		if d.committer == nil {
			outcome = "error"
			return errors.New("produce dispatcher committer is nil")
		}
		if batcher, ok := d.committer.(produceBatchCommitter); ok {
			_, err := batcher.CommitAcceptedProduceBatch(ctx, records)
			if err != nil {
				outcome = "error"
			}
			return err
		}
		for _, record := range records {
			if _, err := d.committer.CommitAcceptedProduce(ctx, record); err != nil {
				outcome = "error"
				return err
			}
		}
		return nil
	}

	if d.peer == nil {
		outcome = "error"
		return errors.New("produce dispatcher peer client is nil")
	}
	req := nodewire.CommitProduceBatchRequest{Records: make([]nodewire.CommitProduceRequest, 0, len(records))}
	for _, record := range records {
		req.Records = append(req.Records, nodewire.CommitProduceRequest{
			MessageID:       record.MessageID,
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	res, err := d.peer.CommitProduceBatch(ctx, target.addr, req)
	if err != nil {
		outcome = "error"
		return err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		outcome = "error"
		return fmt.Errorf("commit produce batch returned status %d", res.Status)
	}
	return nil
}

func (d *ProduceDispatcher) observe(stage, outcome string, duration time.Duration) {
	if d == nil || d.metrics == nil {
		return
	}
	d.metrics.ObserveHotPathStage("produce_dispatcher", "produce", stage, outcome, duration)
}
