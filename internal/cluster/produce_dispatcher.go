package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

const (
	defaultProduceDispatchInterval       = 10 * time.Millisecond
	defaultProduceDispatchBatchSize      = 4096
	defaultProduceDispatchCommitFanout   = 16
	defaultProduceDispatchFailureBackoff = time.Second
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
	// CommitConcurrency bounds how many per-partition batches are committed
	// in parallel within one shard window. <=0 uses the default.
	CommitConcurrency int
}

type ProduceDispatcher struct {
	ingress           *ingress.Manager
	store             *metastore.Store
	selfID            string
	committer         produceCommitter
	peer              peerClient
	logger            *slog.Logger
	metrics           stageObserver
	interval          time.Duration
	batchSize         int
	commitConcurrency int
	failureBackoff    time.Duration

	shards    []produceDispatchShard
	nextShard int
}

type produceDispatchShard struct {
	nextSeq uint64
	cursor  wal.Cursor
	// committedAhead holds WAL seqs that committed in a prior window but
	// sit above the checkpoint because a lower seq has not committed yet.
	// They are skipped (not re-committed) on subsequent passes, so a stuck
	// partition cannot cause duplicate deliveries of its neighbours. The
	// set lives only in memory: a process crash loses it, and those seqs
	// re-commit on replay — the sole, at-least-once duplicate path.
	committedAhead map[uint64]bool
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
	commitConcurrency := cfg.CommitConcurrency
	if commitConcurrency <= 0 {
		commitConcurrency = defaultProduceDispatchCommitFanout
	}
	var observer stageObserver
	if len(observers) > 0 {
		observer = observers[0]
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &ProduceDispatcher{
		ingress:           ingressManager,
		store:             store,
		selfID:            selfID,
		committer:         committer,
		peer:              peer,
		logger:            logger,
		metrics:           observer,
		interval:          interval,
		batchSize:         batchSize,
		commitConcurrency: commitConcurrency,
		failureBackoff:    defaultProduceDispatchFailureBackoff,
	}
}

// Run dispatches every WAL shard concurrently — one goroutine per shard,
// each with its own cursor/checkpoint. Shards are independent (a partition
// is pinned to one shard by pickShard), so they never contend on the same
// partition and per-partition order is preserved within each lane.
func (d *ProduceDispatcher) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.loadCursor(); err != nil {
		d.logger.Error("produce dispatcher: load cursor", "err", err)
		return
	}
	var wg sync.WaitGroup
	for shard := range d.shards {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			d.runShard(ctx, shard)
		}(shard)
	}
	wg.Wait()
}

func (d *ProduceDispatcher) runShard(ctx context.Context, shard int) {
	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := d.dispatchShard(ctx, shard, &d.shards[shard], d.batchSize)
		if err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("produce dispatcher", "shard", shard, "err", err)
		}
		if ctx.Err() != nil {
			return
		}

		// More work pending and no error: keep draining without sleeping.
		if processed > 0 && err == nil {
			continue
		}
		// On failure, back off so a stuck partition (e.g. a dead owner)
		// neither spins nor re-commits the window's already-committed
		// records at the poll rate.
		wait := d.interval
		if err != nil {
			wait = d.failureBackoff
		}
		timer := time.NewTimer(wait)
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

// dispatchShard drains up to `limit` records from one WAL shard, groups
// them by destination (topic,partition,owner), commits the groups as large
// per-partition batches concurrently, then advances the shard checkpoint to
// the lowest WAL seq not yet durably committed and compacts up to it.
//
// Grouping is the throughput lever: the WAL interleaves every partition, so
// flushing on each target change produced ~1-record batches (one fsync
// each). Bucketing a whole window by partition turns N interleaved records
// into a handful of large batches — one fsync per batch — committed in
// parallel across partitions and owners.
//
// Checkpoint = the first seq in the window that did not durably commit
// (records discarded for a deleted topic count as done). Compaction never
// deletes WAL records past the checkpoint, so a buffered-but-uncommitted
// record always survives a crash.
//
// When a lower seq cannot commit (e.g. a temporarily unavailable owner) but
// higher seqs in the same window already committed, those higher seqs are
// remembered in state.committedAhead and skipped on later passes rather than
// re-committed — so a single stuck partition never duplicates its
// neighbours. That set is in-memory only: a process crash loses it and the
// committed-ahead records replay, which is the sole at-least-once duplicate
// path. The steady state delivers each record exactly once.
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

	// 1. Drain a window of records (no commits yet), recording the resume
	// cursor that sits just after each record.
	type windowRecord struct {
		rec         ingress.ProduceRecord
		cursorAfter wal.Cursor
	}
	var window []windowRecord
	err = d.ingress.ReplayProduceShardFromCursor(shard, state.cursor, func(record ingress.ProduceRecord, cursor wal.Cursor) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if record.WAL.Seq < state.nextSeq {
			return nil
		}
		if record.WAL.Seq >= durableNext || len(window) >= limit {
			return errProduceReplayBoundary
		}
		window = append(window, windowRecord{rec: record, cursorAfter: cursor})
		return nil
	})
	if errors.Is(err, errProduceReplayBoundary) {
		err = nil
	}
	if err != nil {
		return 0, err
	}
	if len(window) == 0 {
		return 0, nil
	}

	windowStart := window[0].rec.WAL.Seq
	windowEnd := window[len(window)-1].rec.WAL.Seq + 1
	cursorAfterSeq := make(map[uint64]wal.Cursor, len(window))

	// 2. Resolve each record's target and bucket by destination. A record
	// whose topic is gone from the local replica is discarded (done). A
	// target that cannot be resolved for a still-live topic (e.g. a
	// temporarily unavailable owner) is left uncommitted and bounds the
	// checkpoint.
	done := make(map[uint64]bool, len(window))
	buckets := make(map[produceDispatchTarget][]ingress.ProduceRecord)
	var firstErr error
	for _, w := range window {
		seq := w.rec.WAL.Seq
		cursorAfterSeq[seq] = w.cursorAfter
		// Already committed on an earlier pass but held above the
		// checkpoint by a lower stuck seq: count it done, never re-commit.
		if state.committedAhead[seq] {
			done[seq] = true
			continue
		}
		target, terr := d.dispatchTarget(w.rec)
		if terr != nil {
			if d.topicDeletedLocally(w.rec.Topic) {
				d.logger.Warn("discarding undispatched record for deleted topic",
					"topic", w.rec.Topic, "partition", w.rec.TargetPartition,
					"seq", w.rec.WAL.Seq, "err", terr)
				done[w.rec.WAL.Seq] = true
				continue
			}
			if firstErr == nil {
				firstErr = terr
			}
			continue
		}
		buckets[target] = append(buckets[target], w.rec)
	}

	// 3. Commit the buckets concurrently. Different partitions use
	// different logs/locks (safe to parallelise); each bucket keeps
	// WAL-seq order so per-partition offsets stay monotonic.
	if cerr := d.commitBuckets(ctx, buckets, done); cerr != nil && firstErr == nil {
		firstErr = cerr
	}

	// 4. Advance the checkpoint to the first not-done seq in the window.
	checkpointSeq := windowEnd
	for s := windowStart; s < windowEnd; s++ {
		if !done[s] {
			checkpointSeq = s
			break
		}
	}

	// Carry forward seqs that committed but sit above the checkpoint so the
	// next pass skips (does not re-commit) them; drop those the checkpoint
	// has now passed. This runs even when the checkpoint does not advance.
	ahead := make(map[uint64]bool)
	for s := range state.committedAhead {
		if s >= checkpointSeq {
			ahead[s] = true
		}
	}
	for s := windowStart; s < windowEnd; s++ {
		if done[s] && s >= checkpointSeq {
			ahead[s] = true
		}
	}
	state.committedAhead = ahead

	processed := int(checkpointSeq - windowStart)
	if processed <= 0 {
		return 0, firstErr
	}

	nextCursor := state.cursor
	if c, ok := cursorAfterSeq[checkpointSeq-1]; ok {
		nextCursor = c
	}
	if checkpointErr := d.ingress.StoreProduceCheckpointForShard(shard, checkpointSeq); checkpointErr != nil {
		return processed, errors.Join(firstErr, checkpointErr)
	}
	state.nextSeq = checkpointSeq
	state.cursor = nextCursor
	if compactErr := d.ingress.CompactProduceShardBefore(shard, checkpointSeq); compactErr != nil {
		return processed, errors.Join(firstErr, compactErr)
	}
	return processed, firstErr
}

// commitBuckets commits each bucket concurrently with bounded fan-out and
// marks every record of a successful bucket done. The done map is mutated
// only after all commits finish, so the parallel phase has no shared
// writes.
func (d *ProduceDispatcher) commitBuckets(ctx context.Context, buckets map[produceDispatchTarget][]ingress.ProduceRecord, done map[uint64]bool) error {
	if len(buckets) == 0 {
		return nil
	}
	type job struct {
		target produceDispatchTarget
		recs   []ingress.ProduceRecord
	}
	jobs := make([]job, 0, len(buckets))
	for target, recs := range buckets {
		jobs = append(jobs, job{target: target, recs: recs})
	}

	results := make([]error, len(jobs))
	sem := make(chan struct{}, max(d.commitConcurrency, 1))
	var wg sync.WaitGroup
	for i := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = d.commitBatch(ctx, jobs[i].target, jobs[i].recs)
		}(i)
	}
	wg.Wait()

	var firstErr error
	for i, j := range jobs {
		if results[i] == nil {
			for _, r := range j.recs {
				done[r.WAL.Seq] = true
			}
		} else if firstErr == nil {
			firstErr = results[i]
		}
	}
	return firstErr
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
			nextSeq:        nextSeq,
			cursor:         wal.Cursor{Seq: nextSeq},
			committedAhead: map[uint64]bool{},
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
