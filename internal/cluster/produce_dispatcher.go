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
	defaultProduceDispatchInterval = 10 * time.Millisecond
	// defaultProduceDispatchBatchSize is the hard ceiling on a single drain
	// drain window (BatchSize in the config). The actual window grows
	// adaptively up to this cap (see produceDispatchBaseWindow /
	// produceDispatchTargetPerPartition); the cap only binds at very high
	// fan-out (>~1k partitions) and bounds the transient memory a drain holds.
	defaultProduceDispatchBatchSize = 1 << 16 // 65536

	// produceDispatchBaseWindow is the window used before any fan-out has been
	// observed and the floor it never drops below (clamped to the BatchSize
	// cap). At low fan-out this already yields large per-partition batches.
	produceDispatchBaseWindow = 4096

	// produceDispatchTargetPerPartition is the per-partition batch size the
	// adaptive window aims for: the next window is sized to
	// target * (distinct partitions seen this window), so per-partition commit
	// batches — hence fsyncs — stay fat regardless of how many partitions the
	// WAL interleaves.
	produceDispatchTargetPerPartition = 64

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

type cachedProduceDispatchTarget struct {
	target produceDispatchTarget
	err    error
}

type cachedProduceDispatchTargets struct {
	assignmentVersion     uint64
	routingMembersVersion uint64
	byPartition           map[int]cachedProduceDispatchTarget
}

type ProduceDispatcherConfig struct {
	PollInterval time.Duration
	BatchSize    int
	// CommitConcurrency bounds how many per-partition batches are committed
	// in parallel within one drain window. <=0 uses the default.
	CommitConcurrency int
}

type ProduceDispatcher struct {
	ingress           *ingress.Manager
	store             *metastore.Store
	selfID            string
	committer         produceCommitter
	peer              peerClient
	logger            *slog.Logger
	interval          time.Duration
	batchSize         int
	commitConcurrency int
	failureBackoff    time.Duration

	state *produceDispatchState

	targetMu    sync.RWMutex
	targetCache map[string]cachedProduceDispatchTargets
}

type produceDispatchState struct {
	nextSeq uint64
	cursor  wal.Cursor
	// windowLimit is the adaptive drain window, grown toward
	// produceDispatchTargetPerPartition * (distinct partitions seen) and
	// clamped to [base, BatchSize cap]. It converges in one window: the first
	// pass sees the fan-out and the next is sized to it.
	windowLimit int
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
		interval:          interval,
		batchSize:         batchSize,
		commitConcurrency: commitConcurrency,
		failureBackoff:    defaultProduceDispatchFailureBackoff,
		targetCache:       make(map[string]cachedProduceDispatchTargets),
	}
}

// Run continuously drains the ingress WAL and commits accepted produce records
// to the owning partition logs. Per-partition batches are committed in parallel,
// while checkpointing stays tied to the single ingress WAL cursor.
func (d *ProduceDispatcher) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.loadCursor(); err != nil {
		d.logger.Error("produce dispatcher: load cursor", "err", err)
		return
	}
	d.run(ctx)
}

func (d *ProduceDispatcher) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		processed, err := d.dispatch(ctx, d.state)
		if err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Error("produce dispatcher", "err", err)
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
	if d == nil {
		return 0, errors.New("produce dispatcher is nil")
	}
	if d.ingress == nil {
		return 0, errors.New("produce dispatcher ingress manager is nil")
	}

	if err := d.loadCursor(); err != nil {
		return 0, err
	}
	if d.state == nil {
		return 0, nil
	}

	processed, err := d.dispatch(ctx, d.state)
	if err != nil {
		return processed, err
	}
	return processed, nil
}

// dispatch drains up to the adaptive window (state.windowLimit) of records from
// the ingress WAL, groups them by destination
// (topic,partition,owner), commits the groups as large per-partition batches
// concurrently, then advances the checkpoint to the lowest WAL seq not
// yet durably committed and compacts up to it.
//
// Grouping is the throughput lever: the WAL interleaves every partition, so
// flushing on each target change produced ~1-record batches (one fsync
// each). Bucketing a whole window by partition turns N interleaved records
// into a handful of large batches — one fsync per batch — committed in
// parallel across partitions and owners. The window grows with the observed
// fan-out (see windowLimit) so per-partition batches stay fat even when
// hundreds of partitions interleave, keeping the fsync count low.
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
func (d *ProduceDispatcher) dispatch(ctx context.Context, state *produceDispatchState) (int, error) {
	limit := state.windowLimit
	if limit <= 0 {
		limit = d.clampWindow(produceDispatchBaseWindow)
		state.windowLimit = limit
	}
	durableNext := d.ingress.DurableProduceNext()
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
	err := d.ingress.ReplayProduceFromCursor(state.cursor, func(record ingress.ProduceRecord, cursor wal.Cursor) error {
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

	// Size the next window to this pass's fan-out: aim for ~target records per
	// distinct partition so per-partition commit batches (one fsync each) stay
	// fat no matter how many partitions the WAL interleaves. Converges in one
	// pass — the base window already samples enough records to see the spread.
	if n := len(buckets); n > 0 {
		state.windowLimit = d.clampWindow(produceDispatchTargetPerPartition * n)
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
	if checkpointErr := d.ingress.StoreProduceCheckpoint(checkpointSeq); checkpointErr != nil {
		return processed, errors.Join(firstErr, checkpointErr)
	}
	state.nextSeq = checkpointSeq
	state.cursor = nextCursor
	if compactErr := d.ingress.CompactProduceBefore(checkpointSeq); compactErr != nil {
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

// clampWindow bounds an adaptive window to [base, BatchSize cap]. The cap
// (d.batchSize) wins when it is below the base, so a tiny configured BatchSize
// (e.g. tests) still hard-caps the window.
func (d *ProduceDispatcher) clampWindow(target int) int {
	ceil := max(d.batchSize, 1)
	lo := min(produceDispatchBaseWindow, ceil)
	return min(max(target, lo), ceil)
}

func (d *ProduceDispatcher) loadCursor() error {
	if d.state != nil {
		return nil
	}
	nextSeq, err := d.ingress.LoadProduceCheckpoint()
	if err != nil {
		return err
	}
	d.state = &produceDispatchState{
		nextSeq:        nextSeq,
		cursor:         wal.Cursor{Seq: nextSeq},
		committedAhead: map[uint64]bool{},
		windowLimit:    d.clampWindow(produceDispatchBaseWindow),
	}
	return nil
}

func (d *ProduceDispatcher) dispatchTarget(record ingress.ProduceRecord) (produceDispatchTarget, error) {
	if d.store == nil {
		return produceDispatchTarget{}, errors.New("produce dispatcher metastore is nil")
	}
	targets, err := d.dispatchTargetsForTopic(record.Topic)
	if err != nil {
		return produceDispatchTarget{}, err
	}

	cached, ok := targets.byPartition[record.TargetPartition]
	if !ok {
		return produceDispatchTarget{}, fmt.Errorf("lookup assignment: %w", errs.ErrNotFound)
	}
	if cached.err != nil {
		return produceDispatchTarget{}, cached.err
	}
	return cached.target, nil
}

func (d *ProduceDispatcher) dispatchTargetsForTopic(topicName string) (cachedProduceDispatchTargets, error) {
	assignmentVersion := d.store.AssignmentVersion(topicName)
	routingMembersVersion := d.store.RoutingMembersVersion()

	d.targetMu.RLock()
	cached, ok := d.targetCache[topicName]
	d.targetMu.RUnlock()
	if ok && cached.assignmentVersion == assignmentVersion && cached.routingMembersVersion == routingMembersVersion {
		if d.store.AssignmentVersion(topicName) == assignmentVersion && d.store.RoutingMembersVersion() == routingMembersVersion {
			return cached, nil
		}
		assignmentVersion = d.store.AssignmentVersion(topicName)
		routingMembersVersion = d.store.RoutingMembersVersion()
	}

	for {
		assignments, err := d.store.ListAssignments(topicName)
		currentAssignmentVersion := d.store.AssignmentVersion(topicName)
		currentRoutingMembersVersion := d.store.RoutingMembersVersion()
		if currentAssignmentVersion != assignmentVersion || currentRoutingMembersVersion != routingMembersVersion {
			assignmentVersion = currentAssignmentVersion
			routingMembersVersion = currentRoutingMembersVersion
			continue
		}
		if err != nil {
			return cachedProduceDispatchTargets{}, fmt.Errorf("lookup assignment: %w", err)
		}

		targets := cachedProduceDispatchTargets{
			assignmentVersion:     assignmentVersion,
			routingMembersVersion: routingMembersVersion,
			byPartition:           make(map[int]cachedProduceDispatchTarget, len(assignments)),
		}
		needsMembers := false
		for _, assignment := range assignments {
			if d.selfID != "" && assignment.OwnerID != d.selfID {
				needsMembers = true
				break
			}
		}
		memberByID := map[string]metastore.Member{}
		if needsMembers {
			members, err := d.store.ListMembers()
			currentAssignmentVersion = d.store.AssignmentVersion(topicName)
			currentRoutingMembersVersion = d.store.RoutingMembersVersion()
			if currentAssignmentVersion != assignmentVersion || currentRoutingMembersVersion != routingMembersVersion {
				assignmentVersion = currentAssignmentVersion
				routingMembersVersion = currentRoutingMembersVersion
				continue
			}
			if err != nil {
				return cachedProduceDispatchTargets{}, fmt.Errorf("lookup owner member: %w", err)
			}
			memberByID = make(map[string]metastore.Member, len(members))
			for _, member := range members {
				memberByID[member.ID] = member
			}
		}

		for _, assignment := range assignments {
			target := produceDispatchTarget{
				local:     d.selfID == "" || assignment.OwnerID == d.selfID,
				topic:     topicName,
				partition: assignment.Partition,
			}
			if target.local {
				targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{target: target}
				continue
			}
			member, ok := memberByID[assignment.OwnerID]
			if !ok {
				targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{
					err: fmt.Errorf("lookup owner member: %w", errs.ErrNotFound),
				}
				continue
			}
			if member.Status == metastore.MemberDead || member.Addr == "" {
				targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{
					err: fmt.Errorf("owner %q is unavailable", assignment.OwnerID),
				}
				continue
			}
			target.addr = member.Addr
			targets.byPartition[assignment.Partition] = cachedProduceDispatchTarget{target: target}
		}

		d.targetMu.Lock()
		d.targetCache[topicName] = targets
		d.targetMu.Unlock()
		return targets, nil
	}
}

// commitBatch dispatches a batch. If the commit fails AND the topic is
// genuinely gone from this node's metastore replica, the records are
// DISCARDED (returns nil so the caller advances the WAL checkpoint past
// them) — a topic deleted while it still had undispatched WAL records is
// the motivating case; without this the dispatch window would block on records
// that can never commit.
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
	if len(records) == 0 {
		return nil
	}
	if target.local {
		if d.committer == nil {
			return errors.New("produce dispatcher committer is nil")
		}
		if batcher, ok := d.committer.(produceBatchCommitter); ok {
			_, err := batcher.CommitAcceptedProduceBatch(ctx, records)
			return err
		}
		for _, record := range records {
			if _, err := d.committer.CommitAcceptedProduce(ctx, record); err != nil {
				return err
			}
		}
		return nil
	}

	if d.peer == nil {
		return errors.New("produce dispatcher peer client is nil")
	}
	req := nodewire.CommitProduceBatchRequest{Records: make([]nodewire.CommitProduceRequest, 0, len(records))}
	for _, record := range records {
		req.Records = append(req.Records, nodewire.CommitProduceRequest{
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	res, err := d.peer.CommitProduceBatch(ctx, target.addr, req)
	if err != nil {
		return err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return fmt.Errorf("commit produce batch returned status %d", res.Status)
	}
	return nil
}
