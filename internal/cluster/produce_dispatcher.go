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

	// produceDispatchLookaheadWindows caps how far past a stuck record a
	// drain pass may scan, as a multiple of the current window: the scan
	// horizon is checkpoint + windowLimit*produceDispatchLookaheadWindows WAL
	// seqs. While a low seq cannot commit (its topic has no live-owner
	// partition left to reroute to), healthy records up to that horizon keep
	// committing; only records beyond it stay frozen until the stuck record
	// clears. The horizon also bounds memory and per-pass scan work: the
	// committedAhead skip-set and the seqs examined per pass never exceed it
	// (worst case produceDispatchLookaheadWindows * BatchSize seqs).
	produceDispatchLookaheadWindows = 16

	// produceDispatchRerouteAfterPasses is how many consecutive passes a
	// destination may keep failing commits before the dispatcher treats its
	// owner as dead and reroutes the destination's records to a live-owner
	// partition of the same topic (see dispatch). One failed pass is a
	// transient blip that must not scatter records across partitions, so the
	// records retry on their original partition until this threshold; the
	// commit attempt that keeps failing doubles as the recovery probe, so a
	// destination whose owner comes back stops being rerouted on the very
	// next pass. Destinations that fail to RESOLVE (owner dead per
	// membership) skip this grace entirely — membership death is already
	// authoritative, matching the accept-time dead-owner skip.
	produceDispatchRerouteAfterPasses = 3

	defaultProduceDispatchCommitFanout   = 16
	defaultProduceDispatchFailureBackoff = time.Second

	// produceCommitRPCTimeout bounds a remote commit RPC issued by the
	// dispatcher. The dispatcher's own context carries no deadline, so
	// without an explicit one the peer transport applies its short (~5s)
	// default reply timeout — comfortably shorter than a worst-case remote
	// fsync under load. A commit that succeeds remotely after the client
	// gave up is re-committed on the next pass as duplicates (the server
	// has no dedup) and, after produceDispatchRerouteAfterPasses, even
	// rerouted to a sibling partition. This generous timeout makes that
	// window rare; it cannot eliminate it (see dispatch's at-least-once
	// note), so it just needs to sit far above worst-case commit latency
	// while still letting a genuinely dead owner fail in bounded time.
	produceCommitRPCTimeout = 30 * time.Second
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

// produceDispatchStuckKey identifies a destination partition independently of
// where its owner currently lives, so a partition stays recognisably "stuck"
// across owner-address changes.
type produceDispatchStuckKey struct {
	topic     string
	partition int
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
	metrics           stageObserver
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
	// committedAhead holds WAL seqs that committed in a prior pass but sit
	// above the checkpoint because a lower seq has not committed yet. They
	// are skipped (not re-committed) on subsequent passes — without
	// consuming window budget — so a stuck partition cannot cause duplicate
	// deliveries of its neighbours nor freeze them. Its size is bounded by
	// the lookahead horizon (see produceDispatchLookaheadWindows). The set
	// lives only in memory: a process crash loses it, and those seqs
	// re-commit on replay — one of the at-least-once duplicate paths (the
	// other is a remote commit that outlives produceCommitRPCTimeout; see
	// dispatch).
	committedAhead map[uint64]bool
	// stuck maps each destination partition that failed to resolve or commit
	// on the previous pass to the number of consecutive passes it has been
	// failing. While a destination is stuck (and cannot be rerouted) only
	// its first (lowest WAL seq) record enters the window as a recovery
	// probe; the rest are skipped without consuming window budget, so a dead
	// owner's growing backlog cannot re-fill the window and freeze healthy
	// partitions. Once the count reaches produceDispatchRerouteAfterPasses
	// and a live-owner sibling partition exists, the destination's records
	// are rerouted there instead (see dispatch). Rebuilt every pass from
	// that pass's failures, so a recovered destination drains at full window
	// size one pass after a commit to it succeeds. Bounded by the distinct
	// destinations seen in one window.
	stuck map[produceDispatchStuckKey]int
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
	if d.state == nil {
		outcome = "empty"
		return 0, nil
	}

	processed, err := d.dispatch(ctx, d.state)
	if err != nil {
		outcome = "error"
		return processed, err
	}
	if processed == 0 {
		outcome = "empty"
	}
	return processed, nil
}

// dispatch drains up to the adaptive window (state.windowLimit) of
// not-yet-committed records from the ingress WAL, groups them by destination
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
// Checkpoint = the first scanned seq that did not durably commit (records
// discarded for a deleted topic count as done). Compaction never deletes WAL
// records past the checkpoint, so a buffered-but-uncommitted record always
// survives a crash.
//
// A record whose destination cannot take commits must not block delivery:
// because every destination shares this one WAL, a single dead partition
// owner would otherwise stop newer records for ALL topics and partitions
// while producers keep getting 2xx. The dispatcher therefore REROUTES such
// records to another partition of the same topic whose owner is alive —
// deliberately sacrificing per-key partition ordering to preserve
// availability, the exact trade the accept path already makes when it skips
// dead-owner partitions at partition-selection time
// (messaging.Engine.pickProducePartition). Two tiers trigger a reroute:
//
//   - target resolution fails for a live topic (owner dead or missing per
//     membership): membership death is authoritative, so the record is
//     rerouted immediately, with the same authority as the accept-time skip;
//   - the commit RPC itself keeps failing while membership still says the
//     owner is alive: a single failed pass is a transient blip and retries
//     on the original partition, but after a destination has stayed stuck
//     for produceDispatchRerouteAfterPasses consecutive passes it is treated
//     as dead and its records are rerouted too. The commit attempt that
//     keeps failing doubles as the recovery probe, so rerouting is per-pass,
//     never sticky: one successful commit sends new records back to their
//     original partition.
//
// A successfully rerouted record counts as done — the checkpoint advances
// past it exactly as if it had committed to its original partition.
//
// Only when NO live-owner partition of the topic exists does a record stay
// stuck and pin the checkpoint, and only then does the bounded skip-ahead
// matter: the window admits up to windowLimit records that still need
// committing, scanning at most windowLimit*produceDispatchLookaheadWindows
// seqs above the checkpoint (the lookahead horizon). Within the horizon:
//
//   - seqs already committed on an earlier pass (state.committedAhead) are
//     counted done and skipped without consuming window budget, so they are
//     never re-committed and never crowd out fresh records;
//   - destinations that failed on the previous pass (state.stuck) and have
//     nowhere to reroute contribute only their first record as a recovery
//     probe, so a dead owner's growing backlog cannot re-fill the window
//     either.
//
// Records beyond the horizon stay frozen until the stuck record clears —
// the deliberate memory/scan bound: committedAhead and per-pass scan work
// never exceed the horizon. In steady state each record is delivered exactly
// once; delivery degrades to at-least-once on two paths:
//
//   - crash replay: the committedAhead set is in-memory only, so a process
//     crash replays committed-ahead records;
//   - commit-RPC timeout: remote commits carry no idempotency token on the
//     wire, so a commit that exceeds produceCommitRPCTimeout yet succeeds
//     on the remote is retried (and, once the destination has been stuck
//     for produceDispatchRerouteAfterPasses, rerouted) — duplicating the
//     batch. The generous timeout makes this rare, not impossible.
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
	scanHorizon := state.nextSeq + uint64(limit)*produceDispatchLookaheadWindows

	// 1. Drain up to `limit` records that still need committing (no commits
	// yet), recording the resume cursor that sits just after each scanned
	// record. Already-committed seqs are marked done without consuming the
	// window budget; records of known-stuck destinations beyond their probe
	// are skipped entirely. The scan never looks past scanHorizon, bounding
	// both the work per pass and the skip-set size.
	type windowRecord struct {
		rec         ingress.ProduceRecord
		cursorAfter wal.Cursor
	}
	var window []windowRecord
	var scanStart, scanEnd uint64
	scanned := false
	done := make(map[uint64]bool)
	cursorAfterSeq := make(map[uint64]wal.Cursor)
	probed := make(map[produceDispatchStuckKey]bool, len(state.stuck))
	rerouteReady := make(map[produceDispatchStuckKey]bool, len(state.stuck))
	err := d.ingress.ReplayProduceFromCursor(state.cursor, func(record ingress.ProduceRecord, cursor wal.Cursor) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		seq := record.WAL.Seq
		if seq < state.nextSeq {
			return nil
		}
		if seq >= durableNext || seq >= scanHorizon || len(window) >= limit {
			return errProduceReplayBoundary
		}
		if !scanned {
			scanStart = seq
			scanned = true
		}
		scanEnd = seq + 1
		cursorAfterSeq[seq] = cursor
		// Already committed on an earlier pass but held above the
		// checkpoint by a lower stuck seq: count it done, never re-commit,
		// and keep looking for fresh records.
		if state.committedAhead[seq] {
			done[seq] = true
			return nil
		}
		if len(state.stuck) > 0 {
			key := produceDispatchStuckKey{topic: record.Topic, partition: record.TargetPartition}
			if passes := state.stuck[key]; passes > 0 {
				// A destination stuck long enough to reroute flows freely
				// only when a live-owner sibling partition actually exists;
				// otherwise probe-only admission keeps its backlog from
				// re-filling the window.
				canFlow := false
				if passes >= produceDispatchRerouteAfterPasses {
					ready, checked := rerouteReady[key]
					if !checked {
						_, ready = d.rerouteTarget(key.topic, key.partition, state.stuck, nil)
						rerouteReady[key] = ready
					}
					canFlow = ready
				}
				if !canFlow {
					if probed[key] {
						// Beyond the destination's recovery probe: leave it
						// for a later pass without spending window budget.
						return nil
					}
					probed[key] = true
				}
			}
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
	if !scanned {
		return 0, nil
	}

	// 2. Resolve each record's target and bucket by destination. A record
	// whose topic is gone from the local replica is discarded (done). A
	// target that cannot be resolved for a still-live topic (its owner is
	// dead or missing per membership) is rerouted to a live-owner partition
	// of the same topic; only when no such partition exists is it left
	// uncommitted, bounding the checkpoint and marking its destination stuck
	// for the next pass.
	buckets := make(map[produceDispatchTarget][]ingress.ProduceRecord)
	nextStuck := make(map[produceDispatchStuckKey]int)
	rerouted := make(map[produceDispatchStuckKey]int)
	reroutedTo := make(map[produceDispatchStuckKey]int)
	// Memoize rerouteTarget per destination for this pass (the scan phase's
	// rerouteReady memo is the same pattern): every rerouteTarget call is an
	// uncached store.GetTopic (bbolt tx + JSON unmarshal), and the first
	// pass after an owner dies can admit the full window of one partition's
	// records — thousands of redundant lookups without the memo. Caveat: a
	// fresh call filters candidates against nextStuck, which grows as this
	// loop marks other destinations stuck, so a memoized answer can point at
	// a partition that became stuck later in the SAME pass. That is
	// acceptable — all records of one destination get the same answer within
	// a pass anyway, and a commit to a bad alternative just fails and marks
	// it stuck for the next pass.
	type rerouteResult struct {
		target produceDispatchTarget
		ok     bool
	}
	rerouteMemo := make(map[produceDispatchStuckKey]rerouteResult)
	rerouteTargetFor := func(key produceDispatchStuckKey) (produceDispatchTarget, bool) {
		if res, cached := rerouteMemo[key]; cached {
			return res.target, res.ok
		}
		target, ok := d.rerouteTarget(key.topic, key.partition, state.stuck, nextStuck)
		rerouteMemo[key] = rerouteResult{target: target, ok: ok}
		return target, ok
	}
	var firstErr error
	for _, w := range window {
		target, terr := d.dispatchTarget(w.rec)
		if terr != nil {
			if d.topicDeletedLocally(w.rec.Topic) {
				d.logger.Warn("discarding undispatched record for deleted topic",
					"topic", w.rec.Topic, "partition", w.rec.TargetPartition,
					"seq", w.rec.WAL.Seq, "err", terr)
				done[w.rec.WAL.Seq] = true
				continue
			}
			key := produceDispatchStuckKey{topic: w.rec.Topic, partition: w.rec.TargetPartition}
			if alt, ok := rerouteTargetFor(key); ok {
				rec := w.rec
				rec.TargetPartition = alt.partition
				buckets[alt] = append(buckets[alt], rec)
				rerouted[key]++
				reroutedTo[key] = alt.partition
				continue
			}
			nextStuck[key] = state.stuck[key] + 1
			if firstErr == nil {
				firstErr = terr
			}
			continue
		}
		buckets[target] = append(buckets[target], w.rec)
	}
	for key, count := range rerouted {
		d.logger.Warn("rerouting produce records for dead partition owner",
			"topic", key.topic, "from_partition", key.partition,
			"to_partition", reroutedTo[key], "records", count)
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
	// WAL-seq order so per-partition offsets stay monotonic. Destinations
	// whose bucket fails are marked stuck for the next pass; a destination
	// that has stayed stuck for produceDispatchRerouteAfterPasses
	// consecutive passes has its failed records rerouted to a live-owner
	// partition instead of pinning the checkpoint.
	failed, cerr := d.commitBuckets(ctx, buckets, done)
	if cerr != nil && firstErr == nil {
		firstErr = cerr
	}
	d.rerouteFailedBuckets(ctx, failed, state.stuck, nextStuck, done)
	state.stuck = nextStuck

	// 4. Advance the checkpoint to the first not-done scanned seq.
	checkpointSeq := scanEnd
	for s := scanStart; s < scanEnd; s++ {
		if !done[s] {
			checkpointSeq = s
			break
		}
	}

	// Merge this pass's committed seqs into the carried-forward skip set.
	// Seqs below the checkpoint are pruned only AFTER the checkpoint is
	// durably stored: if the store fails, the next pass replays from the
	// old checkpoint and must still skip everything that already committed,
	// or the whole window would be re-committed as duplicates.
	ahead := make(map[uint64]bool, len(state.committedAhead)+len(done))
	for s := range state.committedAhead {
		ahead[s] = true
	}
	for s := range done {
		ahead[s] = true
	}
	state.committedAhead = ahead

	processed := int(checkpointSeq - scanStart)
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
	// The checkpoint is durable: drop skip-set entries it has passed — the
	// next pass starts at checkpointSeq and never re-reads them.
	for s := range ahead {
		if s < checkpointSeq {
			delete(ahead, s)
		}
	}
	state.nextSeq = checkpointSeq
	state.cursor = nextCursor
	if compactErr := d.ingress.CompactProduceBefore(checkpointSeq); compactErr != nil {
		return processed, errors.Join(firstErr, compactErr)
	}
	return processed, firstErr
}

// commitBuckets commits each bucket concurrently with bounded fan-out and
// marks every record of a successful bucket done. Failed buckets are
// returned so the caller can decide between retrying them on their original
// partition and rerouting them. The done map and the returned failures are
// produced only after all commits finish, so the parallel phase has no
// shared writes.
func (d *ProduceDispatcher) commitBuckets(ctx context.Context, buckets map[produceDispatchTarget][]ingress.ProduceRecord, done map[uint64]bool) (map[produceDispatchTarget][]ingress.ProduceRecord, error) {
	if len(buckets) == 0 {
		return nil, nil
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

	var failed map[produceDispatchTarget][]ingress.ProduceRecord
	var firstErr error
	for i, j := range jobs {
		if results[i] == nil {
			for _, r := range j.recs {
				done[r.WAL.Seq] = true
			}
			continue
		}
		if failed == nil {
			failed = make(map[produceDispatchTarget][]ingress.ProduceRecord)
		}
		failed[j.target] = j.recs
		if firstErr == nil {
			firstErr = results[i]
		}
	}
	return failed, firstErr
}

// rerouteFailedBuckets handles destinations whose commit just failed even
// though their owner still resolves per membership. Every such destination
// is marked stuck with an incremented consecutive-pass count. A destination
// still within its produceDispatchRerouteAfterPasses grace keeps its records
// uncommitted so they retry on the original partition next pass (a transient
// blip must not scatter records across partitions). Beyond the grace the
// destination is treated as dead: its failed records are rerouted to a
// live-owner partition of the same topic and marked done on success, so the
// checkpoint keeps advancing. The failed commit attempt that got us here
// doubles as the recovery probe — one success and the destination leaves the
// stuck set, sending new records back to their original partition.
func (d *ProduceDispatcher) rerouteFailedBuckets(ctx context.Context, failed map[produceDispatchTarget][]ingress.ProduceRecord, prevStuck, nextStuck map[produceDispatchStuckKey]int, done map[uint64]bool) {
	for target, recs := range failed {
		key := produceDispatchStuckKey{topic: target.topic, partition: target.partition}
		nextStuck[key] = prevStuck[key] + 1
		if prevStuck[key] < produceDispatchRerouteAfterPasses {
			continue
		}
		alt, ok := d.rerouteTarget(target.topic, target.partition, prevStuck, nextStuck)
		if !ok {
			continue
		}
		rerouted := make([]ingress.ProduceRecord, len(recs))
		for i, rec := range recs {
			rec.TargetPartition = alt.partition
			rerouted[i] = rec
		}
		start := time.Now()
		err := d.commitBatch(ctx, alt, rerouted)
		d.observe("dispatch_reroute_batch", observeOutcome(err), time.Since(start))
		if err != nil {
			altKey := produceDispatchStuckKey{topic: alt.topic, partition: alt.partition}
			if nextStuck[altKey] == 0 {
				nextStuck[altKey] = prevStuck[altKey] + 1
			}
			continue
		}
		for _, rec := range recs {
			done[rec.WAL.Seq] = true
		}
		d.logger.Warn("rerouting produce records for stuck partition owner",
			"topic", target.topic, "from_partition", target.partition,
			"to_partition", alt.partition, "records", len(rerouted))
	}
}

// rerouteTarget picks a live-owner partition of the same topic to stand in
// for a partition whose owner cannot take commits. It mirrors the
// accept-time dead-owner skip (messaging.Engine.pickProducePartition,
// exercised by TestProduceSkipsDeadOwnerPartition): walk the topic's
// partitions circularly starting just past fromPartition and return the
// first whose owner resolves as alive — reusing dispatchTargetsForTopic's
// membership-backed liveness — and that is not itself a stuck destination.
// Returns false when no such partition exists (single-partition topic, or
// every other owner dead/stuck); the caller then falls back to the pinned
// checkpoint + probe + lookahead-horizon behavior.
func (d *ProduceDispatcher) rerouteTarget(topicName string, fromPartition int, prevStuck, curStuck map[produceDispatchStuckKey]int) (produceDispatchTarget, bool) {
	if d.store == nil {
		return produceDispatchTarget{}, false
	}
	t, err := d.store.GetTopic(context.Background(), topicName)
	if err != nil || t.Partitions <= 1 {
		return produceDispatchTarget{}, false
	}
	targets, err := d.dispatchTargetsForTopic(topicName)
	if err != nil {
		return produceDispatchTarget{}, false
	}
	for i := 1; i < t.Partitions; i++ {
		candidate := (fromPartition + i) % t.Partitions
		cached, ok := targets.byPartition[candidate]
		if !ok || cached.err != nil {
			continue
		}
		key := produceDispatchStuckKey{topic: topicName, partition: candidate}
		if prevStuck[key] > 0 || curStuck[key] > 0 {
			continue
		}
		return cached.target, true
	}
	return produceDispatchTarget{}, false
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
		stuck:          map[produceDispatchStuckKey]int{},
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
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	// Explicit deadline: without one the transport's short default reply
	// timeout applies, and a slow-but-successful remote commit would be
	// re-committed as duplicates (see produceCommitRPCTimeout).
	rpcCtx, cancel := context.WithTimeout(ctx, produceCommitRPCTimeout)
	defer cancel()
	res, err := d.peer.CommitProduceBatch(rpcCtx, target.addr, req)
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
