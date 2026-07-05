package cluster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

const (
	defaultProduceDispatchInterval = 10 * time.Millisecond

	// defaultProduceDispatchBatchSize is the hard ceiling on a single drain
	// window (BatchSize in the config). The actual window grows adaptively up
	// to this cap (see produceDispatchBaseWindow /
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

type produceCommitter interface {
	CommitAcceptedProduce(context.Context, ingress.ProduceRecord) (int64, error)
}

type produceBatchCommitter interface {
	CommitAcceptedProduceBatch(context.Context, []ingress.ProduceRecord) ([]int64, error)
}

// ProduceDispatcherConfig holds tunables for a ProduceDispatcher. Zero values
// use safe defaults.
type ProduceDispatcherConfig struct {
	// PollInterval is how long Run sleeps when a pass finds no work.
	// <=0 uses the default.
	PollInterval time.Duration
	// BatchSize is the hard cap on one drain window (see
	// defaultProduceDispatchBatchSize). <=0 uses the default.
	BatchSize int
	// CommitConcurrency bounds how many per-partition batches are committed
	// in parallel within one drain window. <=0 uses the default.
	CommitConcurrency int
}

// ProduceDispatcher continuously drains accepted produce records from the
// ingress WAL and commits them to the owning partition logs — locally through
// the committer, remotely through the peer client. It is the async half of the
// accept-then-commit produce pipeline: producers get a 2xx once a record is in
// the WAL, and the dispatcher guarantees it eventually reaches a partition.
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

// NewProduceDispatcher constructs a ProduceDispatcher. Call Run to start the
// drain loop, or DispatchAvailable to drive single passes by hand.
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

// DispatchAvailable performs a single dispatch pass over the ingress WAL and
// reports how many records it processed. It is the one-shot form of Run for
// callers (and tests) that drive the drain loop themselves.
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

	return d.dispatch(ctx, d.state)
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
