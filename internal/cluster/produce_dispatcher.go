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
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

const (
	// defaultProduceDispatchInterval is the idle backstop: Run wakes at
	// least this often when nothing else wakes it (a record becoming
	// durable, a commit completing, a retry falling due). It is also how
	// often a destination whose owner cannot be resolved is looked up
	// again.
	defaultProduceDispatchInterval = 10 * time.Millisecond

	// defaultProduceDispatchBatchSize is the hard ceiling on the adaptive
	// window (BatchSize in the config). The window grows up to this cap
	// (see produceDispatchBaseWindow / produceDispatchTargetPerPartition);
	// the cap only binds at very high fan-out (>~1k partitions) and
	// bounds the records the dispatcher holds in memory.
	defaultProduceDispatchBatchSize = 1 << 16 // 65536

	// produceDispatchBaseWindow is the window used before any fan-out has
	// been observed and the floor it never drops below (clamped to the
	// BatchSize cap). It is also the least one destination may queue for
	// a single commit (see perDestCap): the pass-based dispatcher sent a
	// whole window to a lone hot partition per round trip, and a
	// latency-bound destination must not get less.
	produceDispatchBaseWindow = 4096

	// produceRemoteBatchBytes bounds the encoded size of one commit to a
	// remote owner (see remoteBatchLen). The stream client refuses a
	// frame payload over clusterwire.MaxStreamFramePayloadBytes before
	// sending it, and a refused batch fails the same way every time it
	// is retried; the record count alone lets a base window of records
	// of a few KiB each exceed it. Half the frame limit keeps one commit
	// from holding the produce lane for long and still carries a whole
	// base window of records just under 2 KiB each. A local commit needs
	// no bound: the partition log splits a large batch into frames
	// itself.
	produceRemoteBatchBytes = clusterwire.MaxStreamFramePayloadBytes / 2

	// produceDispatchTargetPerPartition is the per-partition batch size
	// the adaptive window aims for: the window is sized to target *
	// (distinct partitions seen), so there is room for fat
	// per-partition commit batches, hence few fsyncs, however many
	// partitions the WAL interleaves. It is also the batch floor below
	// which a destination lingers while other commits are in flight
	// (see lingerUntil).
	produceDispatchTargetPerPartition = 64

	// produceDispatchMaxLinger caps how long a destination below the
	// batch floor waits for more records while other commits are in
	// flight. The wait is twice its owner's recent commit latency, so a
	// fast disk waits a few milliseconds and an fsync-bound or distant
	// owner up to this.
	produceDispatchMaxLinger = 50 * time.Millisecond

	// produceDispatchLookaheadWindows caps how far past the checkpoint
	// the dispatcher reads, as a multiple of the current window: nothing
	// at or above checkpoint + windowLimit*produceDispatchLookaheadWindows
	// is read. While a low seq cannot commit (its destination is failing
	// and has nowhere to reroute to, or its commit is still in flight),
	// records up to that horizon keep committing; only records beyond it
	// wait for the stuck one. The horizon bounds the per-seq bookkeeping
	// (seqMarks) and the work of a rescan (which usually stops well
	// before it, see read).
	produceDispatchLookaheadWindows = 16

	// produceDispatchRerouteGrace is how long a destination may keep
	// failing commits, counted from the start of the first failed
	// attempt, before the dispatcher treats its owner as dead and
	// reroutes the destination's records to a live-owner partition of
	// the same topic (see the header comment of produce_dispatch.go). A
	// shorter failure is a transient blip (an owner restarting, a
	// partition handoff freeze) that must not scatter records across
	// partitions, so the records retry on their original partition
	// until then. Destinations that fail to
	// RESOLVE (owner dead per membership) skip this grace entirely:
	// membership death is already authoritative, matching the
	// accept-time dead-owner skip. The grace is measured in time, not
	// passes: passes run back to back under load, so a pass count would
	// reroute a handoff freeze within milliseconds.
	produceDispatchRerouteGrace = 3 * time.Second

	defaultProduceDispatchCommitFanout = 16

	// defaultProduceDispatchFailureBackoff is how long a destination
	// whose commit failed waits before its next attempt, so a failing
	// owner is probed about once a second rather than at the pass rate.
	// It holds back only that destination. Run also waits this long
	// after it could not read the WAL or store the checkpoint.
	defaultProduceDispatchFailureBackoff = time.Second

	// produceCommitRPCTimeout bounds a remote commit RPC issued by the
	// dispatcher. The dispatcher's own context carries no deadline, so
	// without an explicit one the peer transport applies its short (~5s)
	// default reply timeout — comfortably shorter than a worst-case
	// remote fsync under load. A commit that succeeds remotely after the
	// client gave up is re-committed as duplicates (the server has no
	// dedup) and, once the destination is past its reroute grace, even
	// rerouted to a sibling partition. This generous timeout makes that
	// window rare; it cannot eliminate it (see the at-least-once note in
	// produce_dispatch.go's header comment), so it just needs to sit far
	// above worst-case commit latency while still letting a genuinely
	// dead owner fail in bounded time. Only the destination waits for
	// it: other destinations keep committing while it runs.
	produceCommitRPCTimeout = 30 * time.Second

	// produceProbeRPCTimeout bounds the one-record commit that probes a
	// failing destination. A probe that succeeds after the client gave
	// up duplicates one record, so it can afford a short deadline, and a
	// short one keeps a hung owner from pinning the checkpoint for the
	// full commit timeout on every retry.
	produceProbeRPCTimeout = 5 * time.Second

	// produceDispatchSlowAfter is how long a commit may run before it
	// stops counting against the commit fan-out. A hung owner then holds
	// one slot per destination for this long, not for the RPC deadline,
	// so its destinations cannot starve everyone else's commits.
	produceDispatchSlowAfter = time.Second

	// produceDispatchRescanInterval is the backstop for records the
	// dispatcher left in the WAL (see produce_dispatch.go): at least
	// this often they are read again and re-placed, which picks up a
	// reroute that became possible, an owner that came back, or a
	// delete that became confirmable.
	produceDispatchRescanInterval = time.Second

	// produceLegacyOwnerTTL is how long commits keep going out without
	// topic IDs to an owner that refused them (an older release). Long
	// enough that a rolling upgrade costs one refused batch per owner per
	// TTL, short enough that an upgraded owner is back on the checked
	// path within minutes.
	produceLegacyOwnerTTL = 2 * time.Minute
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
	// PollInterval is the longest Run sleeps when nothing wakes it.
	// <=0 uses the default.
	PollInterval time.Duration
	// BatchSize is the hard cap on the adaptive window (see
	// defaultProduceDispatchBatchSize). <=0 uses the default.
	BatchSize int
	// CommitConcurrency bounds how many per-partition batches are
	// committed in parallel. <=0 uses the default.
	CommitConcurrency int
}

// ProduceDispatcher continuously drains accepted produce records from the
// ingress WAL and commits them to the owning partition logs — locally through
// the committer, remotely through the peer client. It is the async half of the
// accept-then-commit produce pipeline: producers get a 2xx once a record is in
// the WAL, and the dispatcher guarantees it eventually reaches a partition.
//
// Run and DispatchAvailable share the dispatcher's state and must not be
// used at the same time.
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
	// now is the clock the reroute grace and retry backoff read; tests
	// swap it.
	now func() time.Time

	// results carries finished commits from their goroutines back to
	// the loop, which alone owns state.
	results chan dispatchResult

	state *produceDispatchState

	targetMu    sync.RWMutex
	targetCache map[string]cachedProduceDispatchTargets

	// legacyOwners maps an owner address to the time until which
	// commits to it go out without topic IDs (see commitRemote).
	legacyOwners sync.Map
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
		now:               time.Now,
		results:           make(chan dispatchResult, commitConcurrency),
		targetCache:       make(map[string]cachedProduceDispatchTargets),
	}
}

// Run continuously drains the ingress WAL and commits accepted produce
// records to the owning partition logs until ctx is cancelled. Each
// destination partition has at most one commit in flight and the next
// one leaves as soon as it lands, so one slow or unreachable owner holds
// up only its own partitions. Run returns once every commit it started
// has finished.
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
	st := d.state
	st.manual = false
	timer := time.NewTimer(d.interval)
	defer timer.Stop()
	advanced := d.ingress.DurableProduceAdvanced()
	var lastLogged time.Time
	for ctx.Err() == nil {
		st.pass++
		st.err, st.stalled = nil, false
		d.step(ctx, st)
		if st.err != nil && !errors.Is(st.err, context.Canceled) {
			// A failing destination is retried about once a second;
			// log at that pace rather than once per wakeup.
			if now := d.now(); now.Sub(lastLogged) >= d.failureBackoff {
				lastLogged = now
				d.logger.Error("produce dispatcher", "err", st.err)
			}
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(d.nextWake(st))
		// A record becoming durable wakes the loop at once rather than
		// on the idle poll. Not after a round that could not read the
		// WAL or store the checkpoint: the retry then waits out the
		// backoff instead of running once per accept. A failing
		// destination does not count: its retries go by its own clock.
		wake := advanced
		if st.stalled {
			wake = nil
		}
		select {
		case <-ctx.Done():
		case res := <-d.results:
			d.finish(ctx, st, res)
		case <-wake:
		case <-timer.C:
		}
	}
	// Every commit still running aborts on ctx; merge their outcomes so
	// what did commit is checkpointed.
	for st.outstanding > 0 {
		d.finish(ctx, st, <-d.results)
	}
	if err := d.advanceCheckpoint(st); err != nil {
		d.logger.Error("produce dispatcher: final checkpoint", "err", err)
	}
}

// step runs one round of the loop: merge finished commits, read newly
// durable records (and any the dispatcher left in the WAL when it is
// time to look at them again), start the commits that can go, and
// move the checkpoint.
func (d *ProduceDispatcher) step(ctx context.Context, st *produceDispatchState) {
	d.drainResults(ctx, st)
	d.markSlow(st, d.now())
	d.read(ctx, st)
	d.launch(ctx, st)
	if err := d.advanceCheckpoint(st); err != nil {
		st.noteErr(err)
		st.stalled = true
	}
	if now := d.now(); now.Sub(st.lastSweep) >= produceDispatchRescanInterval {
		st.lastSweep = now
		d.sweepIdle(st)
	}
}

// drainResults merges every finished commit that is already waiting.
func (d *ProduceDispatcher) drainResults(ctx context.Context, st *produceDispatchState) {
	for {
		select {
		case res := <-d.results:
			d.finish(ctx, st, res)
		default:
			return
		}
	}
}

// nextWake is how long Run may sleep before something is due: nothing
// while a rescan is due (a commit that just started asks for one to
// refill its destination's queue), else the idle backstop (the failure
// backoff after a stalled round), a running commit turning slow, a
// lingering batch's deadline, a failing destination's retry, or the
// rescan backstop. Every round's read clears rescanDue, and under Run
// only a commit that starts or finishes sets it, so this cannot spin.
func (d *ProduceDispatcher) nextWake(st *produceDispatchState) time.Duration {
	if st.rescanDue {
		return 0
	}
	now := d.now()
	wake := now.Add(d.interval)
	if st.stalled {
		wake = now.Add(d.failureBackoff)
	}
	for job := range st.jobs {
		if !job.slow {
			if at := job.start.Add(produceDispatchSlowAfter); at.Before(wake) {
				wake = at
			}
		}
	}
	for _, dest := range st.ready {
		// Only a deadline still ahead: one that passed while the
		// fan-out is full waits for a commit to land, which wakes the
		// loop anyway.
		if until, ok := d.lingerUntil(st, dest); ok && until.After(now) && until.Before(wake) {
			wake = until
		}
	}
	for dest := range st.waiting {
		if dest.retryAt.Before(wake) {
			wake = dest.retryAt
		}
	}
	if st.skipped > 0 {
		if at := st.lastRescan.Add(produceDispatchRescanInterval); at.Before(wake) {
			wake = at
		}
	}
	return max(wake.Sub(now), 0)
}

// DispatchAvailable performs a single dispatch pass over the ingress WAL and
// reports how far the checkpoint advanced. It is the one-shot form of Run
// for callers (and tests) that drive the loop themselves: it reads the
// available window, commits it, waits for every commit it started, and
// returns the first error that left records uncommitted. A failing
// destination gets one attempt per pass (its probe), not one per
// failureBackoff.
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
	st := d.state
	if st == nil {
		return 0, nil
	}
	st.manual = true
	defer func() { st.manual = false }()
	st.pass++
	st.err = nil
	before := st.nextSeq

	d.drainResults(ctx, st)
	// A pass looks at everything left in the WAL once, like the full
	// scan it replaces, then keeps going while that makes progress:
	// a reroute decided when a probe fails re-places the destination's
	// other records within the same pass.
	st.rescanDue = true
	for range produceDispatchManualRounds {
		d.read(ctx, st)
		d.launch(ctx, st)
		for st.outstanding > 0 {
			d.finish(ctx, st, <-d.results)
			d.launch(ctx, st)
		}
		if !st.rescanDue || ctx.Err() != nil {
			break
		}
	}
	if err := d.advanceCheckpoint(st); err != nil {
		st.noteErr(err)
	}
	return int(st.nextSeq - before), st.err
}

// produceDispatchManualRounds bounds the read-and-commit rounds of one
// DispatchAvailable pass.
const produceDispatchManualRounds = 4

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
	d.state = newProduceDispatchState(nextSeq, d.clampWindow(produceDispatchBaseWindow))
	return nil
}

func newProduceDispatchState(nextSeq uint64, window int) *produceDispatchState {
	return &produceDispatchState{
		nextSeq:     nextSeq,
		storedSeq:   nextSeq,
		readSeq:     nextSeq,
		readCursor:  wal.Cursor{Seq: nextSeq},
		marks:       seqMarks{base: nextSeq},
		windowLimit: window,
		epochDests:  map[dispatchDestKey]struct{}{},
		dests:       map[dispatchDestKey]*dispatchDest{},
		waiting:     map[*dispatchDest]struct{}{},
		jobs:        map[*dispatchJob]struct{}{},
		latency:     map[string]time.Duration{},
		goneIDs:     map[string]struct{}{},
		unsure:      map[string]time.Time{},
		topics:      map[string]cachedDispatchTopic{},
	}
}
