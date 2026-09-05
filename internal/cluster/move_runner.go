package cluster

// MoveRunner drives the destination side of a partition move. It runs on
// every node and is fully self-driven: nothing choreographs it. Each pass
// it reads the local metastore replica and, for every assignment whose
// Target is THIS node, spawns a worker that copies the partition off the
// current owner and proposes the ownership flip. The controller only ever
// writes desired state (Owner + Target) into Raft; who does the work, and
// when, is decided here, locally, per partition.
//
// A worker's lifecycle for one (topic, partition):
//
//	Begin         open a copy session against the source owner
//	CatchUp       freeze-free bulk copy until within lagBytes of the tail
//	PrepareHandoff freeze the source (last moment; produce reroutes, commits
//	              are rejected and retried at the new owner) with a TTL that
//	              auto-resumes the source if this worker dies, and a token
//	              that fences everything after
//	Finalize      drain the now-static tail, re-arming the freeze with the
//	              token as it goes, reproduce the exact HWM + committed
//	              offset + fan-out cursor files, verify the staged copy
//	              recovers
//	fence         present the token once more: the source refuses if the
//	              freeze lapsed, so the flip below can only follow a freeze
//	              that held continuously since the final tail was captured
//	install       atomically move the staged dir (with its move marker) into
//	              the partition's real location on this node
//	CompleteMove  guarded CAS flip: owner := target, only if owner is still
//	              the source and target is still us — the split-brain guard
//
// On any failure before the flip the worker aborts: it discards the staged
// copy and clears the target (AbortMove, itself guarded so a re-plan is
// never clobbered). The source's freeze auto-resumes on its TTL, so a
// worker that dies mid-handoff needs no coordinator to clean up.
//
// The source side is passive: it keeps serving, answers the copy RPCs, and
// freezes on PrepareHandoff. Its now-stale on-disk copy after the flip is
// harmless (nothing routes to a non-owner) and reclaimed by a later sweep.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

const (
	defaultMoveReconcileInterval = time.Second
	// defaultMoveCatchUpLagBytes bounds how many bytes the frozen Finalize
	// must drain, so it bounds the freeze duration. A partition copies its
	// bulk (possibly GBs) freeze-free in CatchUp and only the last few MiB
	// under the freeze.
	defaultMoveCatchUpLagBytes = 4 << 20 // 4 MiB
	// defaultMoveCatchUpMaxRounds and StallRounds bound the freeze-free
	// pre-copy so a hot partition whose writers keep pace with the copy can
	// never loop forever: after this many passes (or this many with no
	// progress toward the lag bound) the worker freezes and does a
	// stop-and-copy of whatever tail remains. The move always completes; a
	// bounded fallback just means a longer freeze.
	defaultMoveCatchUpMaxRounds   = 20
	defaultMoveCatchUpStallRounds = 3
	// defaultMoveFreezeTTL is how long the source stays frozen if this
	// worker dies between PrepareHandoff and the flip. Long enough to cover
	// Finalize + install + the Raft CAS, short enough that a dead worker
	// does not strand produce for long.
	defaultMoveFreezeTTL = 30 * time.Second
	// defaultMoveFreezeRearmDivisor sets how often Finalize re-arms the
	// freeze while draining: every FreezeTTL / divisor. A drain that
	// outlives the TTL (a hot partition that did not converge) keeps the
	// source frozen for as long as it takes; a re-arm that fails means
	// the freeze lapsed and the attempt restarts under a fresh one.
	defaultMoveFreezeRearmDivisor = 4
	defaultMoveChunkBytes         = 1 << 20 // 1 MiB
	// defaultMoveRetryBackoff paces a worker's retries when a copy attempt
	// fails (source briefly unreachable, transient RPC error).
	defaultMoveRetryBackoff = 2 * time.Second
	// defaultMoveForcePromoteAfter is how long a source must stay dead before
	// the destination force-promotes the copy it holds instead of waiting.
	// Generous, so a StatefulSet pod that restarts and rejoins lets the copy
	// finish normally rather than triggering a force-promote.
	defaultMoveForcePromoteAfter = 2 * time.Minute
)

// MoveConfig tunes the runner. Zero values use the defaults above.
type MoveConfig struct {
	ReconcileInterval  time.Duration
	CatchUpLagBytes    int64
	CatchUpMaxRounds   int
	CatchUpStallRounds int
	FreezeTTL          time.Duration
	FreezeRearmEvery   time.Duration // default FreezeTTL / 4
	ChunkBytes         int64
	RetryBackoff       time.Duration
	ForcePromoteAfter  time.Duration
}

func (c MoveConfig) withDefaults() MoveConfig {
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = defaultMoveReconcileInterval
	}
	if c.CatchUpLagBytes <= 0 {
		c.CatchUpLagBytes = defaultMoveCatchUpLagBytes
	}
	if c.CatchUpMaxRounds <= 0 {
		c.CatchUpMaxRounds = defaultMoveCatchUpMaxRounds
	}
	if c.CatchUpStallRounds <= 0 {
		c.CatchUpStallRounds = defaultMoveCatchUpStallRounds
	}
	if c.FreezeTTL <= 0 {
		c.FreezeTTL = defaultMoveFreezeTTL
	}
	if c.FreezeRearmEvery <= 0 {
		c.FreezeRearmEvery = c.FreezeTTL / defaultMoveFreezeRearmDivisor
	}
	if c.ChunkBytes <= 0 {
		c.ChunkBytes = defaultMoveChunkBytes
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = defaultMoveRetryBackoff
	}
	if c.ForcePromoteAfter <= 0 {
		c.ForcePromoteAfter = defaultMoveForcePromoteAfter
	}
	return c
}

// moveStore is the slice of the metastore the runner needs. The ownership
// writes (CompleteMove, AbortMove) are Raft writes that only succeed on the
// leader; IsLeader/LeaderID let the runner forward them to the leader when
// this node (the destination) is a follower.
type moveStore interface {
	AppliedCaughtUp() bool
	IsLeader() bool
	LeaderID() string
	Barrier() error
	GetAssignment(topicName string, partition int) (metastore.Assignment, error)
	ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
	GetMember(id string) (metastore.Member, error)
	CompleteMove(ctx context.Context, topicName string, partition int, expectedOwner, targetID string) error
	AbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) error
}

// movePeer is the peer RPC surface a move needs: the source-side segment
// copy (via the embedded segmentFetcher) and last-moment freeze, plus the
// leader-forwarded ownership writes (the destination is usually not the
// leader, so it forwards the flip/abort there).
type movePeer interface {
	segmentFetcher
	PrepareHandoff(ctx context.Context, addr, topicName string, partition int, freezeTTL time.Duration, freezeToken string) (messaging.PartitionTransferInfo, error)
	CompleteMove(ctx context.Context, addr, topicName string, partition int, expectedOwner, targetID string) error
	AbortMove(ctx context.Context, addr, topicName string, partition int, expectedTarget string) error
	GetAssignment(ctx context.Context, addr, topicName string, partition int) (metastore.Assignment, error)
}

// moveReclaimer is the broker slice the stale-copy sweep needs. The
// engine's ReclaimMovedPartition re-verifies ownership affirmatively
// before deleting — defense in depth behind the sweep's own gates.
type moveReclaimer interface {
	ReclaimMovedPartition(ctx context.Context, topicName string, partition int) error
}

// guardedReclaimer is the reclaim the sweep prefers: it is told the HWM
// the partition was promoted at on its new owner and quarantines instead
// of deleting a local copy that is ahead of it (*messaging.Engine
// implements it; the broker wiring embeds the engine).
type guardedReclaimer interface {
	ReclaimMovedPartitionGuarded(ctx context.Context, topicName string, partition int, guard messaging.ReclaimGuard) error
}

// partitionConsumerStateResetter is the optional broker capability the
// runner uses after installing a copied partition (see finishMove).
type partitionConsumerStateResetter interface {
	ResetPartitionConsumerState(topicName string, partition int)
}

// *PeerClient is the production movePeer.
var _ movePeer = (*PeerClient)(nil)

type moveKey struct {
	topic     string
	partition int
	target    string // included so a re-plan (new target) spawns a fresh worker
}

type moveHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// MoveRunner owns the move workers for partitions targeted at this node.
type MoveRunner struct {
	store     moveStore
	selfID    string
	dataDir   string
	peer      movePeer
	mover     *PartitionMover
	reclaimer moveReclaimer    // may be nil (tests): disables the stale-copy sweep
	metrics   *metrics.Metrics // may be nil (tests, embedded use)
	logger    *slog.Logger
	cfg       MoveConfig

	mu      sync.Mutex
	workers map[moveKey]*moveHandle
	wg      sync.WaitGroup

	reconcilePasses int
}

// NewMoveRunner wires a runner. selfID must be this node's ID; an empty
// selfID disables the runner (a single-process node never moves anything).
func NewMoveRunner(store moveStore, selfID, dataDir string, peer movePeer, reclaimer moveReclaimer, m *metrics.Metrics, logger *slog.Logger, cfg MoveConfig) *MoveRunner {
	if logger == nil {
		logger = slog.Default()
	}
	cfg = cfg.withDefaults()
	return &MoveRunner{
		store:     store,
		selfID:    selfID,
		dataDir:   dataDir,
		peer:      peer,
		mover:     NewPartitionMover(peer, cfg.ChunkBytes, logger),
		reclaimer: reclaimer,
		metrics:   m,
		logger:    logger,
		cfg:       cfg,
		workers:   map[moveKey]*moveHandle{},
	}
}

// Run reconciles the worker set until ctx is cancelled, then stops and
// drains every worker.
func (r *MoveRunner) Run(ctx context.Context) {
	if r.selfID == "" {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		r.Reconcile(ctx)
		select {
		case <-ctx.Done():
			r.mu.Lock()
			for _, h := range r.workers {
				h.cancel()
			}
			r.mu.Unlock()
			r.wg.Wait()
			return
		case <-ticker.C:
		}
	}
}

// Reconcile performs one pass: spawn a worker for every partition now
// targeted at this node, and cancel workers whose target changed or
// cleared. Exported so tests can drive passes directly.
func (r *MoveRunner) Reconcile(ctx context.Context) {
	if ctx.Err() != nil || r.selfID == "" {
		return
	}
	// The stale-copy sweep runs at a fraction of the reconcile cadence
	// (it stats dirs and may RPC the leader). Offset 1 so the first pass
	// after startup sweeps copies left by moves completed while this node
	// was down.
	r.reconcilePasses++
	if r.reconcilePasses%moveSweepEvery == 1 {
		r.sweepStaleCopies(ctx)
	}
	// A replica that has not caught up with the leader must not act on
	// desired state: a stale Target could copy the wrong partition or race
	// a superseded plan. Running workers keep going — the flip is a guarded
	// CAS, so a stale worker can only fail, never corrupt.
	if !r.store.AppliedCaughtUp() {
		return
	}
	topics, _, err := r.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		r.logger.Error("move: list topics", "err", err)
		return
	}

	desired := map[moveKey]metastore.Assignment{}
	for _, t := range topics {
		assignments, err := r.store.ListAssignments(t.Name)
		if err != nil {
			continue // transient; next pass retries
		}
		for _, a := range assignments {
			if a.TargetID == r.selfID && a.OwnerID != r.selfID {
				desired[moveKey{topic: t.Name, partition: a.Partition, target: r.selfID}] = a
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Cancel workers no longer desired (retargeted or aborted), and reap
	// finished ones.
	for key, h := range r.workers {
		if _, want := desired[key]; want {
			select {
			case <-h.done:
				delete(r.workers, key) // finished; a later pass respawns if still desired
			default:
			}
			continue
		}
		h.cancel()
		select {
		case <-h.done:
			delete(r.workers, key)
		default:
		}
	}
	// Spawn missing workers.
	for key, a := range desired {
		if _, running := r.workers[key]; running {
			continue
		}
		workerCtx, cancel := context.WithCancel(ctx)
		h := &moveHandle{cancel: cancel, done: make(chan struct{})}
		r.workers[key] = h
		source := a.OwnerID
		r.wg.Add(1)
		go func(key moveKey, source string) {
			defer r.wg.Done()
			defer close(h.done)
			r.runMove(workerCtx, key.topic, key.partition, source)
		}(key, source)
	}
}

// runMove drives one partition move to completion. It holds ONE copy
// session for the worker's lifetime and RETRIES until the move flips, the
// worker is cancelled (retarget/shutdown), or — if the source dies and stays
// dead — force-promotes the copy it already has. Retrying in place (rather
// than aborting and being re-spawned) keeps the session's memory of the
// source's last-known HWM, which is what force-promote needs after the source
// is gone.
func (r *MoveRunner) runMove(ctx context.Context, topicName string, partition int, source string) {
	staging := r.stagingDir(topicName, partition)
	if err := os.RemoveAll(staging); err != nil {
		r.logger.Warn("move: clear staging", "dir", staging, "err", err)
		return
	}
	started := time.Now()
	if r.metrics != nil {
		r.metrics.MovesInFlight.Inc()
		defer r.metrics.MovesInFlight.Dec()
	}
	var sess *MoveSession

	for {
		if ctx.Err() != nil {
			return
		}
		m, err := r.store.GetMember(source)
		if err != nil || m.Addr == "" {
			if !sleepCtx(ctx, r.cfg.RetryBackoff) {
				return
			}
			continue
		}
		if sess == nil {
			sess = r.mover.Begin(m.Addr, topicName, partition, staging)
		}

		// If the source has been dead long enough, stop waiting for it and try
		// to promote the copy we already have (guarded: ForcePromote refuses
		// unless we copied up to the source's last-known HWM).
		if r.sourceDeadEnough(m) {
			if res, err := sess.ForcePromote(); err == nil {
				r.logger.Warn("move: force-promoting copy of a dead source",
					"topic", topicName, "partition", partition, "source", source, "hwm", res.HighWatermark)
				if r.finishMove(ctx, topicName, partition, source, staging, res, true) {
					r.observeMoveDone("force_promoted", started, res)
					return
				}
			} else {
				r.logger.Warn("move: source dead but copy is behind its last hwm — cannot force-promote; waiting",
					"topic", topicName, "partition", partition, "source", source, "err", err)
			}
			if !sleepCtx(ctx, r.cfg.RetryBackoff) {
				return
			}
			continue
		}
		// Source down but not yet dead-enough — wait for it to return or die.
		if m.Status == metastore.MemberDead {
			if !sleepCtx(ctx, r.cfg.RetryBackoff) {
				return
			}
			continue
		}

		if done, res := r.attemptCopy(ctx, sess, topicName, partition, source, m.Addr); done {
			r.observeMoveDone("completed", started, res)
			return
		}
		if !sleepCtx(ctx, r.cfg.RetryBackoff) {
			return
		}
	}
}

// observeMoveDone records a completed move's outcome, duration, and copied
// bytes. No-op without metrics.
func (r *MoveRunner) observeMoveDone(outcome string, started time.Time, res CopyResult) {
	if r.metrics == nil {
		return
	}
	r.metrics.MovesTotal.WithLabelValues(outcome).Inc()
	r.metrics.MoveDurationSeconds.Observe(time.Since(started).Seconds())
	r.metrics.MoveBytesTotal.Add(float64(res.BytesCopied))
}

// attemptCopy runs one live-source copy attempt (bounded pre-copy → freeze →
// finalize → flip). Returns true only when the move flipped. Any failure
// returns false WITHOUT clearing staging, so the next attempt resumes the
// copy and keeps the session's last-known HWM for a possible force-promote.
func (r *MoveRunner) attemptCopy(ctx context.Context, sess *MoveSession, topicName string, partition int, source, sourceAddr string) (bool, CopyResult) {
	converged, err := sess.CatchUp(ctx, r.cfg.CatchUpLagBytes, r.cfg.CatchUpMaxRounds, r.cfg.CatchUpStallRounds)
	if err != nil {
		r.logger.Warn("move: catch-up failed; will retry", "topic", topicName, "partition", partition, "err", err)
		return false, CopyResult{}
	}
	if !converged {
		r.logger.Warn("move: pre-copy did not converge (writers keep pace with the copy); freezing with a larger tail — the cutover freeze will be longer",
			"topic", topicName, "partition", partition)
	}
	frozen, err := r.peer.PrepareHandoff(ctx, sourceAddr, topicName, partition, r.cfg.FreezeTTL, "")
	if err != nil {
		r.logger.Warn("move: prepare-handoff failed; will retry", "topic", topicName, "partition", partition, "err", err)
		return false, CopyResult{}
	}
	token := frozen.FreezeToken
	if token == "" {
		// A source running an older release arms an unfenced freeze. The
		// re-arms below still extend it; the fence can only check that
		// the source is still reachable and the HWM did not move.
		r.logger.Warn("move: source does not fence its handoff freeze (older release); cutover relies on the freeze TTL alone",
			"topic", topicName, "partition", partition, "source", source)
	}
	rearm := func(ctx context.Context) (messaging.PartitionTransferInfo, error) {
		info, err := r.peer.PrepareHandoff(ctx, sourceAddr, topicName, partition, r.cfg.FreezeTTL, token)
		if err != nil {
			return messaging.PartitionTransferInfo{}, err
		}
		if token != "" && info.FreezeToken != token {
			return messaging.PartitionTransferInfo{}, fmt.Errorf("%w: source armed a different freeze", messaging.ErrHandoffFreezeLapsed)
		}
		return info, nil
	}
	sess.KeepFrozen(func(ctx context.Context) error {
		_, err := rearm(ctx)
		return err
	}, r.cfg.FreezeRearmEvery)
	res, err := sess.Finalize(ctx)
	if err != nil {
		r.logger.Warn("move: finalize failed; will retry", "topic", topicName, "partition", partition, "err", err)
		return false, CopyResult{}
	}
	// The fence. The freeze must have held continuously from the final
	// tail capture to here, or a commit could have landed on the source
	// behind the copy; the source refuses a lapsed token, and the HWM it
	// reports now must match what the copy reproduced. This also renews
	// the TTL so the install + CAS below run under a fresh freeze.
	fence, err := rearm(ctx)
	if err != nil {
		r.logger.Warn("move: handoff freeze lapsed before the flip; not flipping, will re-freeze and drain again",
			"topic", topicName, "partition", partition, "err", err)
		return false, CopyResult{}
	}
	if fence.HighWatermark != res.HighWatermark {
		r.logger.Warn("move: source hwm moved under the freeze; not flipping, will re-freeze and drain again",
			"topic", topicName, "partition", partition, "copied_hwm", res.HighWatermark, "source_hwm", fence.HighWatermark)
		return false, CopyResult{}
	}
	// The source's fan-out cursors kept advancing while frozen (fan-out
	// only reads); the fence's listing is the freshest view of them.
	staging := r.stagingDir(topicName, partition)
	if err := installSidecars(staging, fence.Sidecars); err != nil {
		r.logger.Warn("move: install fan-out cursor sidecars; will retry", "topic", topicName, "partition", partition, "err", err)
		return false, CopyResult{}
	}
	return r.finishMove(ctx, topicName, partition, source, staging, res, false), res
}

// finishMove installs the staged copy and proposes the guarded flip (owner
// := us, only if owner is still the source and target is still us — forwarded
// to the leader when this node is a follower). Returns true when the flip
// commits; on a rejected flip it rolls the install back and returns false so
// the source stays authoritative and the worker retries (a re-plan will
// cancel the worker).
func (r *MoveRunner) finishMove(ctx context.Context, topicName string, partition int, source, stagingDir string, res CopyResult, forcePromoted bool) bool {
	// The marker records how this copy got here. The old owner's sweep
	// reads it (through the transfer info) to refuse deleting a local copy
	// that is ahead of the promoted HWM, and the fan-out reconciler reads
	// it to tell a lost cursor from a fresh attach.
	if err := messaging.WriteMoveMarker(stagingDir, messaging.MoveMarker{
		Source:            source,
		HighWatermark:     res.HighWatermark,
		ForcePromoted:     forcePromoted,
		InstalledAtUnixMs: time.Now().UnixMilli(),
		Children:          r.linkedChildren(ctx, topicName),
	}); err != nil {
		r.logger.Warn("move: write move marker; will retry", "topic", topicName, "partition", partition, "err", err)
		return false
	}
	if err := r.install(topicName, partition, stagingDir); err != nil {
		r.logger.Warn("move: install failed; will retry", "topic", topicName, "partition", partition, "err", err)
		return false
	}
	// A node that owned this partition before it moved away may still
	// hold the old in-memory reservation shard; the installed copy carries
	// the source's consumer.offset, which must win.
	if rs, ok := r.reclaimer.(partitionConsumerStateResetter); ok {
		rs.ResetPartitionConsumerState(topicName, partition)
	}
	if err := r.completeMove(ctx, topicName, partition, source); err != nil {
		r.logger.Warn("move: flip rejected (CAS guard or not applied)", "topic", topicName, "partition", partition, "err", err)
		if rmErr := os.RemoveAll(r.partitionDir(topicName, partition)); rmErr != nil {
			r.logger.Warn("move: roll back install", "topic", topicName, "partition", partition, "err", rmErr)
		}
		return false
	}
	r.logger.Info("move: partition moved",
		"topic", topicName, "partition", partition, "source", source, "hwm", res.HighWatermark, "bytes", res.BytesCopied)
	return true
}

// linkedChildren snapshots the child topics attached to topicName (child
// name -> attach epoch) from the local replica, for the move marker. A
// read failure yields nil: the fan-out reconciler then treats every
// missing cursor as a fresh attach, exactly as before the marker existed.
func (r *MoveRunner) linkedChildren(ctx context.Context, parent string) map[string]string {
	topics, _, err := r.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return nil
	}
	var children map[string]string
	for _, t := range topics {
		if !t.IsChild() || t.Parent != parent {
			continue
		}
		if children == nil {
			children = map[string]string{}
		}
		children[t.Name] = t.AttachEpoch
	}
	return children
}

// sourceDeadEnough reports whether the source has been confirmed dead by the
// controller AND has stayed dead past ForcePromoteAfter — long enough that a
// transient pod restart (which would let the copy finish normally) has been
// ruled out, so promoting the copy we have is the right recovery.
func (r *MoveRunner) sourceDeadEnough(m metastore.Member) bool {
	if m.Status != metastore.MemberDead {
		return false
	}
	return time.Since(time.Unix(m.LastHeartbeat, 0)) > r.cfg.ForcePromoteAfter
}

// completeMove proposes the guarded ownership flip: directly when this node
// is the leader, otherwise forwarded to the leader over peer RPC (the
// destination is usually a follower). A CAS failure or a not-leader forward
// both surface as an error — the worker rolls the install back and retries.
func (r *MoveRunner) completeMove(ctx context.Context, topicName string, partition int, source string) error {
	if r.store.IsLeader() {
		return r.store.CompleteMove(ctx, topicName, partition, source, r.selfID)
	}
	addr, err := r.leaderAddr()
	if err != nil {
		return err
	}
	return r.peer.CompleteMove(ctx, addr, topicName, partition, source, r.selfID)
}

// leaderAddr resolves the current Raft leader's peer-RPC address.
func (r *MoveRunner) leaderAddr() (string, error) {
	id := r.store.LeaderID()
	if id == "" {
		return "", fmt.Errorf("no known leader")
	}
	m, err := r.store.GetMember(id)
	if err != nil {
		return "", fmt.Errorf("lookup leader member %q: %w", id, err)
	}
	if m.Addr == "" {
		return "", fmt.Errorf("leader %q has no address", id)
	}
	return m.Addr, nil
}

// install atomically replaces the partition's real directory with the
// staged copy. Same-filesystem rename (staging lives under dataDir), so it
// is atomic. Called before the flip: after CompleteMove the partition is
// servable here immediately, with no window where we own it but have no data.
func (r *MoveRunner) install(topicName string, partition int, staging string) error {
	dir := r.partitionDir(topicName, partition)
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return fmt.Errorf("make partition parent: %w", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("clear destination dir: %w", err)
	}
	if err := os.Rename(staging, dir); err != nil {
		return fmt.Errorf("install staged copy: %w", err)
	}
	return nil
}

func (r *MoveRunner) partitionDir(topicName string, partition int) string {
	return storage.TopicPartitionDir(r.dataDir, topicName, partition)
}

func (r *MoveRunner) stagingDir(topicName string, partition int) string {
	return filepath.Join(r.dataDir, ".moves", fmt.Sprintf("%s-%d", topicName, partition))
}
