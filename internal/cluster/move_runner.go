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
//	              auto-resumes the source if this worker dies
//	Finalize      drain the now-static tail, reproduce the exact HWM +
//	              committed offset, verify the staged copy recovers
//	install       atomically move the staged dir into the partition's real
//	              location on this node
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
	"errors"
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
)

const (
	defaultMoveReconcileInterval = time.Second
	// defaultMoveCatchUpLagBytes bounds how many bytes the frozen Finalize
	// must drain, so it bounds the freeze duration. A partition copies its
	// bulk (possibly GBs) freeze-free in CatchUp and only the last few MiB
	// under the freeze.
	defaultMoveCatchUpLagBytes = 4 << 20 // 4 MiB
	// defaultMoveFreezeTTL is how long the source stays frozen if this
	// worker dies between PrepareHandoff and the flip. Long enough to cover
	// Finalize + install + the Raft CAS, short enough that a dead worker
	// does not strand produce for long.
	defaultMoveFreezeTTL = 30 * time.Second
	defaultMoveChunkBytes = 1 << 20 // 1 MiB
)

// MoveConfig tunes the runner. Zero values use the defaults above.
type MoveConfig struct {
	ReconcileInterval time.Duration
	CatchUpLagBytes   int64
	FreezeTTL         time.Duration
	ChunkBytes        int64
}

func (c MoveConfig) withDefaults() MoveConfig {
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = defaultMoveReconcileInterval
	}
	if c.CatchUpLagBytes <= 0 {
		c.CatchUpLagBytes = defaultMoveCatchUpLagBytes
	}
	if c.FreezeTTL <= 0 {
		c.FreezeTTL = defaultMoveFreezeTTL
	}
	if c.ChunkBytes <= 0 {
		c.ChunkBytes = defaultMoveChunkBytes
	}
	return c
}

// moveStore is the slice of the metastore the runner needs.
type moveStore interface {
	AppliedCaughtUp() bool
	ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
	GetMember(id string) (metastore.Member, error)
	CompleteMove(ctx context.Context, topicName string, partition int, expectedOwner, targetID string) error
	AbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) error
}

// movePeer is the source-side RPC surface a move needs: the segment copy
// (via the embedded segmentFetcher) plus the last-moment freeze.
type movePeer interface {
	segmentFetcher
	PrepareHandoff(ctx context.Context, addr, topicName string, partition int, freezeTTL time.Duration) (messaging.PartitionTransferInfo, error)
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
	store   moveStore
	selfID  string
	dataDir string
	peer    movePeer
	mover   *PartitionMover
	logger  *slog.Logger
	cfg     MoveConfig

	mu      sync.Mutex
	workers map[moveKey]*moveHandle
	wg      sync.WaitGroup
}

// NewMoveRunner wires a runner. selfID must be this node's ID; an empty
// selfID disables the runner (a single-process node never moves anything).
func NewMoveRunner(store moveStore, selfID, dataDir string, peer movePeer, logger *slog.Logger, cfg MoveConfig) *MoveRunner {
	if logger == nil {
		logger = slog.Default()
	}
	cfg = cfg.withDefaults()
	return &MoveRunner{
		store:   store,
		selfID:  selfID,
		dataDir: dataDir,
		peer:    peer,
		mover:   NewPartitionMover(peer, cfg.ChunkBytes, logger),
		logger:  logger,
		cfg:     cfg,
		workers: map[moveKey]*moveHandle{},
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

// runMove executes one partition move to completion (or abort). It is the
// whole destination-side lifecycle; see the package doc.
func (r *MoveRunner) runMove(ctx context.Context, topicName string, partition int, source string) {
	sourceAddr, err := r.sourceAddr(source)
	if err != nil {
		r.logger.Warn("move: resolve source", "topic", topicName, "partition", partition, "source", source, "err", err)
		return // no abort: the owner may just be briefly unreachable; retry next pass
	}

	staging := r.stagingDir(topicName, partition)
	if err := os.RemoveAll(staging); err != nil {
		r.logger.Warn("move: clear staging", "dir", staging, "err", err)
		return
	}
	sess := r.mover.Begin(sourceAddr, topicName, partition, staging)

	// Phase 1 — freeze-free bulk copy. A GB partition copies here with
	// produce still flowing on the source.
	if err := sess.CatchUp(ctx, r.cfg.CatchUpLagBytes); err != nil {
		r.abort(ctx, topicName, partition, staging, "catch-up", err)
		return
	}

	// Phase 2 — last-moment freeze, then drain the tail and flip.
	if _, err := r.peer.PrepareHandoff(ctx, sourceAddr, topicName, partition, r.cfg.FreezeTTL); err != nil {
		r.abort(ctx, topicName, partition, staging, "prepare-handoff", err)
		return
	}
	res, err := sess.Finalize(ctx)
	if err != nil {
		r.abort(ctx, topicName, partition, staging, "finalize", err)
		return
	}
	if err := r.install(topicName, partition, staging); err != nil {
		r.abort(ctx, topicName, partition, staging, "install", err)
		return
	}
	// The guarded flip: owner := us, only if owner is still the source and
	// target is still us. A re-plan or a competing worker makes this fail —
	// in which case we roll back the install so the source stays authoritative.
	if err := r.store.CompleteMove(ctx, topicName, partition, source, r.selfID); err != nil {
		r.logger.Warn("move: flip rejected (CAS guard)", "topic", topicName, "partition", partition, "err", err)
		if rmErr := os.RemoveAll(r.partitionDir(topicName, partition)); rmErr != nil {
			r.logger.Warn("move: roll back install", "topic", topicName, "partition", partition, "err", rmErr)
		}
		return
	}
	r.logger.Info("move: partition moved",
		"topic", topicName, "partition", partition, "source", source, "hwm", res.HighWatermark, "bytes", res.BytesCopied)
}

// abort discards the staged copy and clears the move target (guarded, so a
// re-plan is never clobbered). The source's freeze, if any, auto-resumes on
// its TTL.
func (r *MoveRunner) abort(ctx context.Context, topicName string, partition int, staging, phase string, cause error) {
	if errors.Is(cause, context.Canceled) {
		return // shutdown or retarget: leave desired state alone
	}
	r.logger.Warn("move: aborting", "topic", topicName, "partition", partition, "phase", phase, "err", cause)
	if err := os.RemoveAll(staging); err != nil {
		r.logger.Warn("move: clear staging on abort", "dir", staging, "err", err)
	}
	if err := r.store.AbortMove(ctx, topicName, partition, r.selfID); err != nil {
		r.logger.Warn("move: clear target on abort", "topic", topicName, "partition", partition, "err", err)
	}
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

func (r *MoveRunner) sourceAddr(sourceID string) (string, error) {
	m, err := r.store.GetMember(sourceID)
	if err != nil {
		return "", fmt.Errorf("lookup source member %q: %w", sourceID, err)
	}
	if m.Status == metastore.MemberDead || m.Addr == "" {
		return "", fmt.Errorf("source %q is unavailable", sourceID)
	}
	return m.Addr, nil
}

func (r *MoveRunner) partitionDir(topicName string, partition int) string {
	return storage.TopicPartitionDir(r.dataDir, topicName, partition)
}

func (r *MoveRunner) stagingDir(topicName string, partition int) string {
	return filepath.Join(r.dataDir, ".moves", fmt.Sprintf("%s-%d", topicName, partition))
}
