package cluster

// Periodic reclaim of deleted topics' directories. A topic's purge is
// best effort: the leader can crash after the delete commits, a member
// marked dead or a lagging replica is skipped, and a broadcast bound to
// a cancelled request stops early. The startup sweep was the only
// backstop, so a node that missed a purge kept a deleted topic's bytes
// until it restarted. The pass below closes that loop while the node
// runs, with the same discipline as every destructive reconciler here: a
// caught-up replica (the caller checks), the LEADER's confirmation that
// the directory's incarnation is gone, and a purge that re-checks under
// the topic's guard (runtime.Logs.ReclaimOrphanTopicDir). Anything
// unconfirmed is kept for the next pass.
//
// Only directories carrying an incarnation marker are purged. An
// unmarked directory (written before markers existed, or by a record
// without an ID) cannot be told from one a concurrent open is making, so
// it is counted in narad_orphan_topic_dirs and left to the startup
// sweep, which runs under the create gate.

import (
	"context"
	"errors"
	"math/rand/v2"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/errs"
)

// orphanTopicReclaimer is the broker capability the pass purges orphan
// directories through (*messaging.Engine implements it; the broker
// facade embeds the engine). A reclaimer without it only counts them.
type orphanTopicReclaimer interface {
	ReclaimOrphanTopicDir(topicName, id string) (bool, error)
}

// orphanReclaimsPerPass bounds how many orphan directories one pass
// confirms with the leader and purges: each costs a leader round trip
// and a directory removal. The rest wait for the next pass, which starts
// at a random directory so none is starved.
const orphanReclaimsPerPass = 16

// RegisterMetrics registers the runner's gauge on reg:
// narad_orphan_topic_dirs, the plain topic directories whose topic this
// node's replica no longer knows and that the last pass left in place. A
// nil reg registers nothing.
func (r *MoveRunner) RegisterMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "narad_orphan_topic_dirs",
		Help: "Topic directories on this node whose topic no longer exists and that the last reclaim pass left in place: unmarked directories (removed only by the startup sweep) and directories the leader has not yet confirmed gone.",
	}, func() float64 { return float64(r.orphanTopicDirs.Load()) }))
}

// ReclaimOrphanTopicDirs runs the leader-confirmed reclaim of deleted
// topics' directories once, outside the reconcile cadence: quarantined
// directories of deleted incarnations, and plain directories of topics
// this replica no longer knows. A boot whose replica caught up only
// after the bounded startup wait gave up runs it (the create-gated
// startup sweep is forfeited by then). It does nothing on a replica that
// has not caught up, and returns the plain orphan directories left in
// place (-1 when it did not run).
func (r *MoveRunner) ReclaimOrphanTopicDirs(ctx context.Context) int {
	if r.selfID == "" || r.reclaimer == nil || !r.store.AppliedCaughtUp() {
		return -1
	}
	r.sweepMu.Lock()
	defer r.sweepMu.Unlock()
	r.reclaimQuarantinedTopicDirs(ctx)
	return r.reclaimOrphanTopicDirs(ctx)
}

// reclaimOrphanTopicDirs purges plain topics/<name> directories whose
// topic the local replica no longer knows, once the leader confirms the
// directory's incarnation is gone, and records how many such directories
// are left in narad_orphan_topic_dirs. Quarantined directories and
// directories of a topic the replica knows are left to the other halves
// of the sweep. Caller holds sweepMu on a caught-up replica.
func (r *MoveRunner) reclaimOrphanTopicDirs(ctx context.Context) int {
	candidates, err := runtime.ListTopicDirs(r.dataDir)
	if err != nil {
		r.logger.Warn("move: orphan topic directory sweep could not classify every directory", "err", err)
	}
	var orphans []runtime.OrphanCandidate
	for _, c := range candidates {
		if c.Quarantined {
			continue
		}
		if _, err := r.store.GetTopic(ctx, c.Topic); !errors.Is(err, errs.ErrNotFound) {
			continue // a topic this replica knows (or cannot read): not an orphan
		}
		orphans = append(orphans, c)
	}
	remaining := len(orphans)
	defer func() { r.orphanTopicDirs.Store(int64(remaining)) }()
	purger, ok := r.reclaimer.(orphanTopicReclaimer)
	if !ok || len(orphans) == 0 {
		return remaining
	}
	start := rand.IntN(len(orphans))
	tried := 0
	for i := range orphans {
		if ctx.Err() != nil || tried >= orphanReclaimsPerPass {
			break
		}
		c := orphans[(start+i)%len(orphans)]
		if c.Incarnation == "" {
			continue // unmarked: counted, removed only by the startup sweep
		}
		tried++
		leaderRec, absent, ok := leaderTopicView(ctx, r.store, r.peer, r.selfID, c.Topic, r.logger)
		if !ok {
			continue
		}
		if !absent && (leaderRec.ID == "" || leaderRec.ID == c.Incarnation) {
			r.logger.Debug("move: orphan topic directory kept; the leader still lists its incarnation",
				"topic", c.Topic, "incarnation", c.Incarnation)
			continue
		}
		purged, err := purger.ReclaimOrphanTopicDir(c.Topic, c.Incarnation)
		if purged {
			remaining--
			r.logger.Info("move: reclaimed the directory of a deleted topic whose purge never reached this node",
				"topic", c.Topic, "incarnation", c.Incarnation)
		}
		switch {
		case errors.Is(err, runtime.ErrNotAnOrphan):
			r.logger.Debug("move: orphan topic directory kept; it changed under the check", "topic", c.Topic, "incarnation", c.Incarnation, "err", err)
		case err != nil:
			r.logger.Warn("move: reclaim orphan topic directory", "topic", c.Topic, "incarnation", c.Incarnation, "err", err)
		}
	}
	return remaining
}
