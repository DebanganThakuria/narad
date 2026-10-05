package cluster

// Force-promote on the destination's own monotonic clock. The leader's
// heartbeat stamp says when the source was last heard from, on another
// node's clock and possibly across a leaderless period, so a worker that
// has only just started can read a stamp that is old the moment it
// first looks. The worker also runs its own dead-since clock, and
// force-promotes only once it has itself watched the source stay dead
// for ForcePromoteAfter. A worker that restarts starts the clock again,
// which only delays a force-promote.

import (
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// observeSource runs the worker's dead-since clock from the source's
// member record as just read: started the first time the source reads
// dead, cleared when it reads alive.
func (w *moveWorker) observeSource(m metastore.Member) {
	if m.Status == metastore.MemberDead {
		if w.deadSince.IsZero() {
			w.deadSince = w.r.now()
		}
		return
	}
	w.deadSince = time.Time{}
}

// sourceDeadLongEnough reports whether a force-promote may replace the
// source m: dead by the leader's stamp for ForcePromoteAfter
// (sourceDeadEnough), and watched dead that long by this worker.
func (w *moveWorker) sourceDeadLongEnough(m metastore.Member) bool {
	if !w.r.sourceDeadEnough(m) || w.deadSince.IsZero() {
		return false
	}
	return w.r.now().Sub(w.deadSince) > w.r.cfg.ForcePromoteAfter
}
