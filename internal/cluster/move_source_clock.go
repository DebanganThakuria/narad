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
	"errors"
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// observeSource runs the worker's dead-since clock from the source's
// member record as just read: started the first time the source reads
// dead, cleared when it reads alive. A source that reads alive also
// clears a refused force-promote's report (cannotForcePromote): the
// copy can catch up again, and a later death is reported anew.
func (w *moveWorker) observeSource(m metastore.Member) {
	if m.Status == metastore.MemberDead {
		if w.deadSince.IsZero() {
			w.deadSince = w.r.now()
		}
		return
	}
	w.deadSince = time.Time{}
	w.deadReported = false
}

// cannotForcePromote reports a force-promote the session refused (err)
// although the source has been dead long enough: the copy is behind the
// source's last high watermark (a promote would lose records the source
// made visible), or it fails verification. The worker keeps waiting for
// the source. It is logged once at error each time the source dies, and
// at debug on every retry after that. The move reports itself blocked
// (source_dead_copy_behind, or copy_unverifiable for a copy that fails
// verification) until the source reads alive and a copy attempt starts.
func (w *moveWorker) cannotForcePromote(err error) {
	r := w.r
	msg := "move: the source is dead and this node's copy is behind its last high watermark, so it cannot force-promote: promoting would lose records the source made visible. Waiting for the source to return; abort the move to give up on it"
	args := []any{"topic", w.topic, "partition", w.partition, "source", w.source}
	reason := MoveBlockedSourceDeadCopyBehind
	var behind *copyBehindError
	switch {
	case errors.As(err, &behind):
		args = append(args, "copy_next_offset", behind.next, "source_last_hwm", behind.hwm)
	case w.sess == nil || !w.sess.sawInfo:
		args = append(args, "copy_next_offset", 0)
	default:
		msg = "move: the source is dead and this node's copy fails verification, so it cannot force-promote. Waiting for the source to return; abort the move to give up on it"
		reason = MoveBlockedCopyUnverifiable
	}
	w.status.setBlocked(reason, fmt.Errorf("force-promote: %w", err))
	args = append(args, "err", err)
	if w.deadReported {
		r.logger.Debug(msg, args...)
		return
	}
	w.deadReported = true
	r.logger.Error(msg, args...)
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
