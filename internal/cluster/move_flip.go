package cluster

// A move's worker state (runMove in move_runner.go drives it) and the
// resolution of an installed copy whose flip was proposed and not
// confirmed: the reply was an error, which does not prove the flip
// failed. A deposed leader answers ErrLeadershipLost for an entry the
// next leader commits, and a forwarded flip's reply can be lost after
// the leader applied it. Undoing the install on the error alone lost
// whole partitions: the new owner served an empty log, and the old
// owner's sweep then reclaimed the only other copy.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// movePendingFreezeLimit bounds how long a worker whose flip does not
// commit keeps the source frozen by re-arming the freeze to propose the
// flip again. Past it the freeze is left to lapse and the worker only
// asks the leader: the install is moved back to staging once the leader
// confirms the flip has not committed, and a flip is proposed again only
// after a fresh drain.
const movePendingFreezeLimit = 2 * time.Minute

// moveFlipSettle is how long after a flip proposal returned an unknown
// outcome it may still reach the leader's log: network transit, the RPC
// server's control-slot wait, and raft.Apply's enqueue timeout (5s). A
// barriered leader read that starts once it has passed sees every such
// proposal that can still commit, so only such a read may undo an
// install on the leader's word. A read that started earlier does not
// count even if it returns after the window: its barrier may predate
// it.
const moveFlipSettle = 15 * time.Second

// pendingFlip is an installed copy whose flip was proposed and not
// confirmed.
type pendingFlip struct {
	res           CopyResult
	expectID      string
	installed     os.FileInfo
	marker        messaging.MoveMarker // the marker the install wrote into its copy
	forcePromoted bool
	token         string
	sourceAddr    string
	since         time.Time
	// unknownAt is when the last flip proposal whose outcome is unknown
	// returned (zero: every proposal so far was refused outright or never
	// sent); see moveFlipSettle.
	unknownAt time.Time
	// unflippable says why the install cannot be flipped as it is any
	// more (the freeze lapsed, the source's HWM moved, the source came
	// back): the worker stops proposing and waits for a leader read that
	// confirms the flip did not commit, then moves the install back.
	unflippable string
}

// moveWorker is one move's state, touched only by its worker goroutine.
type moveWorker struct {
	r         *MoveRunner
	topic     string
	partition int
	source    string
	staging   string
	started   time.Time

	sess     *MoveSession
	prevSess *MoveSession // the session a reset replaced (see carryFrom)
	pending  *pendingFlip

	flipped           bool
	flipForcePromoted bool
	flipResult        CopyResult
	// exit: the move as this worker knows it is over (the leader
	// confirmed its flip cannot happen); a re-plan spawns a fresh worker.
	exit bool
	// movedBack: staging holds a copy this worker installed and then
	// moved back off the partition's path (rollbackInstall restored it),
	// and the worker has not installed again since.
	movedBack bool
	// warned holds the retry reasons this worker has already logged at
	// warn level (see warnOnce).
	warned map[string]bool
	// deadSince is when this worker first read the source as dead, on
	// its own clock; zero while the source reads alive
	// (move_source_clock.go).
	deadSince time.Time
	// deadReported: this worker logged, at error, that the source died
	// with a copy it cannot force-promote, since the source last read
	// alive.
	deadReported bool

	// unverified counts the frozen drains in a row whose staged copy
	// failed verification; gaveUp is set once the copy failed it again
	// after a fresh start, and the worker then waits to be cancelled
	// without freezing the source again (move_runner.go).
	unverified int
	gaveUp     bool

	// status is what MoveStates reports for this worker; nil (a worker
	// built without trackMove) records nothing (move_states.go).
	status *moveStatus
}

// warnOnce logs a retry at warn level the first time this worker meets
// reason and at debug level after that, so a move that keeps retrying
// for the same reason (the topic record changed under it, until the
// next reconcile pass cancels the worker) does not flood the log.
func (w *moveWorker) warnOnce(reason, msg string, args ...any) {
	if w.warned[reason] {
		w.r.logger.Debug(msg, args...)
		return
	}
	if w.warned == nil {
		w.warned = map[string]bool{}
	}
	w.warned[reason] = true
	w.r.logger.Warn(msg, args...)
}

// flipDone records that the move flipped.
func (w *moveWorker) flipDone(res CopyResult, forcePromoted bool) {
	w.flipped, w.flipForcePromoted, w.flipResult = true, forcePromoted, res
}

// observeDone records a flipped move's outcome, duration and copied
// bytes.
func (w *moveWorker) observeDone() {
	outcome := "completed"
	if w.flipForcePromoted {
		outcome = "force_promoted"
	}
	w.r.observeMoveDone(outcome, w.started, w.flipResult)
}

// finish runs as the worker exits. A worker that did not flip removes
// its staging copy: a worker spawned again for the same move copies
// afresh, and one that is not wanted any more must not leave a partition
// copy on disk. Staging is kept when the owner cannot be read (a fresh
// copy from the source, which the next worker clears), and set aside
// (quarantined, where no worker clears it) when it may hold records
// that exist nowhere else: this node owns the partition by now and
// staging may hold records the partition's path lacks (see
// ownedStagingIsRedundant), this worker moved its install back to
// staging and the owner cannot be read, or the partition's owner reads
// dead (or has no member record) and staging holds records, which may be
// the only copy of them left (ownerMayBeGone). A partition with no
// assignment (its topic is gone) has no owner. A worker cancelled
// with a flip pending leaves the install at the partition's path: if the
// flip committed it is the partition, and if not, the next worker's
// install quarantines it (setAsideLiveCopy; error-level log) or the
// stale-copy sweep judges it against the real owner.
func (w *moveWorker) finish() {
	if w.flipped {
		return
	}
	r := w.r
	a, err := r.store.GetAssignment(w.topic, w.partition)
	if err != nil && !errors.Is(err, errs.ErrNotFound) {
		// Unknown owner: keep it. A worker spawned again for this move
		// clears staging when it starts. (No assignment at all is a
		// deleted topic: nobody owns it, so staging goes.)
		if _, serr := os.Stat(w.staging); serr == nil {
			if w.movedBack {
				// Staging holds the install this worker moved back; if
				// its flip committed after all, it is the partition.
				w.setAsideStaging("move: set aside the staging copy this move moved back, since the partition's owner cannot be read; it may hold the partition's records. Operator action required",
					"partition_dir", r.partitionDir(w.topic, w.partition), "err", err)
				return
			}
			r.logger.Warn("move: could not read the partition's owner; keeping the staging copy", "topic", w.topic, "partition", w.partition, "staging", w.staging, "err", err)
		}
		return
	}
	if err == nil && a.OwnerID == r.selfID {
		if _, err := os.Stat(w.staging); err != nil {
			return
		}
		dir := r.partitionDir(w.topic, w.partition)
		if w.ownedStagingIsRedundant(dir) {
			r.logger.Info("move: the partition flipped to this node under an earlier attempt's install; removing this attempt's staging copy",
				"topic", w.topic, "partition", w.partition, "staging", w.staging, "partition_dir", dir)
		} else {
			segs, _ := listLocalSegments(dir)
			w.setAsideStaging("move: set aside the staging copy of a partition this node owns; the partition's records may not all be under its path. Operator action required",
				"partition_dir", dir, "moved_back", w.movedBack, "partition_dir_has_records", holdsRecords(segs))
			return
		}
	}
	if err == nil && a.OwnerID != r.selfID {
		if gone, why := w.ownerMayBeGone(a.OwnerID); gone {
			if segs, lerr := listLocalSegments(w.staging); lerr == nil && holdsRecords(segs) {
				w.setAsideStaging("move: set aside the staging copy of a move that ended while its source is dead; it may be the only copy of the partition's records. Operator action required",
					"owner", a.OwnerID, "owner_state", why, "target", a.TargetID)
				return
			}
		}
	}
	if err := os.RemoveAll(w.staging); err != nil {
		r.logger.Warn("move: remove the staging copy of a move that ended without a flip", "dir", w.staging, "err", err)
	}
}

// setAsideStaging renames this worker's staging copy aside
// (QuarantinePartitionDir), where no later worker's start clears it, and
// logs msg at error level with where it went. The copy is never judged
// or deleted here. If the rename fails, the copy stays at the staging
// path, and the error line says the next move of the partition onto
// this node would clear it.
func (w *moveWorker) setAsideStaging(msg string, args ...any) {
	r := w.r
	base := []any{"topic", w.topic, "partition", w.partition}
	quarantined, err := messaging.QuarantinePartitionDir(w.staging)
	if err != nil {
		r.logger.Error(msg+"; setting it aside failed, so it is still at the staging path, which the next move of this partition onto this node clears: copy it off now",
			append(append(base, "staging", w.staging, "set_aside_err", err), args...)...)
		return
	}
	r.logger.Error(msg, append(append(base, "quarantine_dir", quarantined), args...)...)
}

// ownedStagingIsRedundant reports whether the staging copy of a worker
// that ended without a flip, on a node that owns the partition by now,
// holds nothing the partition's path (dir) lacks. That is so when the
// path holds a copy installed from this move's source and this worker
// never moved an install of its own back to staging: the flip that
// committed was an earlier worker's, made while its install was in
// place (a node restart cancels a worker with its flip pending, and the
// next worker's fresh copy is all staging holds). Anything else keeps
// staging: a copy this worker moved back is the partition's as of the
// flip, and a path without an install may hold none of it.
func (w *moveWorker) ownedStagingIsRedundant(dir string) bool {
	if w.movedBack {
		return false
	}
	m, ok, err := messaging.ReadMoveMarker(dir)
	return err == nil && ok && m.Source == w.source
}

// ownerMayBeGone reports whether the partition's owner (the move's
// source, unless the partition was planned elsewhere since) may no
// longer hold its copy: it reads dead, it has no member record, or its
// record cannot be read. why describes what was read.
func (w *moveWorker) ownerMayBeGone(owner string) (gone bool, why string) {
	if owner == "" {
		return true, "no owner"
	}
	m, err := w.r.store.GetMember(owner)
	switch {
	case errors.Is(err, errs.ErrNotFound):
		return true, "no member record"
	case err != nil:
		return true, "member record unreadable: " + err.Error()
	case m.Status == metastore.MemberDead:
		return true, string(metastore.MemberDead)
	}
	return false, string(m.Status)
}

// holdsRecords reports whether any segment holds bytes.
func holdsRecords(segs []localSegment) bool {
	for _, s := range segs {
		if s.size > 0 {
			return true
		}
	}
	return false
}

// resetSession throws the staged copy away and starts the next attempt
// from scratch, carrying what the session knew about the source (see
// carryFrom): after a rollback that could not move the installed copy
// back to staging, and after a staged copy failed verification.
func (w *moveWorker) resetSession(reason string) {
	r := w.r
	r.logger.Warn("move: copying the partition again from scratch", "topic", w.topic, "partition", w.partition, "reason", reason)
	if w.sess != nil && w.sess.sawInfo {
		w.prevSess = w.sess
	}
	w.sess = nil
	if err := os.RemoveAll(w.staging); err != nil {
		r.logger.Warn("move: clear staging", "dir", w.staging, "err", err)
	}
}

// flipOutcome is what the leader says about an unconfirmed flip.
type flipOutcome int

const (
	// flipUnknown: the leader could not be asked, or its answer cannot
	// decide anything.
	flipUnknown flipOutcome = iota
	// flipDone: this node owns the partition; the flip committed.
	flipDone
	// flipNotYet: the source still owns it and the target is still this
	// node, so the flip did not commit (yet) and can still.
	flipNotYet
	// flipRejected: a leader read shows anything else, or the topic
	// incarnation the copy belongs to is gone. The flip cannot commit any
	// more (the CAS requires the owner and the target this worker saw).
	flipRejected
)

// flipVerdict is resolveFlip's answer.
type flipVerdict struct {
	outcome flipOutcome
	why     string
	// leaderRead: the answer reflects every committed entry (the leader
	// read it behind a Raft barrier, or it is a committed fact of this
	// replica). Only such a NotYet may authorize undoing the install; any
	// other NotYet only lets the worker propose the flip again.
	leaderRead bool
}

// resolveFlip asks the leader for the partition's assignment after a
// flip returned an error: through a Raft barrier when this node is the
// leader (a fresh leader's FSM can lag its log), else over peer RPC,
// which only the barriered leader answers (handleGetAssignment). An
// answer that this node owns the partition is proof on its own (a
// replica shows only committed state); any other answer counts only as a
// leader read, so a follower's or an older leader's answer can confirm a
// flip but never authorize undoing one. expectID, when set, is the topic
// incarnation the install was prepared for: its deletion, or a recreate,
// rejects the flip whatever the assignment says.
func (r *MoveRunner) resolveFlip(ctx context.Context, topicName string, partition int, source, expectID string) flipVerdict {
	if expectID != "" {
		rec, err := r.store.GetTopic(ctx, topicName)
		switch {
		case errors.Is(err, errs.ErrNotFound):
			return flipVerdict{outcome: flipRejected, why: "the topic was deleted", leaderRead: true}
		case err != nil:
			return flipVerdict{outcome: flipUnknown, why: "read topic: " + err.Error()}
		case rec.ID != "" && rec.ID != expectID:
			return flipVerdict{
				outcome: flipRejected, leaderRead: true,
				why: fmt.Sprintf("the topic was recreated (incarnation %s, the copy is for %s)", rec.ID, expectID),
			}
		}
	}
	a, found, leaderRead, why := r.readLeaderAssignment(ctx, topicName, partition)
	if why != "" {
		return flipVerdict{outcome: flipUnknown, why: why}
	}
	var v flipVerdict
	switch {
	case !found:
		v = flipVerdict{outcome: flipRejected, why: "the partition has no assignment (the topic was deleted)"}
	case a.OwnerID == r.selfID:
		return flipVerdict{outcome: flipDone, leaderRead: leaderRead}
	case a.OwnerID == source && a.TargetID == r.selfID:
		v = flipVerdict{outcome: flipNotYet}
	default:
		v = flipVerdict{outcome: flipRejected, why: fmt.Sprintf("the leader has owner %q and target %q", a.OwnerID, a.TargetID)}
	}
	v.leaderRead = leaderRead
	if v.outcome == flipRejected && !leaderRead {
		return flipVerdict{outcome: flipUnknown, why: v.why + " (not a barriered leader read: an older leader or a lagging replica answered)"}
	}
	return v
}

// readLeaderAssignment reads the partition's assignment as the leader
// has it: behind a barrier from the local FSM when this node leads, else
// the leader's answer over peer RPC. found is false for a partition with
// no assignment; leaderRead reports a barriered leader read; why is set
// when there is no usable answer.
func (r *MoveRunner) readLeaderAssignment(ctx context.Context, topicName string, partition int) (a metastore.Assignment, found, leaderRead bool, why string) {
	if r.store.IsLeader() {
		if err := r.store.Barrier(); err != nil {
			return a, false, false, "barrier: " + err.Error()
		}
		got, err := r.store.GetAssignment(topicName, partition)
		if errors.Is(err, errs.ErrNotFound) {
			return a, false, true, ""
		}
		if err != nil {
			return a, false, false, "read assignment: " + err.Error()
		}
		return got, true, true, ""
	}
	addr, err := r.leaderAddr()
	if err != nil {
		return a, false, false, err.Error()
	}
	rpcCtx, cancel := context.WithTimeout(ctx, leaderConfirmRPCTimeout)
	defer cancel()
	if lr, ok := r.peer.(leaderAssignmentReader); ok {
		got, found, leaderRead, err := lr.leaderAssignment(rpcCtx, addr, topicName, partition)
		switch {
		case err != nil:
			return a, false, false, "ask the leader: " + err.Error()
		case !found && !leaderRead:
			return a, false, false, "the node asked reports no assignment, from a read that is not the barriered leader's"
		}
		return got, found, leaderRead, ""
	}
	got, err := r.peer.GetAssignment(rpcCtx, addr, topicName, partition)
	if err != nil {
		return a, false, false, "ask the leader: " + err.Error()
	}
	return got, true, false, ""
}

// proposeFlip proposes the pending install's flip. An error that leaves
// the proposal's outcome unknown restarts the settle window (see
// moveFlipSettle); a refusal or a proposal never sent does not.
func (w *moveWorker) proposeFlip(ctx context.Context) error {
	err := w.r.completeMove(ctx, w.topic, w.partition, w.source)
	if err != nil && !flipSettled(err) {
		w.pending.unknownAt = w.r.now()
	}
	if err != nil {
		w.status.setError(fmt.Errorf("propose the flip: %w", err))
	}
	return err
}

// resolvePending settles an installed copy whose flip was not
// confirmed. It returns true once the move has flipped.
//
//   - the leader says this node owns the partition: done;
//   - the leader confirms the flip cannot commit any more: once no
//     proposal of this worker can still be on its way into the leader's
//     log (moveFlipSettle), the installed copy is moved back to staging
//     and the worker exits (a re-plan starts a fresh one);
//   - otherwise the install stays in place, and the flip is proposed
//     again while the install can still be flipped as it is: under a
//     re-armed freeze whose HWM still matches the copy, for at most
//     movePendingFreezeLimit; or, for a force-promote, while the source
//     stays dead. A source that dies under a pending install turns it
//     into a force-promote once it has been dead long enough, by the
//     leader's stamp and by this worker's own clock. Once the
//     install cannot be flipped as it is, the worker stops proposing
//     and, when a leader read confirms the flip has not committed and no
//     proposal is in flight, moves the install back to staging to drain
//     the source again.
//
// Nothing is deleted on an unconfirmed outcome, and nothing is undone on
// a follower's or an older leader's word. A leader that cannot be asked
// leaves the install in place; the worker asks again after RetryBackoff.
func (w *moveWorker) resolvePending(ctx context.Context) bool {
	r, p := w.r, w.pending
	m, merr := r.store.GetMember(w.source)
	if merr == nil {
		w.observeSource(m)
	}
	// The settle check is taken when the leader read starts, not when it
	// returns: the barrier may be taken anywhere in between, and only a
	// read whose barrier comes after the window can have seen every
	// proposal still on its way into the leader's log.
	readStart := r.now()
	v := r.resolveFlip(ctx, w.topic, w.partition, w.source, p.expectID)
	if v.outcome == flipDone {
		w.pending = nil
		w.flipDone(p.res, p.forcePromoted)
		r.logger.Info("move: partition moved (the flip committed although its reply was an error)",
			"topic", w.topic, "partition", w.partition, "source", w.source, "hwm", p.res.HighWatermark)
		return true
	}
	settled := p.unknownAt.IsZero() || readStart.Sub(p.unknownAt) >= r.flipSettle
	if v.outcome == flipRejected {
		if !settled {
			r.logger.Debug("move: the flip cannot commit; waiting for any proposal still in flight before moving the install back",
				"topic", w.topic, "partition", w.partition, "reason", v.why)
			return false
		}
		r.logger.Warn("move: the leader confirms the flip did not and cannot happen; moving the installed copy back to staging",
			"topic", w.topic, "partition", w.partition, "source", w.source, "reason", v.why)
		restored, err := r.rollbackInstall(w.topic, w.partition, p.expectID, p.installed, p.marker, w.staging)
		if err != nil {
			r.logger.Warn("move: roll back install", "topic", w.topic, "partition", w.partition, "err", err)
		}
		w.movedBack = restored
		w.pending = nil
		w.exit = true
		return false
	}
	if v.outcome == flipUnknown {
		r.logger.Debug("move: flip outcome unknown", "topic", w.topic, "partition", w.partition, "reason", v.why)
	}
	if p.unflippable == "" {
		ok, why, permanent := w.pendingFlippable(ctx, m, merr)
		if !ok {
			if !permanent {
				r.logger.Debug("move: cannot propose the flip again yet", "topic", w.topic, "partition", w.partition, "reason", why)
				return false
			}
			p.unflippable = why
			r.logger.Warn("move: the installed copy can no longer be flipped as it is; waiting for the leader to confirm the flip did not commit",
				"topic", w.topic, "partition", w.partition, "source", w.source, "reason", why)
		}
	}
	if p.unflippable != "" {
		if v.outcome == flipNotYet && v.leaderRead && settled {
			w.rollbackPending(p.unflippable)
		}
		return false
	}
	if err := w.proposeFlip(ctx); err != nil {
		r.logger.Warn("move: flip not confirmed again; will ask the leader", "topic", w.topic, "partition", w.partition, "err", err)
		return false
	}
	w.pending = nil
	w.flipDone(p.res, p.forcePromoted)
	r.logger.Info("move: partition moved (flip proposed again)",
		"topic", w.topic, "partition", w.partition, "source", w.source, "hwm", p.res.HighWatermark)
	return true
}

// pendingFlippable reports whether the pending install may be flipped
// now (m and merr are the source's member record as just read). When it
// may not, why says why, and permanent that it never can as it is.
func (w *moveWorker) pendingFlippable(ctx context.Context, m metastore.Member, merr error) (ok bool, why string, permanent bool) {
	r, p := w.r, w.pending
	if err := r.checkTopicIncarnation(w.topic, p.expectID); err != nil {
		return false, err.Error(), false
	}
	if !p.forcePromoted && merr == nil && w.sourceDeadLongEnough(m) {
		// The install is the whole copy as of the fence; the source died
		// with the flip unconfirmed. Flip it as a force-promote would.
		p.forcePromoted = true
		r.logger.Warn("move: the source died with the flip unconfirmed; flipping the installed copy as a force-promote",
			"topic", w.topic, "partition", w.partition, "source", w.source, "hwm", p.res.HighWatermark)
	}
	if p.forcePromoted {
		switch {
		case merr != nil:
			return false, "read the source's member record: " + merr.Error(), false
		case m.Status != metastore.MemberDead:
			return false, "the source is alive again; its copy may have moved on", true
		}
		return true, "", false
	}
	if r.now().Sub(p.since) > movePendingFreezeLimit {
		return false, fmt.Sprintf("the flip did not commit within %s; the source's freeze is left to lapse", movePendingFreezeLimit), true
	}
	fence, err := r.rearm(ctx, p.sourceAddr, w.topic, w.partition, p.token)
	switch {
	case errors.Is(err, messaging.ErrHandoffFreezeLapsed):
		return false, "the handoff freeze lapsed before the flip committed", true
	case err != nil:
		return false, "re-arm the handoff freeze: " + err.Error(), false
	case fence.HighWatermark != p.res.HighWatermark:
		return false, fmt.Sprintf("the source's hwm moved (%d, the copy has %d)", fence.HighWatermark, p.res.HighWatermark), true
	}
	return true, "", false
}

// rollbackPending moves an install whose flip a leader read confirmed
// has not committed back to staging, because it can no longer be flipped
// as it is (reason); the worker then drains the source again. When the
// copy cannot be moved back, the copy starts over.
func (w *moveWorker) rollbackPending(reason string) {
	r, p := w.r, w.pending
	w.pending = nil
	r.logger.Warn("move: moving the installed copy back to staging; the flip did not commit and the copy must be drained again",
		"topic", w.topic, "partition", w.partition, "reason", reason)
	restored, err := r.rollbackInstall(w.topic, w.partition, p.expectID, p.installed, p.marker, w.staging)
	if err != nil {
		r.logger.Warn("move: roll back install", "topic", w.topic, "partition", w.partition, "err", err)
	}
	w.movedBack = restored
	if !restored {
		w.resetSession("the installed copy could not be moved back to staging")
	}
}
