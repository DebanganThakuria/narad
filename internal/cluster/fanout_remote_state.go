package cluster

// Per-cursor state of a remote child, for the children listing (the
// stall reason, the stuck record, the recovery point) and for the
// metrics. A running remote cursor owns one remoteCursor; its lanes and
// its loop write it, and the cursor-stats handlers read a snapshot.

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"weak"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/remote/sink"
)

// remoteCursor is one remote cursor's state. Every field is guarded by
// mu. mu is never held across I/O, so the listing's snapshot never
// waits on the target. The runtime target check is the link's, shared
// with the link's other cursors on this node (check).
type remoteCursor struct {
	key fanoutCursorKey

	// pubMu serializes publishRemoteState, so the state gauge's delete
	// of the previous series and set of the new one never interleave
	// with another publisher's (two series at 1 would page forever).
	// It also guards loggedState and loggedAt: the stall the cursor
	// last logged entering, and when.
	pubMu       sync.Mutex
	loggedState string
	loggedAt    time.Time

	mu sync.Mutex
	// remote is the link's remote name, for the log lines; answerState
	// and answerStatus are the state and HTTP status of the last answer
	// that refused a chunk.
	remote       string
	answerState  string
	answerStatus int
	// stall is a cursor-wide state the last answer or check put the
	// cursor in (forbidden, target_missing, remote_missing, ...), ""
	// when none.
	stall string
	// stallDue is when the stall's next retry may go out: one stall
	// interval after the answer that stalled the cursor, earlier when a
	// resume asks for one at once. It lives on the cursor, not on a
	// lane, so a slab read again (the held budget had no room) still
	// sends its retry once the wait is over.
	stallDue time.Time
	// lanes holds each lane's state ("" when it has nothing to report)
	// and blocked each lane's stuck record.
	lanes   map[int]string
	blocked map[int]topic.RemoteBlock
	paused  bool
	// lastSuccessMs is when the target last accepted a chunk.
	lastSuccessMs int64
	// lagSeconds is the age of the oldest record not yet on the target;
	// headroom the parent retention minus it (nil when retention is 0).
	lagSeconds float64
	headroom   *float64
	// published is the state last exported on the state gauge.
	published string
	// caps holds each lane's adaptive chunk cap across slabs.
	caps map[int]*sink.ChunkCap
	// progress is what the target accepted (or an admin skipped) of the
	// records at the cursor's unadvanced position, kept across re-reads
	// of that slab; cleared once the cursor advances.
	progress slabProgress
	// idleLookup and idleCheck are what a quiet cursor's own lookup and
	// target check found (remoteIdle): a lookup state (remote_missing,
	// credential_unreadable, ...) and a failed check's class. They show
	// a stall on a link with nothing to send; a clean lookup, a verified
	// check or a committed chunk clears them.
	idleLookup string
	idleCheck  string
	// blockLogged holds the offsets the cursor logged a lane blocking
	// on, so a retry that meets the same refusal logs nothing; cleared
	// once the cursor advances. rereadLoggedAt is when the cursor last
	// logged a re-read.
	blockLogged    map[int64]bool
	rereadLoggedAt time.Time

	// check is the link's runtime target check on this node: its
	// schedule and its verdict. A cursor the runner registers shares
	// it with every other cursor of the link; never nil.
	check *linkCheck
}

// linkCheckKey names one link (one attachment of a remote child).
type linkCheckKey struct {
	child string
	epoch string
}

// linkCheck is one link's runtime target check on this node, shared by
// the link's cursors: every cursor lists the same target topic, so the
// target sees one children listing per node per interval (and per gate
// reopen), not one per partition. mu guards the schedule and the
// marker of the check in flight (single-flight); vmu guards the
// verdict. Neither is held across I/O, so no lane and no listing waits
// on the target behind a lock.
type linkCheck struct {
	// refs counts the registered cursors sharing it (remoteSender.mu).
	refs int
	// sinceMs is when this node began checking the link (set once, at
	// creation): a link with no successful check since then counts as
	// unverified once the window has passed.
	sinceMs int64

	// forceCheck makes the next chunk re-run the target check; atomic so
	// asking for one never waits on a check in flight.
	forceCheck atomic.Bool

	mu sync.Mutex
	// checkedEntry (weak: it only tells whether the entry changed, and
	// must not keep a dropped remote's header and clients reachable),
	// checkedGateEpoch, checkedTargetID and nextCheck
	// decide when the next check is due: at start, after the remote's
	// entry changed (a new credential or URL), after its gate reopened,
	// after the recorded target changed, and every check interval.
	checkedEntry     weak.Pointer[remote.Entry]
	checkedGateEpoch uint64
	checkedTargetID  string
	nextCheck        time.Time
	// inflight, while a check runs, is closed when it ends.
	inflight chan struct{}

	vmu sync.Mutex
	// targetState is what the last successful check found
	// (target_replaced, target_has_remote_children or ""), verifiedMs
	// when a check last succeeded, and failedClass the class of the
	// last check when it did not verify ("" after one that did).
	targetState string
	verifiedMs  int64
	failedClass string
}

// verdict is the last successful check's state and time.
func (lc *linkCheck) verdict() (state string, verifiedMs int64) {
	lc.vmu.Lock()
	defer lc.vmu.Unlock()
	return lc.targetState, lc.verifiedMs
}

// record publishes a check's answer.
func (lc *linkCheck) record(res sink.TargetResult, at time.Time) {
	lc.vmu.Lock()
	defer lc.vmu.Unlock()
	if res.Verified {
		lc.targetState, lc.verifiedMs, lc.failedClass = res.State, at.UnixMilli(), ""
		return
	}
	lc.failedClass = res.Class
}

// lastFailure is the class of the last check when it did not verify,
// "" when it did or none ran.
func (lc *linkCheck) lastFailure() string {
	lc.vmu.Lock()
	defer lc.vmu.Unlock()
	return lc.failedClass
}

func newRemoteCursor(key fanoutCursorKey) *remoteCursor {
	return &remoteCursor{key: key, lanes: map[int]string{}, blocked: map[int]topic.RemoteBlock{}, check: &linkCheck{}}
}

// setRemote records the link's remote name.
func (c *remoteCursor) setRemote(name string) {
	c.mu.Lock()
	c.remote = name
	c.mu.Unlock()
}

// noteAnswer records the state and HTTP status of a chunk's answer
// ("" and 0 once a chunk is accepted).
func (c *remoteCursor) noteAnswer(state string, status int) {
	c.mu.Lock()
	c.answerState, c.answerStatus = state, status
	c.mu.Unlock()
}

// logContext is the remote name and, when the last answer put the
// cursor in state, that answer's HTTP status (0 otherwise).
func (c *remoteCursor) logContext(state string) (remote string, status int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.answerState == state {
		status = c.answerStatus
	}
	return c.remote, status
}

// clearStall clears the cursor-wide stall: the target answered.
func (c *remoteCursor) clearStall() {
	c.mu.Lock()
	c.stall, c.stallDue = "", time.Time{}
	c.mu.Unlock()
}

// stallFor puts the cursor in a stall whose next retry is due after
// retry.
func (c *remoteCursor) stallFor(state string, retry time.Duration) {
	c.mu.Lock()
	c.stall, c.stallDue = state, time.Now().Add(retry)
	c.mu.Unlock()
}

// stallWait reports the cursor-wide stall and how long until its next
// retry is due (0 or less: due now).
func (c *remoteCursor) stallWait() (string, time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stall == "" {
		return "", 0
	}
	return c.stall, time.Until(c.stallDue)
}

// setLane records one lane's state; "" clears it.
func (c *remoteCursor) setLane(lane int, state string) {
	c.mu.Lock()
	if state == "" {
		delete(c.lanes, lane)
	} else {
		c.lanes[lane] = state
	}
	c.mu.Unlock()
}

func (c *remoteCursor) setBlocked(lane int, b *topic.RemoteBlock) {
	c.mu.Lock()
	if b == nil {
		delete(c.blocked, lane)
	} else {
		c.blocked[lane] = *b
	}
	c.mu.Unlock()
}

// setPaused records whether the link is paused. A resume makes a
// stall's retry due at once: the admin changed the link, as a change
// during a stall wait does.
func (c *remoteCursor) setPaused(paused bool) {
	c.mu.Lock()
	if c.paused && !paused {
		c.stallDue = time.Time{}
	}
	c.paused = paused
	c.mu.Unlock()
}

func (c *remoteCursor) succeeded(now time.Time) {
	c.mu.Lock()
	c.lastSuccessMs = now.UnixMilli()
	c.mu.Unlock()
}

// setLag records the recovery point: lag is the age of the oldest
// unshipped record, retentionMs the parent's retention (0 forever).
func (c *remoteCursor) setLag(lag time.Duration, retentionMs int64) {
	c.mu.Lock()
	c.lagSeconds = max(lag.Seconds(), 0)
	if retentionMs > 0 {
		h := float64(retentionMs)/1000 - c.lagSeconds
		c.headroom = &h
	} else {
		c.headroom = nil
	}
	c.mu.Unlock()
}

// clearLanes forgets per-slab lane state when a slab completes.
func (c *remoteCursor) clearLanes() {
	c.mu.Lock()
	clear(c.lanes)
	clear(c.blocked)
	c.mu.Unlock()
}

// setIdleLookup records (or, with "", clears) a quiet cursor's lookup
// state.
func (c *remoteCursor) setIdleLookup(state string) {
	c.mu.Lock()
	c.idleLookup = state
	c.mu.Unlock()
}

// setIdleCheck records a quiet cursor's failed target check.
func (c *remoteCursor) setIdleCheck(state string) {
	c.mu.Lock()
	c.idleCheck = state
	c.mu.Unlock()
}

// clearIdle forgets the quiet cursor's states: a chunk got through.
func (c *remoteCursor) clearIdle() {
	c.mu.Lock()
	c.idleLookup, c.idleCheck = "", ""
	c.mu.Unlock()
}

// targetVerdict is the link's last successful target check's state.
func (c *remoteCursor) targetVerdict() string {
	state, _ := c.check.verdict()
	return state
}

// remoteCursorSnapshot is what the listing and the gauges read.
type remoteCursorSnapshot struct {
	state         string
	blocked       *topic.RemoteBlock
	lastSuccessMs int64
	lagSeconds    float64
	headroom      *float64
	verifiedMs    int64
	checkSinceMs  int64
}

// snapshot folds the cursor's states into its worst one: the
// cursor-wide stall, the target check's verdict, every lane, then
// paused; running when none applies.
func (c *remoteCursor) snapshot() remoteCursorSnapshot {
	targetState, verifiedMs := c.check.verdict()
	c.mu.Lock()
	defer c.mu.Unlock()
	state := topic.WorseRemoteState(c.stall, targetState)
	state = topic.WorseRemoteState(state, c.idleLookup)
	state = topic.WorseRemoteState(state, c.idleCheck)
	for _, s := range c.lanes {
		state = topic.WorseRemoteState(state, s)
	}
	if state == "" && c.paused {
		state = topic.RemoteStatePaused
	}
	if state == "" {
		state = topic.RemoteStateRunning
	}
	snap := remoteCursorSnapshot{
		state:         state,
		lastSuccessMs: c.lastSuccessMs,
		lagSeconds:    c.lagSeconds,
		verifiedMs:    verifiedMs,
		checkSinceMs:  c.check.sinceMs,
	}
	if c.headroom != nil {
		h := *c.headroom
		snap.headroom = &h
	}
	for _, b := range c.blocked {
		if snap.blocked == nil || b.Offset < snap.blocked.Offset {
			bb := b
			snap.blocked = &bb
		}
	}
	return snap
}

// overlay fills the remote fields of a cursor stat from snap.
func (snap remoteCursorSnapshot) overlay(stat *topic.FanoutCursorStat) {
	stat.State = snap.state
	stat.BlockedAt = snap.blocked
	stat.LastSuccessMs = snap.lastSuccessMs
	stat.LagSeconds = snap.lagSeconds
	stat.TargetVerifiedAtMs = snap.verifiedMs
	stat.TargetCheckSinceMs = snap.checkSinceMs
}

// remoteCursorFor returns the state of the running remote cursor for
// (parent, partition, child), if this node runs one.
func (r *FanoutRunner) remoteCursorFor(parent string, partition int, child string) *remoteCursor {
	if r == nil {
		return nil
	}
	r.remoteMu.Lock()
	defer r.remoteMu.Unlock()
	for key, c := range r.remoteCursors {
		if key.parent == parent && key.partition == partition && key.child == child {
			return c
		}
	}
	return nil
}

// OverlayRemoteCursorStats fills the remote link fields of the cursor
// stats this node reported for parent from its running remote cursors.
// A stat of a remote child with no running cursor here (not spawned
// yet, or stopped) reports state unknown.
func (r *FanoutRunner) OverlayRemoteCursorStats(parent string, stats []topic.FanoutCursorStat) []topic.FanoutCursorStat {
	if r == nil {
		return stats
	}
	for i := range stats {
		stat := &stats[i]
		child, err := r.store.GetTopic(context.Background(), stat.Child)
		if err != nil || !child.IsRemoteChild() {
			continue
		}
		stat.Node = r.selfID
		if c := r.remoteCursorFor(parent, stat.Partition, stat.Child); c != nil {
			c.snapshot().overlay(stat)
			continue
		}
		stat.State = topic.RemoteStateUnknown
	}
	return stats
}

// slabProgress records, for each lane of a slab the cursor has not yet
// advanced past, the highest offset the target accepted or an admin
// skipped. A lane sends its records in order, so every record of that
// lane at or below the mark is done. The marks are only valid under the
// lane count they were taken with.
type slabProgress struct {
	lanes int
	marks map[int]int64
}

// unshipped drops from records those the target already has: a re-read
// of the slab (the held budget could not keep a waiting lane's records,
// or a pass that left records unsent) sends only what is still missing.
// It never changes records itself.
func (c *remoteCursor) unshipped(records []topic.KeyedRecord) []topic.KeyedRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.progress.marks) == 0 {
		return records
	}
	out := make([]topic.KeyedRecord, 0, len(records))
	for _, r := range records {
		if mark, ok := c.progress.marks[sink.LaneOf(r, c.progress.lanes)]; ok && r.Offset <= mark {
			continue
		}
		out = append(out, r)
	}
	return out
}

// noteProgress merges what one pass over the slab got done. Under the
// same lane count a lane's mark only grows; under a new one the old
// marks no longer name lanes and are replaced (records they covered may
// be sent again: duplicates, never loss).
func (c *remoteCursor) noteProgress(lanes int, marks map[int]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.progress.lanes != lanes || c.progress.marks == nil {
		c.progress = slabProgress{lanes: lanes, marks: map[int]int64{}}
	}
	for lane, off := range marks {
		if prev, ok := c.progress.marks[lane]; !ok || off > prev {
			c.progress.marks[lane] = off
		}
	}
}

// clearProgress forgets the slab's progress once every record of it is
// on the target and the cursor advances past it.
func (c *remoteCursor) clearProgress() {
	c.mu.Lock()
	c.progress = slabProgress{}
	c.blockLogged = nil
	c.mu.Unlock()
}

// firstBlockOn reports whether a lane blocking on offset is the first
// the cursor logs, and notes it.
func (c *remoteCursor) firstBlockOn(offset int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blockLogged[offset] {
		return false
	}
	if c.blockLogged == nil {
		c.blockLogged = map[int64]bool{}
	}
	c.blockLogged[offset] = true
	return true
}

// rereadLogDue reports whether the cursor may log a re-read now: at
// most once per gap.
func (c *remoteCursor) rereadLogDue(now time.Time, gap time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.rereadLoggedAt.IsZero() && now.Sub(c.rereadLoggedAt) < gap {
		return false
	}
	c.rereadLoggedAt = now
	return true
}
