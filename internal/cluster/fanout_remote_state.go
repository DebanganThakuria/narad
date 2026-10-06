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

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/remote/sink"
)

// remoteCursor is one remote cursor's state. Every field is guarded by
// mu except the check-scheduling fields, which checkMu guards. mu is
// never held across I/O, so the listing's snapshot never waits on the
// target; checkMu only makes the target check single-flight.
type remoteCursor struct {
	key fanoutCursorKey

	// pubMu serializes publishRemoteState, so the state gauge's delete
	// of the previous series and set of the new one never interleave
	// with another publisher's (two series at 1 would page forever).
	pubMu sync.Mutex

	mu sync.Mutex
	// stall is a cursor-wide state the last answer or check put the
	// cursor in (forbidden, target_missing, remote_missing, ...), ""
	// when none.
	stall string
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
	// targetState is what the last successful target check found
	// (target_replaced, target_has_remote_children or ""), and
	// verifiedMs when a target check last succeeded. The check writes
	// them once it has its answer.
	targetState string
	verifiedMs  int64
	// idleLookup and idleCheck are what a quiet cursor's own lookup and
	// target check found (remoteIdle): a lookup state (remote_missing,
	// credential_unreadable, ...) and a failed check's class. They show
	// a stall on a link with nothing to send; a clean lookup, a verified
	// check or a committed chunk clears them.
	idleLookup string
	idleCheck  string

	// forceCheck makes the next chunk re-run the target check; atomic so
	// asking for one never waits on a check in flight.
	forceCheck atomic.Bool

	checkMu sync.Mutex
	// checkedEntry, checkedGateEpoch, checkedTargetID and nextCheck
	// decide when the next check is due: at start, after the remote's
	// entry changed (a new credential or URL), after its gate reopened,
	// after the recorded target changed, and every check interval.
	checkedEntry     *remote.Entry
	checkedGateEpoch uint64
	checkedTargetID  string
	nextCheck        time.Time
}

func newRemoteCursor(key fanoutCursorKey) *remoteCursor {
	return &remoteCursor{key: key, lanes: map[int]string{}, blocked: map[int]topic.RemoteBlock{}}
}

// setStall sets (or, with "", clears) the cursor-wide stall state.
func (c *remoteCursor) setStall(state string) {
	c.mu.Lock()
	c.stall = state
	c.mu.Unlock()
}

func (c *remoteCursor) stallState() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stall
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

func (c *remoteCursor) setPaused(paused bool) {
	c.mu.Lock()
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

// setTargetVerdict records a successful target check's answer.
func (c *remoteCursor) setTargetVerdict(state string, at time.Time) {
	c.mu.Lock()
	c.targetState = state
	c.verifiedMs = at.UnixMilli()
	c.idleCheck = ""
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

// targetVerdict is the last successful target check's state.
func (c *remoteCursor) targetVerdict() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.targetState
}

// remoteCursorSnapshot is what the listing and the gauges read.
type remoteCursorSnapshot struct {
	state         string
	blocked       *topic.RemoteBlock
	lastSuccessMs int64
	lagSeconds    float64
	headroom      *float64
	verifiedMs    int64
}

// snapshot folds the cursor's states into its worst one: the
// cursor-wide stall, the target check's verdict, every lane, then
// paused; running when none applies.
func (c *remoteCursor) snapshot() remoteCursorSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := topic.WorseRemoteState(c.stall, c.targetState)
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
		verifiedMs:    c.verifiedMs,
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
	c.mu.Unlock()
}
