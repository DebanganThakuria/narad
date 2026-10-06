package cluster

// The remote child branch of the fan-out engine. A remote child's cursor
// reads its parent partition exactly like a local child's; its slab goes
// to a topic on another cluster as batch-produce requests instead of to
// local child partitions, and the cursor advances only after the target
// accepted every record of the slab (or an admin skipped one).
//
// Each slab splits into lanes (a key always on one lane), and each lane
// ships its records in order, one chunk at a time. Everything a lane may
// wait for (the remote's gate, a stall only a change fixes, a pause, a
// record the target refuses) keeps the records in hand: copied off the
// parent log into the per-node held budget, or, when the budget is full,
// dropped and re-read from the unadvanced cursor later. Nothing about a
// remote (a changed password, a deleted remote, a refused address) moves
// a cursor; each only stalls it, with the reason in the listing.

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/remote"
	"github.com/debanganthakuria/narad/internal/remote/sink"
)

// SetRemotes gives the runner the credential cache and the per-node
// held-record budget (remotes.max_held_bytes). Call before Run. Without
// it remote cursors hold their positions in state unavailable.
func (r *FanoutRunner) SetRemotes(l remote.Lookup, heldBudgetBytes int64) {
	s := newRemoteSender(r, l, heldBudgetBytes)
	r.remoteMu.Lock()
	r.remote = s
	r.remoteMu.Unlock()
}

// remoteSender is the node's remote send path: the credential lookup,
// the held budget and the per-remote pacing.
type remoteSender struct {
	r        *FanoutRunner
	lookup   remote.Lookup
	held     *sink.HeldBudget
	classify sink.Classifier

	// stallRetry and changePoll pace waits, and lagRefresh keeps the
	// recovery-point gauges live while a slab does not ship; tests
	// shorten them.
	stallRetry time.Duration
	changePoll time.Duration
	lagRefresh time.Duration

	mu      sync.Mutex
	remotes map[string]*remoteState
}

func newRemoteSender(r *FanoutRunner, l remote.Lookup, heldBudget int64) *remoteSender {
	s := &remoteSender{
		r:          r,
		lookup:     l,
		stallRetry: sink.StallRetry,
		changePoll: 200 * time.Millisecond,
		lagRefresh: 5 * time.Second,
		remotes:    map[string]*remoteState{},
	}
	var observe func(int64)
	if rl := r.remoteMetrics(); rl != nil {
		observe = func(v int64) { rl.HeldBytes.Set(float64(v)) }
	}
	s.held = sink.NewHeldBudget(heldBudget, observe)
	return s
}

// remoteState is one remote's pacing on this node, keyed by name and
// independent of the cache entry, which a credential or limits change
// replaces.
type remoteState struct {
	name string
	gate *sink.Gate
	sem  *sink.Semaphore

	mu          sync.Mutex
	entry       *remote.Entry
	caps        sink.Capabilities
	capsKnown   bool
	capsID      string
	capsCV      uint64
	capsRetryAt time.Time
	probing     bool
	laneCaps    map[*sink.ChunkCap]int
}

func (s *remoteSender) remoteState(name string) *remoteState {
	s.mu.Lock()
	defer s.mu.Unlock()
	rs := s.remotes[name]
	if rs == nil {
		rs = &remoteState{
			name:     name,
			gate:     sink.NewGate(),
			sem:      sink.NewSemaphore(16),
			laneCaps: map[*sink.ChunkCap]int{},
		}
		s.remotes[name] = rs
	}
	return rs
}

// watchChanges re-reads every known remote's entry each time the
// lookup publishes, until ctx ends: a new credential version reopens
// the remote's gate at once, even while every lane of it waits on the
// gate's 30 s ceiling after an auth failure.
func (s *remoteSender) watchChanges(ctx context.Context) {
	if s.lookup == nil {
		return
	}
	for {
		changed := s.lookup.Changed()
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
		s.mu.Lock()
		states := make([]*remoteState, 0, len(s.remotes))
		for _, rs := range s.remotes {
			states = append(states, rs)
		}
		s.mu.Unlock()
		for _, rs := range states {
			if e, err := s.lookup.Get(rs.name); err == nil {
				rs.observe(e)
			}
		}
	}
}

// changed is the lookup's change channel (nil without a lookup).
func (s *remoteSender) changed() <-chan struct{} {
	if s.lookup == nil {
		return nil
	}
	return s.lookup.Changed()
}

// entry looks the remote up, as every chunk does. On success it also
// notes a new credential version (which reopens the gate at once) and
// the remote's current in-flight limit. On failure it returns the link
// state the lookup error stands for.
func (s *remoteSender) entry(name string) (*remote.Entry, *remoteState, string) {
	rs := s.remoteState(name)
	if s.lookup == nil {
		return nil, rs, topic.RemoteStateUnavailable
	}
	e, err := s.lookup.Get(name)
	switch {
	case err == nil:
		rs.observe(e)
		return e, rs, ""
	case errors.Is(err, remote.ErrRemoteMissing):
		return nil, rs, topic.RemoteStateRemoteMissing
	case errors.Is(err, remote.ErrCredentialUnreadable):
		return nil, rs, topic.RemoteStateCredentialUnreadable
	case errors.Is(err, remote.ErrNodeInsecure):
		return nil, rs, topic.RemoteStateNodeInsecure
	}
	return nil, rs, topic.RemoteStateUnavailable
}

func (rs *remoteState) observe(e *remote.Entry) {
	rs.mu.Lock()
	prev := rs.entry
	rs.entry = e
	rs.mu.Unlock()
	if prev != nil && (prev.RemoteID() != e.RemoteID() || prev.CredentialVersion() != e.CredentialVersion()) {
		rs.gate.Reset()
	}
	rs.sem.Resize(e.Limits().MaxInFlight)
}

// superseded reports whether a newer entry for the remote (another
// remote under the name, or a newer credential version) arrived since e
// was looked up. An answer to e then says nothing about the remote as it
// now stands.
func (rs *remoteState) superseded(e *remote.Entry) bool {
	rs.mu.Lock()
	cur := rs.entry
	rs.mu.Unlock()
	return cur != nil && cur != e &&
		(cur.RemoteID() != e.RemoteID() || cur.CredentialVersion() > e.CredentialVersion())
}

// capabilities is what the remote's batch produce takes, probed once
// per remote per node and credential version. While a probe runs, or
// after one proved nothing, the conservative defaults apply.
func (s *remoteSender) capabilities(ctx context.Context, rs *remoteState, e *remote.Entry, topicName string) sink.Capabilities {
	rs.mu.Lock()
	if rs.capsKnown && rs.capsID == e.RemoteID() && rs.capsCV == e.CredentialVersion() {
		caps := rs.caps
		rs.mu.Unlock()
		return caps
	}
	if rs.probing || time.Now().Before(rs.capsRetryAt) {
		rs.mu.Unlock()
		return sink.DefaultCapabilities()
	}
	rs.probing = true
	rs.mu.Unlock()

	pctx, cancel := context.WithTimeout(ctx, requestTimeout(e))
	caps, err := sink.ProbeCapabilities(pctx, e, topicName, e.Limits().Compression == "zstd")
	cancel()

	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.probing = false
	if err != nil {
		rs.capsRetryAt = time.Now().Add(sink.StallRetry)
		return sink.DefaultCapabilities()
	}
	rs.caps, rs.capsKnown, rs.capsID, rs.capsCV = caps, true, e.RemoteID(), e.CredentialVersion()
	return caps
}

// disableZstd turns compression off for this remote on this node until
// the next capability probe, after the target could not decode a
// compressed chunk.
func (rs *remoteState) disableZstd(e *remote.Entry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.caps.Zstd = false
	rs.capsRetryAt = time.Now().Add(time.Duration(e.Limits().CheckIntervalMs) * time.Millisecond)
	if rs.capsKnown {
		rs.capsKnown = false
	}
}

// noteLaneCap records a lane's chunk cap and returns the smallest one of
// the remote's live lanes (the degraded-path gauge).
func (rs *remoteState) noteLaneCap(c *sink.ChunkCap, bytes int) int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if bytes <= 0 {
		delete(rs.laneCaps, c)
	} else {
		rs.laneCaps[c] = bytes
	}
	smallest := sink.MaxChunkBytes
	for _, b := range rs.laneCaps {
		smallest = min(smallest, b)
	}
	return smallest
}

func requestTimeout(e *remote.Entry) time.Duration {
	return time.Duration(e.Limits().RequestTimeoutMs) * time.Millisecond
}

// registerRemoteCursor records the state of a remote cursor this node
// now runs; the returned function forgets it.
func (r *FanoutRunner) registerRemoteCursor(key fanoutCursorKey) (*remoteCursor, func()) {
	c := newRemoteCursor(key)
	r.remoteMu.Lock()
	if r.remoteCursors == nil {
		r.remoteCursors = map[fanoutCursorKey]*remoteCursor{}
	}
	r.remoteCursors[key] = c
	r.remoteMu.Unlock()
	return c, func() {
		r.remoteMu.Lock()
		if r.remoteCursors[key] == c {
			delete(r.remoteCursors, key)
		}
		r.remoteMu.Unlock()
		r.unpublishRemoteState(c)
	}
}

func (r *FanoutRunner) remoteCursorOf(key fanoutCursorKey) *remoteCursor {
	r.remoteMu.Lock()
	defer r.remoteMu.Unlock()
	return r.remoteCursors[key]
}

func (r *FanoutRunner) remoteMetrics() *metrics.RemoteLinkMetrics {
	if r.metrics == nil {
		return nil
	}
	return r.metrics.RemoteLink
}

// sender returns the remote send path, a lookup-less one when SetRemotes
// was never called (its cursors hold in state unavailable).
func (r *FanoutRunner) sender() *remoteSender {
	r.remoteMu.Lock()
	defer r.remoteMu.Unlock()
	if r.remote == nil {
		r.remote = newRemoteSender(r, nil, 0)
	}
	return r.remote
}

// remoteBeforeRead runs before a remote cursor reads its next slab:
// while the link is paused it reads nothing (refreshing its gauges from
// a one-record peek every stall retry), and while the remote's gate
// backs off it waits for the probe to be due, so a dead remote costs no
// reads. It returns the slab caps to read with, and false when the
// cursor must stop.
func (r *FanoutRunner) remoteBeforeRead(ctx context.Context, key fanoutCursorKey, cur *remoteCursor, next int64) (maxRecords int, maxBytes int64, ok bool) {
	s := r.sender()
	for {
		// The version before the record: a resume applied between the two
		// reads wakes the wait below at once instead of after stallRetry.
		version := r.store.TopicVersion(key.child)
		stub, err := r.store.GetTopic(ctx, key.child)
		if err != nil || !stub.IsRemoteChild() || stub.Parent != key.parent || stub.AttachEpoch != key.epoch {
			// Dissolved or replaced: the commit's link check refuses too,
			// and the reconciler stops this cursor.
			return defaultFanoutMaxBatchRecords, defaultFanoutMaxBatchBytes, ctx.Err() == nil
		}
		if stub.Remote.Paused {
			cur.setPaused(true)
			r.refreshRemoteLag(ctx, key, cur, next, nil)
			r.publishRemoteState(cur)
			if !s.waitChange(ctx, key.child, version, s.stallRetry, nil) {
				return 0, 0, false
			}
			continue
		}
		cur.setPaused(false)
		lanes := stub.Remote.EffectiveLanes()
		if rs := s.remoteState(stub.Remote.Name); rs.gate.Closed() {
			r.refreshRemoteLag(ctx, key, cur, next, nil)
			r.publishRemoteState(cur)
			if err := rs.gate.WaitDue(ctx); err != nil {
				return 0, 0, false
			}
		}
		return lanes * sink.SlabRecordsPerLane, int64(lanes) * sink.SlabBytesPerLane, true
	}
}

// waitChange waits up to d for ctx to end, the remote registry to
// change on this node, or the stub's record to move past version, and
// runs tick (when not nil) every few seconds meanwhile. It reports false
// only when ctx ended.
func (s *remoteSender) waitChange(ctx context.Context, stub string, version uint64, d time.Duration, tick func()) bool {
	deadline := time.Now().Add(d)
	poll := time.NewTicker(s.changePoll)
	defer poll.Stop()
	changed := s.changed()
	lastTick := time.Now()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-changed:
			return true
		case now := <-poll.C:
			if s.r.store.TopicVersion(stub) != version || !now.Before(deadline) {
				return true
			}
			if tick != nil && now.Sub(lastTick) >= 5*time.Second {
				lastTick = now
				tick()
			}
		}
	}
}

// refreshRemoteLag keeps the recovery-point gauges live while the link
// does not advance: lag_messages from the parent's high watermark,
// lag_seconds from the oldest record not yet on the target (the one at
// next when the cursor holds none; oldestHeldMs otherwise), and the
// headroom left before drop-behind.
func (r *FanoutRunner) refreshRemoteLag(ctx context.Context, key fanoutCursorKey, cur *remoteCursor, next int64, oldestHeldMs *int64) {
	peek, err := r.broker.ReadFanoutSlab(ctx, key.parent, key.partition,
		topic.FanoutReadOpts{FromOffset: next, MaxRecords: 1, MaxBytes: 1})
	if err != nil {
		return
	}
	r.recordLag(key, peek.HighWatermark-next)
	var retention int64
	if parent, err := r.store.GetTopic(ctx, key.parent); err == nil {
		retention = parent.RetentionMs
	}
	oldest := int64(0)
	switch {
	case oldestHeldMs != nil && *oldestHeldMs > 0:
		oldest = *oldestHeldMs
	case len(peek.Records) > 0:
		oldest = peek.Records[0].CommittedAtUnixMs
	}
	lag := time.Duration(0)
	if oldest > 0 {
		lag = time.Since(time.UnixMilli(oldest))
	}
	cur.setLag(lag, retention)
}

// publishRemoteState exports the cursor's state, lag and headroom. The
// lanes, the lag refresher and the commit all publish; pubMu makes each
// publish (the snapshot, the swap of the published state, the delete of
// the previous series and the set of the new one) one step, so an older
// snapshot's set never lands after a newer publish's delete.
func (r *FanoutRunner) publishRemoteState(cur *remoteCursor) {
	rl := r.remoteMetrics()
	if rl == nil {
		return
	}
	cur.pubMu.Lock()
	defer cur.pubMu.Unlock()
	snap := cur.snapshot()
	key := cur.key
	part := fanoutPartitionLabel(key.partition)
	cur.mu.Lock()
	previous := cur.published
	cur.published = snap.state
	cur.mu.Unlock()
	if previous != snap.state && previous != "" {
		rl.State.DeleteLabelValues(key.parent, key.child, part, previous)
	}
	rl.State.WithLabelValues(key.parent, key.child, part, snap.state).Set(1)
	rl.LagSeconds.WithLabelValues(key.parent, key.child, part).Set(snap.lagSeconds)
	if snap.headroom != nil {
		rl.RetentionHeadroomSeconds.WithLabelValues(key.parent, key.child, part).Set(*snap.headroom)
	} else {
		rl.RetentionHeadroomSeconds.DeleteLabelValues(key.parent, key.child, part)
	}
	if snap.lastSuccessMs > 0 {
		rl.LastSuccessTimestampSeconds.WithLabelValues(key.parent, key.child).Set(float64(snap.lastSuccessMs) / 1000)
	}
}

// unpublishRemoteState drops the per-partition series a stopped cursor
// exported. The cursor stopped because its link dissolved, its
// partition moved to another owner, its delay changed its key, or it
// exited for the reconciler to respawn; either way every publisher of
// cur has returned (commit joins its lanes before it returns, runCursor
// forgets cur last, and a successor over the same labels starts only
// after that). Left in place, a stalled state would page from a node
// that no longer runs the partition, and a successor starting with
// nothing published would never delete it.
func (r *FanoutRunner) unpublishRemoteState(cur *remoteCursor) {
	rl := r.remoteMetrics()
	if rl == nil {
		return
	}
	cur.pubMu.Lock()
	defer cur.pubMu.Unlock()
	key := cur.key
	part := fanoutPartitionLabel(key.partition)
	cur.mu.Lock()
	published := cur.published
	cur.published = ""
	cur.mu.Unlock()
	if published != "" {
		rl.State.DeleteLabelValues(key.parent, key.child, part, published)
	}
	rl.LagSeconds.DeleteLabelValues(key.parent, key.child, part)
	rl.RetentionHeadroomSeconds.DeleteLabelValues(key.parent, key.child, part)
}

// commit ships one slab to the remote child's target. It returns nil
// once every record was accepted (or skipped); the records not shipped
// when the link dissolved or ctx ended; and reread=true when the held
// budget could not keep the records of a lane that had to wait, after
// waiting out what that lane was waiting for, so the cursor re-reads
// the slab from its unadvanced position.
//
// childVersion is the stub's version read before child was read, so a
// change applied in between makes the first lane pass re-read the stub.
func (s *remoteSender) commit(ctx context.Context, key fanoutCursorKey, child topic.Topic, childVersion uint64, records []topic.KeyedRecord) ([]topic.KeyedRecord, bool) {
	cur := s.r.remoteCursorOf(key)
	if cur == nil {
		// Only a cursor registered by runCursor ships; anything else is a
		// caller bug, and holding the records is always safe.
		return records, false
	}
	var slabStart int64
	if len(records) > 0 {
		slabStart = records[0].Offset
	}
	// What an earlier pass over this slab got onto the target is not
	// sent again.
	records = cur.unshipped(records)
	if len(records) == 0 {
		cur.clearProgress()
		cur.clearLanes()
		return nil, false
	}
	shipCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sh := &slabShip{s: s, cur: cur, key: key, cancel: cancel, link: child, linkVersion: childVersion, slabStart: slabStart}
	if parent, err := s.r.store.GetTopic(ctx, key.parent); err == nil {
		sh.retentionMs = parent.RetentionMs
	}
	lanes := child.Remote.EffectiveLanes()
	for i, recs := range sink.SplitLanes(records, lanes) {
		if len(recs) == 0 {
			continue
		}
		lane := &laneShip{idx: i, recs: recs, done: -1, cap: cur.chunkCap(i), backoff: sink.LaneBackoff(), b64: map[int64]bool{}}
		lane.oldestMs.Store(recs[0].CommittedAtUnixMs)
		sh.lanes = append(sh.lanes, lane)
	}
	var wg sync.WaitGroup
	for _, lane := range sh.lanes {
		wg.Go(func() { sh.runLane(shipCtx, lane) })
	}
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		sh.refreshWhileShipping(shipCtx)
	}()
	wg.Wait()
	cancel()
	<-refreshDone
	sh.releaseHeld()
	s.r.publishRemoteState(cur)

	var remaining []topic.KeyedRecord
	marks := map[int]int64{}
	for _, lane := range sh.lanes {
		remaining = append(remaining, lane.recs...)
		if lane.done >= 0 {
			marks[lane.idx] = lane.done
		}
	}
	if len(remaining) == 0 {
		cur.clearProgress()
		cur.clearLanes()
		return nil, false
	}
	cur.noteProgress(lanes, marks)
	if sh.reread.Load() && ctx.Err() == nil {
		if rl := s.r.remoteMetrics(); rl != nil {
			rl.RereadsTotal.WithLabelValues(key.parent, key.child).Inc()
		}
		s.r.logger.Error("remote child could not hold a waiting lane's records (remotes.max_held_bytes is full): it reads them again after the wait and sends only what the target does not have yet",
			"parent", key.parent, "partition", key.partition, "child", key.child,
			"remote", child.Remote.Name, "unsent_records", len(remaining))
		sh.waitBeforeReread(ctx)
		cur.clearLanes()
		return nil, true
	}
	return remaining, false
}

// slabShip is one slab on its way to the remote.
type slabShip struct {
	s           *remoteSender
	cur         *remoteCursor
	key         fanoutCursorKey
	cancel      context.CancelFunc
	retentionMs int64
	lanes       []*laneShip

	linkMu      sync.Mutex
	link        topic.Topic
	linkVersion uint64
	linkGone    bool

	slabStart int64

	reread atomic.Bool
	// rereadWait is what the lane that could not hold its records was
	// about to wait for: a duration, or -1 for the remote's gate.
	rereadWait atomic.Int64
	rereadGate atomic.Pointer[sink.Gate]
}

// laneShip is one lane of a slab.
type laneShip struct {
	idx  int
	recs []topic.KeyedRecord
	// done is the highest offset of this lane the target accepted or an
	// admin skipped in this slab, -1 for none yet.
	done    int64
	held    bool
	heldN   int64
	cap     *sink.ChunkCap
	backoff sink.Backoff
	builder sink.ChunkBuilder
	// plan holds the sizes of the next chunks while the lane narrows a
	// refusal down to one record (a "message N" answer, a 400 without an
	// index, a 413); empty means full chunks.
	plan []int
	// b64 marks records to send as base64: a raw record the target
	// refused is retried once that way.
	b64 map[int64]bool
	// blocked is the record the lane is stuck on.
	blocked *topic.RemoteBlock
	// oldestMs is the commit time of the lane's oldest unshipped record,
	// read by the lag refresher while the lane waits.
	oldestMs atomic.Int64
	// lookupState marks a lane state set by a failed remote lookup, and
	// retryStall a lane that waited out a cursor stall and tries once.
	lookupState bool
	retryStall  bool
}

// currentLink returns the stub as it stands, re-read only when its
// version moved; ok=false once the link dissolved or was replaced.
func (sh *slabShip) currentLink(ctx context.Context) (topic.Topic, bool) {
	sh.linkMu.Lock()
	defer sh.linkMu.Unlock()
	if sh.linkGone {
		return sh.link, false
	}
	version := sh.s.r.store.TopicVersion(sh.key.child)
	if version != sh.linkVersion {
		t, err := sh.s.r.store.GetTopic(ctx, sh.key.child)
		if err != nil || !t.IsRemoteChild() || t.Parent != sh.key.parent || t.AttachEpoch != sh.key.epoch {
			sh.linkGone = true
			return sh.link, false
		}
		sh.link, sh.linkVersion = t, version
	}
	return sh.link, true
}

// wait is waitChange on this slab's stub, refreshing the lag gauges.
func (sh *slabShip) wait(ctx context.Context, d time.Duration) bool {
	version := sh.s.r.store.TopicVersion(sh.key.child)
	tick := func() {
		oldest := sh.oldestUnshippedMs()
		sh.s.r.refreshRemoteLag(ctx, sh.key, sh.cur, sh.startOffset(), &oldest)
		sh.s.r.publishRemoteState(sh.cur)
	}
	tick()
	return sh.s.waitChange(ctx, sh.key.child, version, d, tick)
}

// refreshWhileShipping keeps the lag gauges live while the slab's lanes
// wait (on the gate, a backoff, a stall), until ctx ends.
func (sh *slabShip) refreshWhileShipping(ctx context.Context) {
	ticker := time.NewTicker(sh.s.lagRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			oldest := sh.oldestUnshippedMs()
			sh.s.r.refreshRemoteLag(ctx, sh.key, sh.cur, sh.startOffset(), &oldest)
			sh.s.r.publishRemoteState(sh.cur)
		}
	}
}

// startOffset is where the cursor stands: the slab's first offset (the
// cursor advances only past a whole slab).
func (sh *slabShip) startOffset() int64 { return sh.slabStart }

func (sh *slabShip) oldestUnshippedMs() int64 {
	oldest := int64(0)
	for _, lane := range sh.lanes {
		if ms := lane.oldestMs.Load(); ms > 0 && (oldest == 0 || ms < oldest) {
			oldest = ms
		}
	}
	return oldest
}

// hold keeps the lane's unsent records in the held budget before the
// lane waits. When the budget is full it asks for a re-read instead and
// stops every lane of the slab; wait is what the lane was about to wait
// for (-1: the gate).
func (sh *slabShip) hold(lane *laneShip, wait time.Duration, gate *sink.Gate) bool {
	if lane.held {
		return true
	}
	size := sink.HeldSize(lane.recs)
	if !sh.s.held.TryReserve(size) {
		sh.rereadWait.Store(int64(wait))
		if gate != nil {
			sh.rereadGate.Store(gate)
		}
		sh.reread.Store(true)
		sh.cancel()
		return false
	}
	lane.recs = sink.Hold(lane.recs)
	lane.held, lane.heldN = true, size
	return true
}

func (sh *slabShip) releaseHeld() {
	for _, lane := range sh.lanes {
		if lane.held {
			sh.s.held.Release(lane.heldN)
			lane.held, lane.heldN = false, 0
		}
		if rs := sh.s.remoteStateIfAny(sh.link.Remote.Name); rs != nil {
			rs.noteLaneCap(lane.cap, 0)
		}
	}
}

func (s *remoteSender) remoteStateIfAny(name string) *remoteState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remotes[name]
}

// waitBeforeReread waits out what the lane that gave up its records was
// waiting for, so a re-read does not spin against a remote that is down.
func (sh *slabShip) waitBeforeReread(ctx context.Context) {
	if gate := sh.rereadGate.Load(); gate != nil {
		_ = gate.WaitDue(ctx)
		return
	}
	if d := time.Duration(sh.rereadWait.Load()); d > 0 {
		sh.wait(ctx, d)
	}
}

// runLane ships one lane's records in order until they are all accepted
// or skipped, ctx ends, the link dissolves, or the slab must be re-read.
func (sh *slabShip) runLane(ctx context.Context, lane *laneShip) {
	s := sh.s
	for len(lane.recs) > 0 {
		if ctx.Err() != nil {
			return
		}
		stub, ok := sh.currentLink(ctx)
		if !ok {
			sh.cancel()
			return
		}
		link := stub.Remote
		if link.Paused {
			sh.cur.setLane(lane.idx, topic.RemoteStatePaused)
			if !sh.hold(lane, s.stallRetry, nil) || !sh.wait(ctx, s.stallRetry) {
				return
			}
			sh.cur.setLane(lane.idx, "")
			continue
		}
		if lane.blocked != nil {
			if link.Skipped(sh.key.partition, lane.recs[0].Offset) {
				sh.skip(lane)
				continue
			}
			if !sh.hold(lane, s.stallRetry, nil) || !sh.wait(ctx, s.stallRetry) {
				return
			}
			// Try the stuck record again, alone, as it went last time: a
			// fix on the target (a schema change) is picked up this way.
			lane.blocked = nil
			sh.cur.setBlocked(lane.idx, nil)
			lane.plan = append([]int{1}, lane.plan...)
			continue
		}
		// Wait on the gate first and look the entry up after: a lane that
		// waited out an auth failure must send with the entry a corrected
		// password built, not the one it held before the wait.
		rs := s.remoteState(link.Name)
		probe, err := rs.gate.Wait(ctx)
		if err != nil {
			return
		}
		release := func() {
			if probe {
				rs.gate.Released()
			}
		}
		entry, _, state := s.entry(link.Name)
		if state != "" {
			release()
			sh.cur.setLane(lane.idx, state)
			lane.lookupState = true
			if !sh.hold(lane, s.stallRetry, nil) || !sh.wait(ctx, s.stallRetry) {
				return
			}
			continue
		}
		if lane.lookupState {
			lane.lookupState = false
			sh.cur.setLane(lane.idx, "")
		}
		// A stall only a change or a retry fixes: wait, then try once
		// more; the answer sets or clears the stall.
		if stall := sh.cur.stallState(); stall != "" && !lane.retryStall {
			release()
			if !sh.hold(lane, s.stallRetry, nil) || !sh.wait(ctx, s.stallRetry) {
				return
			}
			lane.retryStall = true
			continue
		}
		lane.retryStall = false
		if ok, _, _ := s.checkTarget(ctx, sh.cur, entry, rs, stub); !ok {
			release()
			if ctx.Err() != nil {
				return
			}
			if !sh.hold(lane, s.stallRetry, nil) || !sh.wait(ctx, s.stallRetry) {
				return
			}
			// The lane waited (or an admin changed the link, say resumed
			// with accept_target): check again at once, without waiting
			// out the cursor's stall a second time.
			sh.cur.forceTargetCheck()
			lane.retryStall = true
			continue
		}
		sh.sendChunk(ctx, lane, entry, rs, link, probe)
	}
	sh.cur.setLane(lane.idx, "")
}

// checkTarget runs the runtime target check when it is due and reports
// whether sending may go on, the check's result and whether it ran. A
// check that errors never stops sending; one that answers stops it on a
// loop or a replaced target. checkMu keeps one check in flight per
// cursor; the verdict is published under mu once the check answered, so
// the listing never waits on the target.
func (s *remoteSender) checkTarget(ctx context.Context, c *remoteCursor, e *remote.Entry, rs *remoteState, stub topic.Topic) (ok bool, res sink.TargetResult, ran bool) {
	c.checkMu.Lock()
	defer c.checkMu.Unlock()
	now := time.Now()
	epoch := rs.gate.Epoch()
	due := c.forceCheck.Load() || c.checkedEntry != e || c.checkedGateEpoch != epoch ||
		c.checkedTargetID != stub.Remote.TargetID || !now.Before(c.nextCheck)
	if !due {
		return c.targetVerdict() == "", res, false
	}
	c.forceCheck.Store(false)
	cctx, cancel := context.WithTimeout(ctx, requestTimeout(e))
	res = sink.CheckTarget(cctx, e, stub.Remote.Topic, stub.Remote.TargetID, s.classify)
	cancel()
	if ctx.Err() != nil {
		c.forceCheck.Store(true)
		return false, res, false
	}
	c.checkedEntry, c.checkedGateEpoch, c.checkedTargetID = e, epoch, stub.Remote.TargetID
	c.nextCheck = now.Add(sink.CheckInterval(e.Limits().CheckIntervalMs))
	key := c.key
	if res.Verified {
		c.setTargetVerdict(res.State, now)
	} else if rl := s.r.remoteMetrics(); rl != nil {
		rl.CheckFailuresTotal.WithLabelValues(key.parent, key.child).Inc()
		rl.ErrorsTotal.WithLabelValues(rs.name, res.Class).Inc()
	}
	state := c.targetVerdict()
	if state != "" {
		s.r.logger.Warn("remote child stalled by its target check",
			"parent", key.parent, "partition", key.partition, "child", key.child,
			"remote", rs.name, "state", state)
	}
	return state == "", res, true
}

// remoteIdle keeps a quiet link honest, run by a remote cursor each
// time a read finds nothing to send. The lookup's state (remote_missing,
// credential_unreadable, node_insecure) shows without waiting for a
// record, and the runtime target check runs on its own schedule (at
// start, then every check interval with jitter) whether or not records
// wait: target_verified_at stays fresh on a healthy idle link, and a
// replaced target, a missing one or refused credentials show before
// anything is sent.
func (r *FanoutRunner) remoteIdle(ctx context.Context, key fanoutCursorKey, cur *remoteCursor) {
	stub, err := r.store.GetTopic(ctx, key.child)
	if err != nil || !stub.IsRemoteChild() || stub.Parent != key.parent || stub.AttachEpoch != key.epoch || stub.Remote.Paused {
		return
	}
	s := r.sender()
	e, rs, state := s.entry(stub.Remote.Name)
	cur.setIdleLookup(state)
	if state != "" {
		return
	}
	if _, res, ran := s.checkTarget(ctx, cur, e, rs, stub); ran && !res.Verified {
		cur.setIdleCheck(idleCheckState(res.Class))
	}
}

// idleCheckState is the link state a quiet cursor shows for a failed
// target check's class: the class itself when it is a link state, else
// unavailable (an edge answered, or the answer made no sense).
func idleCheckState(class string) string {
	if slices.Contains(topic.RemoteStateSeverity, class) {
		return class
	}
	return topic.RemoteStateUnavailable
}

// forceTargetCheck makes the next chunk re-run the target check. It
// never waits on a check in flight.
func (c *remoteCursor) forceTargetCheck() {
	c.forceCheck.Store(true)
}

// chunkCap is lane i's adaptive chunk cap, kept across slabs.
func (c *remoteCursor) chunkCap(i int) *sink.ChunkCap {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.caps == nil {
		c.caps = map[int]*sink.ChunkCap{}
	}
	cc := c.caps[i]
	if cc == nil {
		v := sink.NewChunkCap()
		cc = &v
		c.caps[i] = cc
	}
	return cc
}

// sendChunk sends the lane's next chunk and acts on the answer.
func (sh *slabShip) sendChunk(ctx context.Context, lane *laneShip, e *remote.Entry, rs *remoteState, link *topic.RemoteLink, probe bool) {
	s := sh.s
	rl := s.r.remoteMetrics()
	waited, err := rs.sem.Acquire(ctx)
	if rl != nil {
		rl.InflightWaitSeconds.WithLabelValues(rs.name).Observe(waited.Seconds())
	}
	if err != nil {
		if probe {
			rs.gate.Released()
		}
		return
	}
	if rs.superseded(e) {
		// A password rotation (or a new remote under the name) arrived
		// while the lane waited for a slot: send nothing with the old
		// credential; the lane looks the entry up again.
		rs.sem.Release()
		if probe {
			rs.gate.Released()
		}
		return
	}
	caps := s.capabilities(ctx, rs, e, link.Topic)
	maxMessages := caps.MaxMessages
	if len(lane.plan) > 0 {
		maxMessages = min(maxMessages, lane.plan[0])
	}
	body, n := lane.builder.Build(lane.recs, maxMessages, lane.cap.Bytes(), func(i int) bool { return lane.b64[lane.recs[i].Offset] })
	wire, compressed := body, false
	if e.Limits().Compression == "zstd" && caps.Zstd {
		wire, compressed = sink.MaybeCompress(body)
	}
	encoding := ""
	if compressed {
		encoding = "zstd"
	}
	path, err := remote.TopicPath(link.Topic, "produce", "batch")
	if err != nil {
		rs.sem.Release()
		if probe {
			rs.gate.Released()
		}
		sh.cur.setStall(topic.RemoteStateTargetMissing)
		return
	}
	rctx, cancel := context.WithTimeout(ctx, requestTimeout(e))
	resp, err := e.Do(rctx, remote.Outbound{Method: "POST", Path: path, Body: wire, ContentType: "application/json", ContentEncoding: encoding})
	var answer []byte
	if err == nil {
		answer, _ = remote.ReadBody(resp, remote.MaxProduceAnswerBytes)
	}
	cancel()
	rs.sem.Release()
	if rl != nil {
		rl.BodyBytesTotal.WithLabelValues(rs.name).Add(float64(len(body)))
		rl.WireBytesTotal.WithLabelValues(rs.name).Add(float64(len(wire)))
	}
	if err != nil && ctx.Err() != nil {
		// Shutting down, detaching or re-reading: not the remote's fault.
		if probe {
			rs.gate.Released()
		}
		return
	}
	v := s.classify.Classify(sink.Answer{Resp: resp, Body: answer, Err: err, Chunk: n, Compressed: compressed})
	if v.Action == sink.ActResolveRoute {
		lctx, lcancel := context.WithTimeout(ctx, requestTimeout(e))
		lresp, lbody, lerr := sink.FetchListing(lctx, e, link.Topic)
		lcancel()
		v = sink.ResolveRoute(lresp, lbody, lerr)
	}
	if v.Action != sink.ActCommitted && rl != nil {
		rl.ErrorsTotal.WithLabelValues(rs.name, v.Class).Inc()
	}
	sh.act(ctx, lane, rs, e, v, n, probe)
	if rl != nil {
		rl.ChunkBytesLimit.WithLabelValues(rs.name).Set(float64(rs.noteLaneCap(lane.cap, lane.cap.Bytes())))
	}
	s.r.publishRemoteState(sh.cur)
}

// act applies a classified answer to the lane. Every answer that
// refuses or narrows down records came from the target topic through the
// replicator's grant, so it proves a cursor stall (target_missing,
// forbidden, ...) no longer holds and clears it, as a commit does.
func (sh *slabShip) act(ctx context.Context, lane *laneShip, rs *remoteState, e *remote.Entry, v sink.Verdict, n int, probe bool) {
	s := sh.s
	switch v.Action {
	case sink.ActCommitted:
		rs.gate.Succeeded(probe)
		lane.cap.Accepted()
		lane.backoff.Reset()
		if n > 0 {
			lane.done = lane.recs[n-1].Offset
		}
		lane.recs = lane.recs[n:]
		lane.consumePlan(n)
		if len(lane.recs) > 0 {
			lane.oldestMs.Store(lane.recs[0].CommittedAtUnixMs)
		} else {
			lane.oldestMs.Store(0)
		}
		sh.cur.succeeded(time.Now())
		sh.cur.setLane(lane.idx, "")
		sh.cur.setStall("")
		sh.cur.clearIdle()

	case sink.ActResendPrefix:
		rs.gate.Succeeded(probe)
		sh.cur.setStall("")
		i := v.Index
		switch {
		case i >= n:
			sh.narrow(lane, n, n/2, n-n/2)
		case n == 1:
			sh.refuseOne(lane, topic.RemoteStateRejectedRecord)
		default:
			sh.narrow(lane, n, i, 1, n-i-1)
		}

	case sink.ActBisect:
		rs.gate.Succeeded(probe)
		sh.cur.setStall("")
		if n == 1 {
			sh.refuseOne(lane, topic.RemoteStateRejectedRecord)
		} else {
			sh.narrow(lane, n, n/2, n-n/2)
		}

	case sink.ActBlock:
		rs.gate.Succeeded(probe)
		sh.cur.setStall("")
		sh.block(lane, topic.RemoteStateRejectedRecord)
		sh.cur.forceTargetCheck()

	case sink.ActSplitTooLarge:
		rs.gate.Succeeded(probe)
		sh.cur.setStall("")
		switch {
		case n == 1:
			sh.block(lane, topic.RemoteStateRecordTooLarge)
		case v.Index > 0 && v.Index < n:
			sh.narrow(lane, n, v.Index, 1, n-v.Index-1)
		case v.Index == 0:
			sh.narrow(lane, n, 1, n-1)
		default:
			sh.narrow(lane, n, n/2, n-n/2)
		}

	case sink.ActRetry:
		laneBackoff := rs.gate.Failed(v, probe)
		if v.Shrink {
			lane.cap.Shrink()
		}
		if v.Ambiguous {
			if rl := s.r.remoteMetrics(); rl != nil {
				rl.ResentRecordsTotal.WithLabelValues(rs.name).Add(float64(n))
			}
		}
		sh.cur.setLane(lane.idx, v.State)
		if laneBackoff {
			d := lane.backoff.Next()
			if sh.hold(lane, d, nil) {
				sleepCtx(ctx, d)
			}
			return
		}
		sh.hold(lane, -1, rs.gate)

	case sink.ActGate:
		if rs.superseded(e) {
			// The answer is to a credential the cache has since replaced
			// (a 401 to the old password): it must not close the gate the
			// new credential reopened. The lane retries with the new one.
			if probe {
				rs.gate.Released()
			}
			return
		}
		rs.gate.Failed(v, probe)
		sh.cur.setLane(lane.idx, v.State)
		sh.hold(lane, -1, rs.gate)

	case sink.ActStall:
		if v.State == topic.RemoteStateDestinationRefused {
			// Nothing was sent: the dial was refused on this side.
			if probe {
				rs.gate.Released()
			}
		} else {
			rs.gate.Succeeded(probe)
		}
		sh.cur.setStall(v.State)
		s.r.logger.Warn("remote child stalled", "parent", sh.key.parent, "partition", sh.key.partition,
			"child", sh.key.child, "remote", rs.name, "state", v.State, "status", v.Status)
		// The retry re-runs the target check before it sends: a target
		// deleted and recreated meanwhile turns into target_replaced,
		// never a send into the new topic without accept_target.
		sh.cur.forceTargetCheck()
		sh.hold(lane, s.stallRetry, nil)

	case sink.ActUncompressed:
		if probe {
			rs.gate.Released()
		}
		rs.disableZstd(e)
	}
}

// narrow replaces the chunk of n records just refused with the sizes
// given (which sum to n), so the lane narrows the refusal down to one
// record while shipping the others in order.
func (sh *slabShip) narrow(lane *laneShip, n int, sizes ...int) {
	lane.consumePlan(n)
	next := make([]int, 0, len(sizes)+len(lane.plan))
	for _, sz := range sizes {
		if sz > 0 {
			next = append(next, sz)
		}
	}
	lane.plan = append(next, lane.plan...)
}

// consumePlan removes n records' worth from the front of the plan.
func (lane *laneShip) consumePlan(n int) {
	for n > 0 && len(lane.plan) > 0 {
		take := min(n, lane.plan[0])
		lane.plan[0] -= take
		n -= take
		if lane.plan[0] == 0 {
			lane.plan = lane.plan[1:]
		}
	}
}

// refuseOne handles the target refusing the lane's front record on its
// own: a record that went raw is retried once as base64, then the lane
// blocks on it.
func (sh *slabShip) refuseOne(lane *laneShip, state string) {
	rec := lane.recs[0]
	if state == topic.RemoteStateRejectedRecord && sink.SentRaw(rec) && !lane.b64[rec.Offset] {
		lane.b64[rec.Offset] = true
		sh.narrow(lane, 1, 1)
		return
	}
	sh.block(lane, state)
}

// block stops the lane on its front record until an admin skips it, the
// target changes its mind (retried every stall interval), or the link
// changes.
func (sh *slabShip) block(lane *laneShip, state string) {
	rec := lane.recs[0]
	lane.consumePlan(1)
	lane.plan = append([]int{1}, lane.plan...)
	lane.blocked = &topic.RemoteBlock{Partition: sh.key.partition, Offset: rec.Offset, State: state}
	sh.cur.setBlocked(lane.idx, lane.blocked)
	sh.cur.setLane(lane.idx, state)
	sh.s.r.logger.Warn("remote child blocked on a record the target refuses",
		"parent", sh.key.parent, "partition", sh.key.partition, "offset", rec.Offset,
		"child", sh.key.child, "state", state)
}

// skip drops the lane's blocked front record, which an admin skipped.
func (sh *slabShip) skip(lane *laneShip) {
	rec := lane.recs[0]
	lane.done = rec.Offset
	lane.recs = lane.recs[1:]
	lane.consumePlan(1)
	lane.blocked = nil
	sh.cur.setBlocked(lane.idx, nil)
	sh.cur.setLane(lane.idx, "")
	if len(lane.recs) > 0 {
		lane.oldestMs.Store(lane.recs[0].CommittedAtUnixMs)
	} else {
		lane.oldestMs.Store(0)
	}
	if rl := sh.s.r.remoteMetrics(); rl != nil {
		rl.SkippedRecordsTotal.WithLabelValues(sh.key.parent, sh.key.child).Inc()
	}
	sh.s.r.logger.Warn("remote child skipped a record an admin skipped",
		"parent", sh.key.parent, "partition", sh.key.partition, "offset", rec.Offset, "child", sh.key.child)
}
