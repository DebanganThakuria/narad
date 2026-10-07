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
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"weak"

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
	// checks holds each link's shared runtime target check.
	checks map[linkCheckKey]*linkCheck
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

	mu sync.Mutex
	// seenID and seenCV identify the last entry a lookup returned
	// (seen: one has). The identity, not the entry: an entry pins its
	// Authorization header and its clients, which a deleted or
	// unreadable remote must let go of.
	seen        bool
	seenID      string
	seenCV      uint64
	caps        sink.Capabilities
	capsKnown   bool
	capsID      string
	capsCV      uint64
	capsRetryAt time.Time
	// capsAt is when caps were probed; they are probed again after a
	// check interval.
	capsAt   time.Time
	probing  bool
	laneCaps map[*sink.ChunkCap]int
	// gateState is the state of the last failure reported to the gate,
	// which quiet cursors show while it stays closed.
	gateState string
}

// gateFailed reports a failure to the remote's gate (Gate.Failed) and
// remembers its state.
func (rs *remoteState) gateFailed(v sink.Verdict, probe bool) (laneBackoff bool) {
	rs.mu.Lock()
	rs.gateState = v.State
	rs.mu.Unlock()
	return rs.gate.Failed(v, probe)
}

// closedState is why the gate is closed, "" while it is open.
func (rs *remoteState) closedState() string {
	if !rs.gate.Closed() {
		return ""
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.gateState
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
		if rl := s.r.remoteMetrics(); rl != nil {
			backoff := rl.GateBackoffSeconds.WithLabelValues(name)
			rs.gate.ObserveBackoff(func(d time.Duration) { backoff.Set(d.Seconds()) })
		}
		s.remotes[name] = rs
	}
	return rs
}

// forgetRemote drops a remote the registry no longer holds: this node's
// pacing state for it (its gate, its in-flight slots and its probed
// capabilities) and its per-remote series. A link that
// still names it builds fresh state on its next lookup and holds in
// remote_missing.
func (s *remoteSender) forgetRemote(rs *remoteState) {
	s.mu.Lock()
	if s.remotes[rs.name] != rs {
		s.mu.Unlock()
		return
	}
	delete(s.remotes, rs.name)
	s.mu.Unlock()
	if rl := s.r.remoteMetrics(); rl != nil {
		rl.PruneRemote(rs.name)
	}
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
			e, err := s.lookup.Get(rs.name)
			switch {
			case err == nil:
				rs.observe(e)
			case errors.Is(err, remote.ErrRemoteMissing):
				s.forgetRemote(rs)
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
	changed := rs.seen && (rs.seenID != e.RemoteID() || rs.seenCV != e.CredentialVersion())
	rs.seen, rs.seenID, rs.seenCV = true, e.RemoteID(), e.CredentialVersion()
	rs.mu.Unlock()
	if changed {
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
	defer rs.mu.Unlock()
	return rs.seen && (rs.seenID != e.RemoteID() || rs.seenCV > e.CredentialVersion())
}

// stillCurrent reports whether e is still what the lookup holds for the
// remote and rs still its pacing state: one atomic load and one map
// lookup, so a chunk that waited for a slot never goes out with the
// credential of a remote deleted (or deleted and created again)
// meanwhile.
func (s *remoteSender) stillCurrent(rs *remoteState, e *remote.Entry) bool {
	if s.lookup == nil {
		return false
	}
	if cur, err := s.lookup.Get(rs.name); err != nil || cur != e {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remotes[rs.name] == rs
}

// capabilities is what the remote's batch produce takes, probed once
// per remote per node and credential version, and again every check
// interval so a target upgraded (or rolled back) behind the same remote
// is picked up. While a first probe runs, or after one proved nothing or
// the target refused a chunk's message count, the conservative defaults
// apply; while a refresh runs, the caps it refreshes do.
func (s *remoteSender) capabilities(ctx context.Context, rs *remoteState, e *remote.Entry, topicName string) sink.Capabilities {
	rs.mu.Lock()
	now := time.Now()
	known := rs.capsKnown && rs.capsID == e.RemoteID() && rs.capsCV == e.CredentialVersion()
	ttl := time.Duration(e.Limits().CheckIntervalMs) * time.Millisecond
	if known && (rs.probing || now.Sub(rs.capsAt) < ttl) {
		caps := rs.caps
		rs.mu.Unlock()
		return caps
	}
	if rs.probing || now.Before(rs.capsRetryAt) {
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
	rs.caps, rs.capsKnown, rs.capsID, rs.capsCV, rs.capsAt = caps, true, e.RemoteID(), e.CredentialVersion(), time.Now()
	return caps
}

// forgetCaps drops the probed capabilities after the target refused a
// chunk's message count: the defaults apply until the next probe, one
// check interval later, so a target whose pods answer differently
// (mid-roll behind a load balancer) is not probed in a loop.
func (rs *remoteState) forgetCaps(e *remote.Entry) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.capsKnown = false
	rs.caps = sink.DefaultCapabilities()
	rs.capsRetryAt = time.Now().Add(time.Duration(e.Limits().CheckIntervalMs) * time.Millisecond)
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
	s := r.sender()
	var releaseCheck func()
	c.check, releaseCheck = s.linkCheck(key)
	r.remoteMu.Lock()
	if r.remoteCursors == nil {
		r.remoteCursors = map[fanoutCursorKey]*remoteCursor{}
	}
	r.remoteCursors[key] = c
	r.remoteMu.Unlock()
	r.holdLinkSuccess(key)
	return c, func() {
		r.remoteMu.Lock()
		if r.remoteCursors[key] == c {
			delete(r.remoteCursors, key)
		}
		r.remoteMu.Unlock()
		releaseCheck()
		r.unpublishRemoteState(c)
		r.releaseLinkSuccess(key)
	}
}

// remoteLinkLabels are a link's series labels.
type remoteLinkLabels struct{ parent, child string }

// remoteLinkSuccess is the newest success among this node's running
// cursors of one link, and how many of them run.
type remoteLinkSuccess struct {
	refs     int
	newestMs int64
}

func (r *FanoutRunner) holdLinkSuccess(key fanoutCursorKey) {
	k := remoteLinkLabels{key.parent, key.child}
	r.remoteSuccessMu.Lock()
	defer r.remoteSuccessMu.Unlock()
	if r.remoteSuccess == nil {
		r.remoteSuccess = map[remoteLinkLabels]*remoteLinkSuccess{}
	}
	ls := r.remoteSuccess[k]
	if ls == nil {
		ls = &remoteLinkSuccess{}
		r.remoteSuccess[k] = ls
	}
	ls.refs++
}

// releaseLinkSuccess lets go of a stopped cursor's share; once the
// node's last cursor of the link stopped, the link's last-success
// series goes too, so a node the link's partitions moved off exports
// no frozen timestamp.
func (r *FanoutRunner) releaseLinkSuccess(key fanoutCursorKey) {
	k := remoteLinkLabels{key.parent, key.child}
	r.remoteSuccessMu.Lock()
	defer r.remoteSuccessMu.Unlock()
	ls := r.remoteSuccess[k]
	if ls == nil {
		return
	}
	if ls.refs--; ls.refs > 0 {
		return
	}
	delete(r.remoteSuccess, k)
	if rl := r.remoteMetrics(); rl != nil {
		rl.LastSuccessTimestampSeconds.DeleteLabelValues(key.parent, key.child)
	}
}

// noteLinkSuccess sets the link's last-success gauge when ms is newer
// than every success of the node's cursors of the link so far: a quiet
// partition re-publishing an old success never pulls it back.
func (r *FanoutRunner) noteLinkSuccess(rl *metrics.RemoteLinkMetrics, key fanoutCursorKey, ms int64) {
	if ms <= 0 {
		return
	}
	r.remoteSuccessMu.Lock()
	defer r.remoteSuccessMu.Unlock()
	ls := r.remoteSuccess[remoteLinkLabels{key.parent, key.child}]
	if ls == nil || ms <= ls.newestMs {
		return
	}
	ls.newestMs = ms
	rl.LastSuccessTimestampSeconds.WithLabelValues(key.parent, key.child).Set(float64(ms) / 1000)
}

// linkCheck returns the runtime target check of key's link on this
// node, shared by the link's registered cursors, and the function that
// lets go of it (the last one forgets it).
func (s *remoteSender) linkCheck(key fanoutCursorKey) (*linkCheck, func()) {
	k := linkCheckKey{child: key.child, epoch: key.epoch}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checks == nil {
		s.checks = map[linkCheckKey]*linkCheck{}
	}
	lc := s.checks[k]
	if lc == nil {
		lc = &linkCheck{sinceMs: time.Now().UnixMilli()}
		s.checks[k] = lc
	}
	lc.refs++
	return lc, sync.OnceFunc(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if lc.refs--; lc.refs == 0 && s.checks[k] == lc {
			delete(s.checks, k)
		}
	})
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
		cur.setRemote(stub.Remote.Name)
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
	cur.pubMu.Lock()
	defer cur.pubMu.Unlock()
	snap := cur.snapshot()
	r.logRemoteTransition(cur, snap.state)
	rl := r.remoteMetrics()
	if rl == nil {
		return
	}
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
	r.noteLinkSuccess(rl, key, snap.lastSuccessMs)
}

// remoteStallLogGap is how often one cursor may log entering a stall
// that clears on its own (unavailable, throttled): a lone reset or
// timeout is routine on a WAN, and a flapping link must not flood the
// log. A stall that needs a fix is logged on every entry.
const remoteStallLogGap = time.Minute

// logRemoteTransition writes one line when the cursor enters a stall
// (whatever stalled it: an answer, a lookup, a target check, a refused
// record), at error level when only a person's fix clears it, and one
// line when it runs again. A retry that meets the same stall logs
// nothing. Called under cur.pubMu.
func (r *FanoutRunner) logRemoteTransition(cur *remoteCursor, state string) {
	key := cur.key
	switch state {
	case topic.RemoteStateRunning:
		if cur.loggedState != "" {
			r.logger.Info("remote child running again",
				"parent", key.parent, "partition", key.partition, "child", key.child, "after", cur.loggedState)
			cur.loggedState = ""
		}
		return
	case topic.RemoteStatePaused, cur.loggedState:
		return
	}
	needsFix := topic.RemoteStateNeedsFix(state)
	now := time.Now()
	if !needsFix && now.Sub(cur.loggedAt) < remoteStallLogGap {
		return
	}
	cur.loggedState, cur.loggedAt = state, now
	level := slog.LevelWarn
	if needsFix {
		level = slog.LevelError
	}
	remoteName, status := cur.logContext(state)
	attrs := []any{"parent", key.parent, "partition", key.partition, "child", key.child, "remote", remoteName, "state", state}
	if status > 0 {
		attrs = append(attrs, "status", status)
	}
	r.logger.Log(context.Background(), level, "remote child stalled", attrs...)
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
	// shipCtx ends the slab's requests (the link dissolved, or the slab
	// is done); waitCtx, below it, ends only the lanes' waits, when one
	// lane cannot hold its records and the slab must be read again. A
	// request in flight then still gets its answer, so what the target
	// accepted is recorded and not sent again.
	shipCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	waitCtx, stopWaits := context.WithCancel(shipCtx)
	defer stopWaits()
	sh := &slabShip{
		s: s, cur: cur, key: key, cancel: cancel, stopWaits: stopWaits, sendCtx: shipCtx,
		link: child, linkVersion: childVersion, slabStart: slabStart,
	}
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
		wg.Go(func() { sh.runLane(waitCtx, lane) })
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
	if sh.paused.Load() && ctx.Err() == nil {
		// A planned pause, not a budget shortfall: no re-read is counted
		// and nothing is logged. remoteBeforeRead parks the cursor with
		// nothing held, and the progress marks keep what the target
		// accepted from being sent again after the resume.
		cur.clearLanes()
		return nil, true
	}
	if sh.reread.Load() && ctx.Err() == nil {
		if rl := s.r.remoteMetrics(); rl != nil {
			rl.RereadsTotal.WithLabelValues(key.parent, key.child).Inc()
		}
		// Logged once per remoteStallLogGap per cursor: a long outage
		// re-reads after every gate backoff, and the counter has each.
		if cur.rereadLogDue(time.Now(), remoteStallLogGap) {
			s.r.logger.Error("remote child could not hold a waiting lane's records (remotes.max_held_bytes is full): it reads them again after the wait and sends only what the target does not have yet",
				"parent", key.parent, "partition", key.partition, "child", key.child,
				"remote", child.Remote.Name, "unsent_records", len(remaining))
		}
		sh.waitBeforeReread(ctx)
		cur.clearLanes()
		return nil, true
	}
	return remaining, false
}

// slabShip is one slab on its way to the remote.
type slabShip struct {
	s      *remoteSender
	cur    *remoteCursor
	key    fanoutCursorKey
	cancel context.CancelFunc
	// stopWaits ends every lane's waits (not its requests in flight);
	// sendCtx is what requests run under.
	stopWaits   context.CancelFunc
	sendCtx     context.Context
	retentionMs int64
	lanes       []*laneShip

	linkMu      sync.Mutex
	link        topic.Topic
	linkVersion uint64
	linkGone    bool

	slabStart int64

	reread atomic.Bool
	// paused marks a slab ended because its link was paused: the cursor
	// reads it again after the resume, holding nothing meanwhile.
	paused atomic.Bool
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
	// retryStall a lane whose stall wait a change woke early, so it
	// tries once at once.
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

// endForPause ends the slab because its link is paused: every lane's
// waits end (a request in flight still gets its answer) and commit
// returns it for a re-read, so no lane holds records across the pause.
func (sh *slabShip) endForPause() {
	sh.paused.Store(true)
	sh.stopWaits()
}

// refreshWhileShipping keeps the lag gauges live while the slab's lanes
// wait (on the gate, a backoff, a stall), until ctx ends. It also
// watches the stub, so a pause or a detach reaches lanes parked on the
// gate or a slot at once instead of when they next wake.
func (sh *slabShip) refreshWhileShipping(ctx context.Context) {
	ticker := time.NewTicker(sh.s.lagRefresh)
	defer ticker.Stop()
	poll := time.NewTicker(sh.s.changePoll)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			stub, ok := sh.currentLink(ctx)
			switch {
			case !ok:
				sh.cancel()
				return
			case stub.Remote.Paused:
				sh.endForPause()
				return
			}
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
// ends every lane's waits (a request in flight still gets its answer,
// so what the target accepts is recorded); wait is what the lane was
// about to wait for (-1: the gate).
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
		sh.stopWaits()
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
		start := time.Now()
		_ = gate.WaitDue(ctx)
		// A probe given back without an answer leaves the gate due at
		// once: wait at least its shortest backoff, so the re-read never
		// spins against a closed gate.
		if gate.Closed() {
			if d := sink.GateMinBackoff - time.Since(start); d > 0 {
				sleepCtx(ctx, d)
			}
		}
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
			// Hold nothing across a pause: end the slab.
			sh.endForPause()
			return
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
		// A lane about to wait on a closed gate keeps its records in the
		// held budget first, so an outage never pins parent log frames
		// outside max_held_bytes; a full budget asks for a re-read. A
		// lane whose sibling in this slab holds the probe waits for its
		// answer with the records in hand instead (one request, as the
		// prober's own records wait): a re-read would end the probe
		// before the target answered it, and the gate would never open.
		if wait, siblingProbe := rs.gate.WouldWaitFor(sh); wait {
			if siblingProbe {
				if rs.gate.AwaitProbe(ctx, sh) != nil {
					return
				}
				continue
			}
			if !sh.hold(lane, -1, rs.gate) {
				return
			}
		}
		probe, err := rs.gate.WaitAs(ctx, sh)
		if err != nil {
			return
		}
		gateEpoch := rs.gate.Epoch()
		release := func() {
			if probe {
				rs.gate.Released()
			}
		}
		// The wait may have been long: a pause or a detach applied
		// meanwhile wins over sending.
		if stub, ok = sh.currentLink(ctx); !ok || stub.Remote.Paused {
			release()
			continue
		}
		link = stub.Remote
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
		// A stall only a change or a retry fixes: wait until its retry is
		// due (a change wakes the wait early), then try once more; the
		// answer sets or clears the stall. The due time is the cursor's,
		// so a slab read again because the budget could not hold it
		// sends the retry once the re-read's wait is over.
		if stall, due := sh.cur.stallWait(); stall != "" && due > 0 && !lane.retryStall {
			release()
			if !sh.hold(lane, due, nil) || !sh.wait(ctx, due) {
				return
			}
			lane.retryStall = true
			continue
		}
		lane.retryStall = false
		// The check is a request like a chunk: it runs under sendCtx, so
		// a sibling's re-read never cuts it off before the target
		// answers and its answer is always recorded.
		ok, res, ran := s.checkTarget(sh.sendCtx, ctx, sh.cur, entry, rs, stub)
		if v, outcome := res.GateVerdict(); ran && probe && outcome == sink.CheckFailed && v.Action == sink.ActGate {
			// The check went out as the gate's probe and the remote
			// refused it as a whole (a wrong password, a throttle, a
			// TLS failure): that is the probe's answer, as for a
			// quiet cursor. Sending the chunk behind it would cost a
			// wrong password two failed logins per backoff. The lane
			// holds its records and waits for the next probe, which
			// checks again first. A transient failure (unavailable,
			// an edge) says nothing about produce: the chunk goes
			// out behind it, so a target that takes chunks while its
			// listing fails is not held for as long as the listing
			// fails. Its key is still owed a check, so once the
			// chunk reopens the gate the next lane checks first.
			if rs.superseded(entry) {
				// The answer is to a credential since replaced: the
				// lane checks again with the new one.
				rs.gate.Released()
			} else {
				rs.gateFailed(v, true)
				sh.cur.setLane(lane.idx, v.State)
			}
			continue
		}
		if !ok {
			release()
			if ctx.Err() != nil {
				return
			}
			if !sh.hold(lane, s.stallRetry, nil) {
				// The re-read waits stallRetry first; its pass checks again.
				sh.cur.forceTargetCheck()
				return
			}
			if !sh.wait(ctx, s.stallRetry) {
				return
			}
			// The lane waited (or an admin changed the link, say resumed
			// with accept_target): check again at once, without waiting
			// out the cursor's stall a second time.
			sh.cur.forceTargetCheck()
			lane.retryStall = true
			continue
		}
		sh.sendChunk(ctx, lane, entry, rs, link, probe, gateEpoch)
	}
	sh.cur.setLane(lane.idx, "")
	// Every record is on the target or skipped: give the held bytes back
	// now, not when the slab ends, which a sibling blocked on a refused
	// record can put off for days.
	// Dropping the emptied slice lets the held copy go too.
	lane.recs = nil
	if lane.held {
		s.held.Release(lane.heldN)
		lane.held, lane.heldN = false, 0
	}
}

// checkTarget runs the link's runtime target check when it is due and
// reports whether sending may go on, the check's result and whether it
// ran. A check that errors never stops sending, but neither does it
// count as the check that was due: the next send asks again (beside an
// open gate, once a short retry has passed). One that answers stops
// sending on a loop or a replaced target. The schedule and the verdict are
// the link's, shared by its cursors on this node, and one check is in
// flight per link. lc.mu is never held across the request: a cursor
// that finds the check not due, or merely due on its interval or as the
// retry of an errored check (beside an open gate) while another
// cursor's check is in flight, reads the published verdict at once. One
// that needs a fresh verdict (a forced check, a new entry,
// a reopened gate, a new recorded target) waits for the check in
// flight, then runs its own if that one did not cover it; it waits no
// longer than wait lives. The request itself runs under ctx.
func (s *remoteSender) checkTarget(ctx, wait context.Context, c *remoteCursor, e *remote.Entry, rs *remoteState, stub topic.Topic) (ok bool, res sink.TargetResult, ran bool) {
	lc := c.check
	var waited time.Time
	for {
		lc.mu.Lock()
		now := time.Now()
		key := checkKey{entry: weak.Make(e), gateEpoch: rs.gate.Epoch(), targetID: stub.Remote.TargetID}
		force := lc.forceCheck.Load()
		fresh := force || lc.checked != key
		due := fresh || !now.Before(lc.nextCheck)
		if due && !force && lc.erred == key && (now.Before(lc.retryAt) || (!waited.IsZero() && !lc.erredAt.Before(waited))) {
			// A check for the same key errored a moment ago beside an
			// open gate, or while this caller waited for it: go on the
			// last verdict rather than ask again at once.
			fresh, due = false, false
		}
		if !due {
			lc.mu.Unlock()
			return c.targetVerdict() == "", res, false
		}
		if done := lc.inflight; done != nil {
			// Beside an open gate, a check in flight for a key that
			// already erred is the link's retry: a lane that finds it
			// sends on the last verdict, as it did before the retry
			// fell due, instead of waiting out a target that keeps
			// failing its listing. While the gate is closed the probe
			// that reopens it is still checked first.
			retry := !force && lc.erred == key && !rs.gate.Closed()
			lc.mu.Unlock()
			if !fresh || retry {
				return c.targetVerdict() == "", res, false
			}
			if waited.IsZero() {
				waited = now
			}
			select {
			case <-done:
				continue
			case <-ctx.Done():
			case <-wait.Done():
			}
			return false, res, false
		}
		done := make(chan struct{})
		lc.inflight = done
		lc.forceCheck.Store(false)
		lc.mu.Unlock()

		cctx, cancel := context.WithTimeout(ctx, requestTimeout(e))
		res = sink.CheckTarget(cctx, e, stub.Remote.Topic, stub.Remote.TargetID, s.classify)
		cancel()

		lc.mu.Lock()
		lc.inflight = nil
		close(done)
		if ctx.Err() != nil {
			lc.forceCheck.Store(true)
			lc.mu.Unlock()
			return false, res, false
		}
		interval := sink.CheckInterval(e.Limits().CheckIntervalMs)
		if _, outcome := res.GateVerdict(); outcome == sink.CheckReached {
			lc.checked, lc.nextCheck = key, now.Add(interval)
		} else {
			// The target was not reached, so the check covers nothing:
			// the next send after a pause asks again. While the gate is
			// closed that is the next probe (paced by the gate), so the
			// probe that reopens it is checked first; beside an open
			// gate the link's other sends wait a while before asking.
			lc.erred, lc.erredAt, lc.retryAt = key, time.Now(), time.Time{}
			if !rs.gate.Closed() {
				lc.retryAt = lc.erredAt.Add(min(interval, sink.GateMaxBackoff))
			}
		}
		lc.record(res, now)
		lc.mu.Unlock()
		break
	}
	ck := c.key
	if res.Verified {
		c.setIdleCheck("")
	} else if rl := s.r.remoteMetrics(); rl != nil {
		rl.CheckFailuresTotal.WithLabelValues(ck.parent, ck.child).Inc()
		rl.ErrorsTotal.WithLabelValues(rs.name, res.Class).Inc()
	}
	return c.targetVerdict() == "", res, true
}

// remoteIdle keeps a quiet link honest, run by a remote cursor each
// time a read finds nothing to send. The lookup's state (remote_missing,
// credential_unreadable, node_insecure) shows without waiting for a
// record, and the runtime target check runs on its own schedule (at
// start, then every check interval with jitter) whether or not records
// wait: target_verified_at stays fresh on a healthy idle link, and a
// replaced target, a missing one or refused credentials show before
// anything is sent.
//
// The check is a credentialed request like a chunk, so it goes through
// the remote's gate and its outcome paces the gate: while the gate is
// closed only its due probe goes out, from any cursor, so a wrong
// password costs the target one failed login per node per backoff and a
// dead target one request, however many quiet cursors the node runs. A
// cursor the gate keeps quiet shows why the gate closed.
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
	probe, ok := rs.gate.TryWait()
	if !ok {
		if closed := rs.closedState(); closed != "" {
			cur.setIdleCheck(idleCheckState(closed))
		}
		return
	}
	_, res, ran := s.checkTarget(ctx, ctx, cur, e, rs, stub)
	if !ran {
		if probe {
			rs.gate.Released()
		}
		// Not due: another cursor of the link checked; show what the
		// gate or that check found.
		switch closed, class := rs.closedState(), cur.check.lastFailure(); {
		case closed != "":
			cur.setIdleCheck(idleCheckState(closed))
		case class != "":
			cur.setIdleCheck(idleCheckState(class))
		default:
			cur.setIdleCheck("")
		}
		return
	}
	switch v, outcome := res.GateVerdict(); {
	case outcome == sink.CheckReached:
		rs.gate.Succeeded(probe)
	case outcome == sink.CheckFailed && (probe || v.Action == sink.ActGate) && !rs.superseded(e):
		// A remote-wide refusal (a wrong password, a throttle) closes
		// the gate whoever asked. A transient failure (unavailable, an
		// edge) counts only as the gate's probe: a check that went out
		// beside an open gate never trips it, so a local timeout on a
		// busy side pool cannot close it.
		rs.gateFailed(v, probe)
	case probe:
		// Nothing went out, or the answer is to a credential since
		// replaced: it says nothing about the gate.
		rs.gate.Released()
	}
	if !res.Verified {
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
	c.check.forceCheck.Store(true)
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

// sendChunk sends the lane's next chunk and acts on the answer. gateEpoch
// is the remote's gate epoch the lane passed the gate under.
func (sh *slabShip) sendChunk(ctx context.Context, lane *laneShip, e *remote.Entry, rs *remoteState, link *topic.RemoteLink, probe bool, gateEpoch uint64) {
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
	if rs.superseded(e) || !s.stillCurrent(rs, e) {
		// A password rotation, a new remote under the name, or a delete
		// arrived while the lane waited for a slot: send nothing with
		// the old credential; the lane looks the entry up again (and
		// holds in remote_missing when it is gone).
		rs.sem.Release()
		if probe {
			rs.gate.Released()
		}
		return
	}
	if !probe && (rs.gate.Closed() || rs.gate.Epoch() != gateEpoch) {
		// The gate closed while the lane waited for a slot: while it is
		// closed only its probe goes out, and once it reopens the target
		// is checked again before anything is sent. Send nothing; the
		// lane loop waits on the gate with its records held.
		rs.sem.Release()
		return
	}
	if stub, ok := sh.currentLink(ctx); !ok || stub.Remote.Paused {
		// Paused or detached while the lane waited for a slot: send
		// nothing; the lane loop ends the slab.
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
		sh.cur.stallFor(topic.RemoteStateTargetMissing, s.stallRetry)
		return
	}
	rctx, cancel := context.WithTimeout(sh.sendCtx, requestTimeout(e))
	resp, err := e.Do(rctx, remote.Outbound{Method: "POST", Path: path, Body: wire, ContentType: "application/json", ContentEncoding: encoding, Chunk: true})
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
	if err != nil && sh.sendCtx.Err() != nil {
		// Shutting down, detaching or re-reading: not the remote's fault.
		if probe {
			rs.gate.Released()
		}
		return
	}
	v := s.classify.Classify(sink.Answer{Resp: resp, Body: answer, Err: err, Chunk: n, Compressed: compressed})
	if v.Action == sink.ActResolveRoute {
		lctx, lcancel := context.WithTimeout(sh.sendCtx, requestTimeout(e))
		lresp, lbody, lerr := sink.FetchListing(lctx, e, link.Topic)
		lcancel()
		v = sink.ResolveRoute(lresp, lbody, lerr)
	}
	if v.Action != sink.ActCommitted && v.Action != sink.ActRecapacity && rl != nil {
		rl.ErrorsTotal.WithLabelValues(rs.name, v.Class).Inc()
	}
	sh.cur.noteAnswer(v.State, v.Status)
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
		sh.cur.clearStall()
		sh.cur.clearIdle()

	case sink.ActResendPrefix:
		rs.gate.Succeeded(probe)
		sh.cur.clearStall()
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
		sh.cur.clearStall()
		if n == 1 {
			sh.refuseOne(lane, topic.RemoteStateRejectedRecord)
		} else {
			sh.narrow(lane, n, n/2, n-n/2)
		}

	case sink.ActBlock:
		rs.gate.Succeeded(probe)
		sh.cur.clearStall()
		sh.block(lane, topic.RemoteStateRejectedRecord)
		sh.cur.forceTargetCheck()

	case sink.ActSplitTooLarge:
		rs.gate.Succeeded(probe)
		sh.cur.clearStall()
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
		laneBackoff := rs.gateFailed(v, probe)
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
		rs.gateFailed(v, probe)
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
		sh.cur.stallFor(v.State, s.stallRetry)
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
	case sink.ActRecapacity:
		// The target is reachable and refused no record: resend the same
		// records at the default chunk size.
		rs.gate.Succeeded(probe)
		rs.forgetCaps(e)
		s.r.logger.Warn("remote child target takes fewer messages per request than it did; sending default-size chunks until the next capability probe",
			"parent", sh.key.parent, "partition", sh.key.partition, "child", sh.key.child, "remote", rs.name)
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
	// Logged when the lane first blocks on the record, not on each
	// stall-interval retry that meets the same refusal.
	if sh.cur.firstBlockOn(rec.Offset) {
		sh.s.r.logger.Warn("remote child blocked on a record the target refuses",
			"parent", sh.key.parent, "partition", sh.key.partition, "offset", rec.Offset,
			"child", sh.key.child, "state", state)
	}
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
