package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// dispatchJob is one commit in flight: a destination's batch, sent from
// its own goroutine.
type dispatchJob struct {
	dest    *dispatchDest
	target  produceDispatchTarget
	records []ingress.ProduceRecord
	origs   []int
	// probe marks the single-record retry of a failing destination; it
	// runs on the short probe deadline.
	probe bool
	start time.Time
	// slow is set once the commit has run for produceDispatchSlowAfter;
	// it then no longer counts against the fan-out.
	slow bool
}

// dispatchResult is a finished commit. discard, set only on failure,
// marks the records confirmed gone (topic deleted, or an incarnation
// replaced by a new topic of the same name) that are dropped rather than
// retried.
type dispatchResult struct {
	job     *dispatchJob
	err     error
	discard []bool
}

// retryDue reports whether a failing or unresolved destination may try
// again: by the clock under Run, once per pass under DispatchAvailable.
func (d *ProduceDispatcher) retryDue(st *produceDispatchState, dest *dispatchDest, now time.Time) bool {
	if st.manual {
		return dest.probedPass != st.pass
	}
	return !now.Before(dest.retryAt)
}

// markSlow stops charging commits that have run for
// produceDispatchSlowAfter to the fan-out, and returns what queued
// behind each to the WAL: an owner that stopped answering must not hold
// the window's records for the whole RPC deadline. They are read again
// once the commit lands.
func (d *ProduceDispatcher) markSlow(st *produceDispatchState, now time.Time) {
	for job := range st.jobs {
		if !job.slow && now.Sub(job.start) >= produceDispatchSlowAfter {
			job.slow = true
			job.dest.slow = true
			st.active--
			st.slowHeld += len(job.records)
			d.releaseAll(st, job.dest, nil, nil)
		}
	}
}

// launch starts a commit for each ready destination, oldest first, up
// to the fan-out, except those lingering for a fuller batch. Destinations
// waiting out a retry rejoin the list when it falls due.
func (d *ProduceDispatcher) launch(ctx context.Context, st *produceDispatchState) {
	now := d.now()
	for dest := range st.waiting {
		if d.retryDue(st, dest, now) {
			delete(st.waiting, dest)
			st.enqueue(dest)
		}
	}
	fanout := max(d.commitConcurrency, 1)
	// Destinations that do not go now keep their place, in order.
	// startCommit can append to st.ready (a reroute), so its length is
	// read on every turn.
	kept := 0
	for i := 0; i < len(st.ready); i++ {
		dest := st.ready[i]
		if st.active >= fanout {
			kept += copy(st.ready[kept:], st.ready[i:])
			break
		}
		if until, ok := d.lingerUntil(st, dest); ok && now.Before(until) {
			st.ready[kept] = dest
			kept++
			continue
		}
		dest.inReady = false
		d.startCommit(ctx, st, dest, now)
	}
	clear(st.ready[kept:])
	st.ready = st.ready[:kept]
}

// lingerUntil reports whether dest's next commit waits for a fuller
// batch, and until when. While other commits are in flight, a batch
// below the floor (produceDispatchTargetPerPartition records) waits up to
// twice its owner's recent commit latency, capped at
// produceDispatchMaxLinger: the loop wakes on every WAL group commit,
// and without the wait each one would go out as a handful of records
// per partition, an fsync each, on the same disks the WAL group commit
// waits for. The latency is the owner's own, so a slow owner does not
// hold back another's partitions. An idle dispatcher, an owner with no
// commit measured yet, a failing or unresolved destination, and
// DispatchAvailable commit at once.
func (d *ProduceDispatcher) lingerUntil(st *produceDispatchState, dest *dispatchDest) (time.Time, bool) {
	if st.manual || st.active == 0 || dest.failing() || dest.unresolved || len(dest.queue) == 0 ||
		len(dest.queue) >= min(produceDispatchTargetPerPartition, d.perDestCap(st)) {
		return time.Time{}, false
	}
	if !dest.ownerKnown {
		target, err := d.dispatchTarget(ingress.ProduceRecord{Topic: dest.key.topic, TargetPartition: dest.key.partition})
		if err != nil {
			return time.Time{}, false
		}
		dest.owner, dest.ownerKnown = target.addr, true
	}
	latency, ok := st.latency[dest.owner]
	if !ok {
		return time.Time{}, false
	}
	return dest.queuedAt.Add(min(2*latency, produceDispatchMaxLinger)), true
}

// noteLatency folds a commit's duration into its owner's EWMA; the first
// commit to an owner sets it.
func (st *produceDispatchState) noteLatency(owner string, took time.Duration) {
	prev, ok := st.latency[owner]
	if !ok {
		if len(st.latency) >= produceDispatchMemoLimit {
			clear(st.latency)
		}
		st.latency[owner] = took
		return
	}
	st.latency[owner] = prev + (took-prev)/8
}

// startCommit sends dest's queued records as one batch, or, for a
// failing destination, its first record as a probe. A batch for a remote
// owner takes only as many records as fit in one stream frame (see
// remoteBatchLen); the rest stay queued and go out once it lands.
func (d *ProduceDispatcher) startCommit(ctx context.Context, st *produceDispatchState, dest *dispatchDest, now time.Time) {
	if dest.inflight || len(dest.queue) == 0 {
		return
	}
	if dest.failing() || dest.unresolved {
		dest.probedPass = st.pass
	}
	target, err := d.dispatchTarget(ingress.ProduceRecord{Topic: dest.key.topic, TargetPartition: dest.key.partition})
	if err != nil {
		d.unresolvedAtLaunch(ctx, st, dest, err, now)
		return
	}
	dest.unresolved = false
	dest.owner, dest.ownerKnown = target.addr, true
	d.dropReplaced(st, dest)
	if len(dest.queue) == 0 {
		st.forget(dest)
		return
	}
	n := len(dest.queue)
	probe := dest.failing()
	switch {
	case probe:
		n = 1
	case !target.local:
		n = remoteBatchLen(dest.queue)
	}
	job := &dispatchJob{
		dest:    dest,
		target:  target,
		records: dest.queue[:n:n],
		origs:   dest.origs[:n:n],
		probe:   probe,
		start:   now,
	}
	dest.queue, dest.origs = dest.queue[n:], dest.origs[n:]
	if len(dest.queue) == 0 {
		dest.queue, dest.origs = nil, nil
	}
	dest.inflight = true
	st.jobs[job] = struct{}{}
	st.active++
	st.outstanding++
	go d.runJob(ctx, job)
}

// produceRemoteRecordOverhead is what one record adds to an encoded
// commit batch beyond its topic, key, payload and topic ID: four length
// prefixes and the created-at time, plus a topic-ID run header, counted
// as if every record started a run so the estimate never falls short.
const produceRemoteRecordOverhead = 4 + 4 + 4 + 4 + 8 + 4 + 4

// remoteBatchLen is how many of records, from the front, one remote
// commit carries: as many as fit in produceRemoteBatchBytes of encoded
// batch. Always at least one, so a record larger than the budget still
// goes out, on its own.
func remoteBatchLen(records []ingress.ProduceRecord) int {
	size := 0
	for i, rec := range records {
		size += produceRemoteRecordOverhead + len(rec.Topic) + len(rec.Key) + len(rec.Payload) + len(rec.TopicID)
		if size > produceRemoteBatchBytes && i > 0 {
			return i
		}
	}
	return len(records)
}

// unresolvedAtLaunch handles a destination whose owner stopped
// resolving after its records were queued: discard them if the topic is
// confirmed deleted, reroute them if a sibling partition can take them,
// and otherwise keep one as the probe and leave the rest in the WAL
// until the owner resolves again.
func (d *ProduceDispatcher) unresolvedAtLaunch(ctx context.Context, st *produceDispatchState, dest *dispatchDest, err error, now time.Time) {
	if d.topicDeleted(ctx, st, dest.key.topic) {
		d.logger.Warn("discarding undispatched produce records for deleted topic",
			"topic", dest.key.topic, "partition", dest.key.partition,
			"records", len(dest.queue), "err", err)
		for _, rec := range dest.queue {
			st.marks.set(rec.WAL.Seq, seqDone)
		}
		d.unhold(st, dest, len(dest.queue))
		dest.queue, dest.origs = nil, nil
		dest.unresolved = false
		st.forget(dest)
		return
	}
	if alt, ok := d.rerouteFor(st, dest.key); ok {
		d.logger.Warn("rerouting produce records for unavailable partition owner",
			"topic", dest.key.topic, "from_partition", dest.key.partition,
			"to_partition", alt.key.partition, "records", len(dest.queue))
		d.moveQueue(st, dest, alt)
		st.forget(dest)
		return
	}
	st.noteErr(err)
	dest.unresolved = true
	dest.retryAt = now.Add(d.interval)
	st.waiting[dest] = struct{}{}
	d.keepProbe(st, dest, nil, nil)
}

// dropReplaced returns to the WAL the queued records whose incarnation
// the local replica has since replaced with a new topic of the same
// name: the reader confirms that with the leader and discards them.
// Committing them here would put them into the new topic.
func (d *ProduceDispatcher) dropReplaced(st *produceDispatchState, dest *dispatchDest) {
	info := d.topicInfo(st, dest.key.topic)
	if !info.exists || info.id == "" {
		return
	}
	kept := 0
	for i, rec := range dest.queue {
		if rec.TopicID == "" || rec.TopicID == info.id {
			dest.queue[kept], dest.origs[kept] = rec, dest.origs[i]
			kept++
			continue
		}
		orig := st.dest(dispatchDestKey{topic: rec.Topic, partition: dest.origs[i]})
		st.release(rec, orig)
		st.requestRescan(orig)
	}
	if dropped := len(dest.queue) - kept; dropped > 0 {
		d.unhold(st, dest, dropped)
		clear(dest.queue[kept:])
		dest.queue, dest.origs = dest.queue[:kept], dest.origs[:kept]
	}
}

// runJob commits one batch and hands the outcome back to the loop.
func (d *ProduceDispatcher) runJob(ctx context.Context, job *dispatchJob) {
	timeout := produceCommitRPCTimeout
	if job.probe {
		timeout = produceProbeRPCTimeout
	}
	err := d.dispatchRecordBatch(ctx, job.target, job.records, timeout)
	var discard []bool
	if err != nil && ctx.Err() == nil {
		discard = d.discardable(ctx, job.records)
	}
	d.results <- dispatchResult{job: job, err: err, discard: discard}
}

// finish merges a finished commit into the state.
func (d *ProduceDispatcher) finish(ctx context.Context, st *produceDispatchState, res dispatchResult) {
	job := res.job
	dest := job.dest
	delete(st.jobs, job)
	st.outstanding--
	if job.slow {
		st.slowHeld -= len(job.records)
	} else {
		st.active--
	}
	dest.inflight, dest.slow = false, false
	d.unhold(st, dest, len(job.records))

	if res.err == nil {
		if !job.slow {
			st.noteLatency(job.target.addr, d.now().Sub(job.start))
		}
		for _, rec := range job.records {
			st.marks.set(rec.WAL.Seq, seqDone)
		}
		if dest.failing() || dest.unresolved {
			// It takes commits again: its records left in the WAL come
			// back, in order, and new ones queue normally.
			dest.failingSince = time.Time{}
			dest.unresolved = false
			delete(st.waiting, dest)
			st.requestRescan(dest)
		}
		d.afterCommit(st, dest)
		return
	}

	failed, failedOrigs := job.records, job.origs
	if res.discard != nil {
		failed, failedOrigs = nil, nil
		for i, rec := range job.records {
			if res.discard[i] {
				d.logger.Warn("discarding undispatched record for deleted topic",
					"topic", rec.Topic, "topic_id", rec.TopicID, "partition", rec.TargetPartition,
					"seq", rec.WAL.Seq, "err", res.err)
				st.marks.set(rec.WAL.Seq, seqDone)
				continue
			}
			failed = append(failed, rec)
			failedOrigs = append(failedOrigs, job.origs[i])
		}
	}
	if len(failed) == 0 {
		d.afterCommit(st, dest)
		return
	}
	if ctx.Err() != nil {
		// Shutting down (or the caller gave up): nothing retries now,
		// so the records go back to the WAL for whoever reads it next.
		d.releaseAll(st, dest, failed, failedOrigs)
		st.noteErr(res.err)
		return
	}
	if errors.Is(res.err, brokermsg.ErrTopicIncarnationMismatch) {
		// The owner answered, with another incarnation of the topic than
		// the records': a delete and recreate raced them, or its replica
		// or this one lags. That says nothing about the owner, so the
		// destination is not failing and nothing is rerouted: the records,
		// and what queued behind them, go back to the WAL in order, and the
		// rescan checks each against the current incarnation again (see
		// place), which discards it once the leader confirms its
		// incarnation is gone, or commits it where it belongs once the
		// owner has caught up.
		d.logger.Warn("owner refused produce records accepted for another topic incarnation; checking them again",
			"topic", dest.key.topic, "partition", dest.key.partition, "owner", job.target.addr,
			"records", len(failed)+len(dest.queue), "err", res.err)
		dest.failingSince = time.Time{}
		delete(st.waiting, dest)
		d.releaseAll(st, dest, failed, failedOrigs)
		st.forget(dest)
		return
	}

	now := d.now()
	if !dest.failing() {
		dest.failingSince = job.start
	}
	dest.retryAt = now.Add(d.failureBackoff)
	dest.probedPass = st.pass
	if now.Sub(dest.failingSince) >= produceDispatchRerouteGrace {
		if alt, ok := d.rerouteFor(st, dest.key); ok {
			// Past the grace the owner counts as dead: the failed
			// records, and whatever queued behind them, go to a live
			// sibling in order. The error is not the pass's: nothing
			// is left behind.
			d.logger.Warn("rerouting produce records for stuck partition owner",
				"topic", dest.key.topic, "from_partition", dest.key.partition,
				"to_partition", alt.key.partition, "records", len(failed)+len(dest.queue), "err", res.err)
			d.holdAll(st, alt, failed, failedOrigs)
			d.moveQueue(st, dest, alt)
			st.waiting[dest] = struct{}{}
			// Its records left in the WAL follow them now rather than
			// on the rescan backstop.
			st.requestRescan(dest)
			return
		}
	}
	st.noteErr(res.err)
	// Waiting first, so the probe it keeps is not put straight back on
	// the ready list: the next attempt goes when the retry falls due.
	st.waiting[dest] = struct{}{}
	d.keepProbe(st, dest, failed, failedOrigs)
}

// afterCommit puts dest back on the ready list if records queued behind
// the commit, and asks for its records left in the WAL for want of room
// once its queue has drained.
func (d *ProduceDispatcher) afterCommit(st *produceDispatchState, dest *dispatchDest) {
	if dest.skipped > 0 && !dest.failing() && !dest.unresolved && len(dest.queue) < d.perDestCap(st)/2 {
		st.requestRescan(dest)
	}
	st.enqueue(dest)
	st.forget(dest)
}

// keepProbe makes dest's lowest record (of failed, then its queue) the
// one it holds as the next probe and returns the rest to the WAL.
func (d *ProduceDispatcher) keepProbe(st *produceDispatchState, dest *dispatchDest, failed []ingress.ProduceRecord, failedOrigs []int) {
	all := append(append([]ingress.ProduceRecord(nil), failed...), dest.queue...)
	origs := append(append([]int(nil), failedOrigs...), dest.origs...)
	d.unhold(st, dest, len(dest.queue))
	dest.queue, dest.origs = nil, nil
	if len(all) == 0 {
		return
	}
	d.holdAll(st, dest, all[:1], origs[:1])
	for i, rec := range all[1:] {
		st.release(rec, st.dest(dispatchDestKey{topic: rec.Topic, partition: origs[i+1]}))
	}
}

// releaseAll returns failed and dest's queue to the WAL.
func (d *ProduceDispatcher) releaseAll(st *produceDispatchState, dest *dispatchDest, failed []ingress.ProduceRecord, failedOrigs []int) {
	for i, rec := range failed {
		st.release(rec, st.dest(dispatchDestKey{topic: rec.Topic, partition: failedOrigs[i]}))
	}
	for i, rec := range dest.queue {
		st.release(rec, st.dest(dispatchDestKey{topic: rec.Topic, partition: dest.origs[i]}))
	}
	d.unhold(st, dest, len(dest.queue))
	dest.queue, dest.origs = nil, nil
}

// holdAll queues records that are not currently counted as held on
// dest, keeping their order.
func (d *ProduceDispatcher) holdAll(st *produceDispatchState, dest *dispatchDest, records []ingress.ProduceRecord, origs []int) {
	if len(dest.queue) == 0 && len(records) > 0 {
		dest.queuedAt = d.now()
	}
	for i, rec := range records {
		rec.TargetPartition = dest.key.partition
		st.marks.set(rec.WAL.Seq, seqHeld)
		dest.queue = append(dest.queue, rec)
		dest.origs = append(dest.origs, origs[i])
	}
	dest.held += len(records)
	st.held += len(records)
	st.enqueue(dest)
}

// moveQueue moves from's queued records onto to's queue.
func (d *ProduceDispatcher) moveQueue(st *produceDispatchState, from, to *dispatchDest) {
	records, origs := from.queue, from.origs
	d.unhold(st, from, len(records))
	from.queue, from.origs = nil, nil
	d.holdAll(st, to, records, origs)
}

// unhold drops n records from dest's and the state's held counts.
func (d *ProduceDispatcher) unhold(st *produceDispatchState, dest *dispatchDest, n int) {
	dest.held -= n
	st.held -= n
}

// discardable reports which records of a failed batch are confirmed
// gone: all of them when the topic is confirmed deleted, and those whose
// incarnation the leader confirms was replaced by a new topic of the
// same name. Everything else is retried. It runs on the commit's own
// goroutine, so a slow leader holds up only this destination.
func (d *ProduceDispatcher) discardable(ctx context.Context, records []ingress.ProduceRecord) []bool {
	if len(records) == 0 || d.store == nil {
		return nil
	}
	if d.topicConfirmedDeleted(ctx, records[0].Topic) {
		discard := make([]bool, len(records))
		for i := range discard {
			discard[i] = true
		}
		return discard
	}
	local, err := d.store.GetTopic(ctx, records[0].Topic)
	if err != nil || local.ID == "" {
		return nil
	}
	var discard []bool
	gone := map[string]bool{}
	for i, rec := range records {
		if rec.TopicID == "" || rec.TopicID == local.ID {
			continue
		}
		isGone, checked := gone[rec.TopicID]
		if !checked {
			isGone = d.incarnationGoneOnLeader(ctx, rec.Topic, rec.TopicID)
			gone[rec.TopicID] = isGone
		}
		if isGone {
			if discard == nil {
				discard = make([]bool, len(records))
			}
			discard[i] = true
		}
	}
	return discard
}

// topicConfirmedDeleted reports whether topicName is deleted with enough
// certainty to discard its accepted WAL records: absent from the local
// replica, the replica caught up with the leader, and — unless this node
// IS the leader — the leader itself confirming the topic is gone. Local
// absence alone is not enough: a replica restored from an old Raft
// snapshot is missing every topic created after the snapshot point, and
// discarding on that view would destroy 202-accepted records. Returning
// false just retries the records later.
//
// The key is the local replica, never a commit error. That is the safe
// signal: a record only reached this WAL because AcceptProduce saw the
// topic in this replica, and Raft replicas only move forward, so if the
// topic is now absent here, a delete was truly applied (it cannot be
// create-replication lag). Any other failure (transient network, a
// lagging remote owner returning 404 for a live topic, owner moved,
// malformed record) is retried rather than silently dropping data.
func (d *ProduceDispatcher) topicConfirmedDeleted(ctx context.Context, topicName string) bool {
	if d.store == nil {
		return false
	}
	if _, err := d.store.GetTopic(ctx, topicName); !errors.Is(err, errs.ErrNotFound) {
		return false
	}
	if !d.store.AppliedCaughtUp() {
		return false
	}
	return topicAbsentOnLeader(ctx, d.store, d.peer, d.selfID, topicName, d.logger)
}

// incarnationGoneOnLeader reports whether the leader confirms that the
// topic incarnation id no longer exists: the name is absent, or it now
// belongs to another incarnation. The local replica cannot decide this
// alone: one restored from an old snapshot can show an older
// incarnation under the name.
func (d *ProduceDispatcher) incarnationGoneOnLeader(ctx context.Context, topicName, id string) bool {
	if d.store == nil || !d.store.AppliedCaughtUp() {
		return false
	}
	t, absent, ok := leaderTopicView(ctx, d.store, d.peer, d.selfID, topicName, d.logger)
	if !ok {
		return false
	}
	return absent || (t.ID != "" && t.ID != id)
}

// topicDeleted is topicConfirmedDeleted memoized for the loop. A
// confirmed delete holds for every record read so far (all accepted
// before the check), so it is kept for the current read only: a later
// read may see records of a topic recreated since. An unconfirmed
// answer is kept for failureBackoff, so a leader that cannot be reached
// is asked about once a second, not once per record.
func (d *ProduceDispatcher) topicDeleted(ctx context.Context, st *produceDispatchState, topicName string) bool {
	if st.deletedEpoch != st.readEpoch || st.deleted == nil {
		st.deleted = map[string]bool{}
		st.deletedEpoch = st.readEpoch
	}
	if deleted, ok := st.deleted[topicName]; ok {
		return deleted
	}
	key := "deleted:" + topicName
	now := d.now()
	if until, ok := st.unsure[key]; ok && now.Before(until) {
		return false
	}
	deleted := d.topicConfirmedDeleted(ctx, topicName)
	st.deleted[topicName] = deleted
	if !deleted {
		// Stamped after the check: one slower than failureBackoff would
		// otherwise leave a memo that has already run out.
		d.noteUnsure(st, key, d.now())
	}
	return deleted
}

// incarnationCheck classifies a record by the topic incarnation it was
// accepted under.
type incarnationCheck uint8

const (
	// incarnationLive: the record has no incarnation, or its topic
	// still is that incarnation (or is gone, which the name-based
	// delete check handles).
	incarnationLive incarnationCheck = iota
	// incarnationGone: the leader confirms the incarnation was
	// replaced; the record is discarded.
	incarnationGone
	// incarnationUnconfirmed: the local replica shows another
	// incarnation but the leader has not confirmed it; the record waits.
	incarnationUnconfirmed
)

// incarnationState checks a record's topic incarnation against the
// local replica before anything resolves or reroutes it by name: a
// record of a deleted topic whose name was recreated would otherwise
// commit into the new topic, bypassing its schema and ownership.
func (d *ProduceDispatcher) incarnationState(ctx context.Context, st *produceDispatchState, rec ingress.ProduceRecord) incarnationCheck {
	if rec.TopicID == "" || d.store == nil {
		return incarnationLive
	}
	info := d.topicInfo(st, rec.Topic)
	if !info.exists || info.id == "" || info.id == rec.TopicID {
		return incarnationLive
	}
	if _, gone := st.goneIDs[rec.TopicID]; gone {
		return incarnationGone
	}
	key := "incarnation:" + rec.TopicID
	now := d.now()
	if until, ok := st.unsure[key]; ok && now.Before(until) {
		return incarnationUnconfirmed
	}
	if d.incarnationGoneOnLeader(ctx, rec.Topic, rec.TopicID) {
		// A replaced incarnation never comes back: remember it for
		// good (bounded, the set only needs the incarnations still in
		// the WAL).
		if len(st.goneIDs) >= produceDispatchMemoLimit {
			clear(st.goneIDs)
		}
		st.goneIDs[rec.TopicID] = struct{}{}
		return incarnationGone
	}
	d.noteUnsure(st, key, d.now()) // after the check, as in topicDeleted
	return incarnationUnconfirmed
}

// produceDispatchMemoLimit bounds the loop's per-topic memos.
const produceDispatchMemoLimit = 1024

func (d *ProduceDispatcher) noteUnsure(st *produceDispatchState, key string, now time.Time) {
	if len(st.unsure) >= produceDispatchMemoLimit {
		for k, until := range st.unsure {
			if !now.Before(until) {
				delete(st.unsure, k)
			}
		}
		if len(st.unsure) >= produceDispatchMemoLimit {
			clear(st.unsure)
		}
	}
	st.unsure[key] = now.Add(d.failureBackoff)
}

func (d *ProduceDispatcher) dispatchRecordBatch(ctx context.Context, target produceDispatchTarget, records []ingress.ProduceRecord, timeout time.Duration) error {
	if len(records) == 0 {
		return nil
	}
	if target.local {
		return d.commitLocal(ctx, records)
	}
	return d.commitRemote(ctx, target.addr, records, timeout)
}

func (d *ProduceDispatcher) commitLocal(ctx context.Context, records []ingress.ProduceRecord) error {
	if d.committer == nil {
		return errors.New("produce dispatcher committer is nil")
	}
	if batcher, ok := d.committer.(produceBatchCommitter); ok {
		_, err := batcher.CommitAcceptedProduceBatch(ctx, records)
		return err
	}
	for _, record := range records {
		if _, err := d.committer.CommitAcceptedProduce(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

// commitRemote sends a batch to its owner within timeout. The records
// carry their topic incarnation so the owner can refuse a replaced one;
// an owner on an older release refuses the whole frame for its trailing
// field, and then gets the batch again without it, on what is left of
// the same budget, and is remembered for produceLegacyOwnerTTL.
func (d *ProduceDispatcher) commitRemote(ctx context.Context, addr string, records []ingress.ProduceRecord, timeout time.Duration) error {
	if d.peer == nil {
		return errors.New("produce dispatcher peer client is nil")
	}
	legacy := d.legacyOwner(addr)
	withIDs := false
	req := nodewire.CommitProduceBatchRequest{Records: make([]nodewire.CommitProduceRequest, 0, len(records))}
	for _, record := range records {
		r := nodewire.CommitProduceRequest{
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		}
		if !legacy && record.TopicID != "" {
			r.TopicID = record.TopicID
			withIDs = true
		}
		req.Records = append(req.Records, r)
	}
	// An explicit budget: without one the transport's short default reply
	// timeout applies, and a slow-but-successful remote commit would be
	// re-committed as duplicates (see produceCommitRPCTimeout). It goes
	// to the transport rather than into a context derived per commit,
	// and runs out as an error wrapping context.DeadlineExceeded, as a
	// ctx deadline would; ctx still carries cancellation.
	start := time.Now()
	res, err := d.peer.CommitProduceBatchWithin(ctx, addr, timeout, req)
	if err == nil && withIDs && isTrailingFieldRefusal(res) {
		d.legacyOwners.Store(addr, d.now().Add(produceLegacyOwnerTTL))
		for i := range req.Records {
			req.Records[i].TopicID = ""
		}
		left, spent := remainingBudget(timeout, start)
		if spent != nil {
			return fmt.Errorf("commit produce batch without topic ids: %w", spent)
		}
		res, err = d.peer.CommitProduceBatchWithin(ctx, addr, left, req)
	}
	if err != nil {
		return err
	}
	if res.Status == http.StatusPreconditionFailed {
		// The owner holds another incarnation of the topic under the name
		// (see RPCServer.brokerErrorStatus): the same outcome as a local
		// commit's, so finish handles both alike.
		return fmt.Errorf("commit produce batch to %s: %w", addr, brokermsg.ErrTopicIncarnationMismatch)
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return fmt.Errorf("commit produce batch returned status %d", res.Status)
	}
	return nil
}

// remainingBudget is what is left at this moment of a per-call budget of
// timeout that started at start. A budget of zero or less bounds
// nothing and stays as it is; one that has run out is an error wrapping
// context.DeadlineExceeded, as the transport reports its own.
func remainingBudget(timeout time.Duration, start time.Time) (time.Duration, error) {
	if timeout <= 0 {
		return timeout, nil
	}
	left := timeout - time.Since(start)
	if left <= 0 {
		return 0, fmt.Errorf("budget of %s spent: %w", timeout, context.DeadlineExceeded)
	}
	return left, nil
}

// legacyOwner reports whether commits to addr must go out without topic
// IDs, and forgets an entry whose TTL has passed so they are tried
// again.
func (d *ProduceDispatcher) legacyOwner(addr string) bool {
	v, ok := d.legacyOwners.Load(addr)
	if !ok {
		return false
	}
	if d.now().Before(v.(time.Time)) {
		return true
	}
	d.legacyOwners.Delete(addr)
	return false
}
