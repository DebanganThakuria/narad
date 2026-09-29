package cluster

import (
	"context"
	"errors"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

var errProduceReplayBoundary = errors.New("produce replay reached durable boundary")

// How the dispatcher moves records, in brief.
//
// Every seq in [checkpoint, read frontier) is in one of three states
// (seqMarks): done (committed, discarded for a deleted topic or a
// replaced incarnation, or a hole in the WAL), held (in memory, queued
// for or in flight to a destination partition), or skipped (left in the
// WAL, to be read again). The checkpoint is the first seq that is not
// done; compaction never deletes WAL records at or past it, so a record
// that is not yet committed always survives a crash.
//
// The reader reads each newly durable record once and places it on its
// destination partition's queue. Each destination has at most one
// commit in flight: when it lands, whatever queued meanwhile goes out
// as the next batch. Different destinations commit independently (up
// to the fan-out), so a slow or unreachable owner holds up its own
// partitions and nothing else; the records it pins keep the checkpoint
// where it is, which the lookahead horizon bounds. Batching comes from
// the queueing itself: the longer a commit takes, the more records the
// next one carries, so a busy owner gets fewer, fatter commits (one
// fsync each), up to what fits in one stream frame for a remote owner
// (see remoteBatchLen). While other commits are in flight, a
// destination below the batch floor also lingers briefly for more
// records rather than sending each WAL group commit's handful on its
// own (see lingerUntil); an idle dispatcher commits at once.
//
// A record is left in the WAL (skipped) rather than held when holding
// it would cost memory for nothing or break partition order: its
// destination is failing and already holds its probe, its commit has
// been in flight for produceDispatchSlowAfter, its queue is at
// perDestCap, an earlier record of the same destination is still
// skipped, or a delete or incarnation change is not yet confirmed.
// Skipped records are read again, in WAL order, by a rescan: when their
// destination recovers or drains, and on the rescan backstop.
//
// A record whose destination cannot take commits must not block
// delivery: because every destination shares this one WAL, a single
// dead partition owner would otherwise stop newer records for ALL
// topics and partitions while producers keep getting 2xx. The
// dispatcher therefore REROUTES such records to another partition of
// the same topic whose owner is alive — deliberately sacrificing
// per-key partition ordering to preserve availability, the exact trade
// the accept path already makes when it skips dead-owner partitions at
// partition-selection time (messaging.Engine.pickProducePartition). Two
// tiers trigger a reroute:
//
//   - target resolution fails for a live topic (owner dead or missing
//     per membership): membership death is authoritative, so the record
//     is rerouted immediately, with the same authority as the
//     accept-time skip;
//   - commits keep failing while membership still says the owner is
//     alive: for produceDispatchRerouteGrace the destination's records
//     retry on their own partition (one probe record, once per
//     failureBackoff), and after that its records are rerouted, while
//     a probe still goes to the destination once per failureBackoff so
//     it gets its records back the moment it recovers.
//
// Only when NO live-owner partition of the topic exists does a record
// stay stuck and pin the checkpoint; records up to the lookahead
// horizon keep committing past it.
//
// Records of a destination whose commit is still in flight are never
// re-sent or rerouted: they are held by that commit until it finishes.
// In steady state each record is therefore delivered exactly once;
// delivery degrades to at-least-once on two paths:
//
//   - crash replay: which records above the checkpoint committed lives
//     only in memory, and the stored checkpoint may trail the in-memory
//     one (the store is written every pass but synced lazily), so a
//     crash re-commits records;
//   - commit-RPC timeout: remote commits carry no idempotency token on
//     the wire, so a commit that exceeds its deadline yet succeeds on
//     the remote is retried (and, past the reroute grace, rerouted),
//     duplicating the batch. The generous timeout makes this rare, not
//     impossible; a probe's shorter one risks a single record.

// seqMark is the state of one seq between the checkpoint and the read
// frontier.
type seqMark uint8

const (
	seqSkipped seqMark = iota // in the WAL, to be read again
	seqHeld                   // queued or in flight
	seqDone                   // needs no further work
)

// seqMarks holds a seqMark for every seq in [base, base+len). WAL seqs
// are dense, so a slice indexed from the checkpoint replaces a per-seq
// map; the lookahead horizon bounds its length.
type seqMarks struct {
	base  uint64
	marks []seqMark
	head  int
}

func (m *seqMarks) end() uint64 { return m.base + uint64(len(m.marks)-m.head) }

func (m *seqMarks) get(seq uint64) seqMark {
	return m.marks[m.head+int(seq-m.base)]
}

func (m *seqMarks) set(seq uint64, mark seqMark) {
	m.marks[m.head+int(seq-m.base)] = mark
}

// extendTo appends seq and marks the seqs between the old end and it
// done: a gap in the WAL's seq space is records the WAL no longer
// holds (a lying disk's lost tail), and waiting for them would pin the
// checkpoint forever.
func (m *seqMarks) extendTo(seq uint64) {
	for m.end() < seq {
		m.marks = append(m.marks, seqDone)
	}
	m.marks = append(m.marks, seqSkipped)
}

// popDone drops the leading done seqs and returns how many it dropped.
func (m *seqMarks) popDone() uint64 {
	n := 0
	for m.head+n < len(m.marks) && m.marks[m.head+n] == seqDone {
		n++
	}
	m.head += n
	m.base += uint64(n)
	if m.head == len(m.marks) {
		m.marks, m.head = m.marks[:0], 0
	} else if m.head >= 4096 && m.head*2 >= len(m.marks) {
		m.marks = append(m.marks[:0], m.marks[m.head:]...)
		m.head = 0
	}
	return uint64(n)
}

// produceDispatchState is the dispatcher's state. Only the loop
// goroutine (Run's, or DispatchAvailable's caller) touches it; commit
// goroutines hand their outcome back through ProduceDispatcher.results.
type produceDispatchState struct {
	// nextSeq is the checkpoint: every seq below it is done.
	nextSeq uint64
	// storedSeq is the last checkpoint written to disk; compaction never
	// goes past it.
	storedSeq uint64
	// compactedSeq is the bound the WAL was last compacted to. It trails
	// storedSeq until the checkpoint's sync lands (see
	// ingress.Manager.CompactProduceBefore); while it does, idle passes
	// keep compacting so a node that stops producing still reclaims the
	// WAL behind its last checkpoint.
	compactedSeq uint64
	// readSeq is the first seq the reader has not seen, and readCursor
	// the WAL position to resume reading from.
	readSeq    uint64
	readCursor wal.Cursor
	// marks covers [nextSeq, readSeq).
	marks seqMarks

	// windowLimit is the adaptive window: it sets how many records may
	// be held in memory at once (holdLimit) and, times
	// produceDispatchLookaheadWindows, the horizon past the checkpoint.
	// It grows toward
	// produceDispatchTargetPerPartition * (distinct destinations) as
	// soon as that fan-out is seen and shrinks back only after a whole
	// window of records has shown less, clamped to [base, BatchSize].
	windowLimit  int
	epochDests   map[dispatchDestKey]struct{}
	epochRecords int

	dests map[dispatchDestKey]*dispatchDest
	// ready lists, in order, the destinations with queued records and
	// no commit in flight; waiting holds the failing or unresolved ones
	// until their retry is due.
	ready   []*dispatchDest
	waiting map[*dispatchDest]struct{}
	// held counts records queued or in flight, and slowHeld the part of
	// them in slow commits, which holdLimit does not count: a hung
	// owner's batches stay in memory until their RPCs give up, but must
	// not stop everyone else's records from being read.
	held     int
	slowHeld int
	// jobs are the commits in flight; active counts those still charged
	// to the fan-out, outstanding all of them.
	jobs        map[*dispatchJob]struct{}
	active      int
	outstanding int
	// latency is each owner's recent commit latency (keyed by address,
	// "" for this node), an EWMA over the commits that succeeded before
	// turning slow. It sets how long a small batch lingers.
	latency map[string]time.Duration

	// skipped counts skipped records. A rescan reads them again from
	// rescanFrom (when rescanDue) or, on the backstop, from the lowest
	// firstSkipped of any destination.
	skipped    int
	rescanDue  bool
	rescanFrom wal.Cursor
	rescanSet  bool
	lastRescan time.Time
	// lastSweep is when sweepIdle last ran.
	lastSweep time.Time

	// pass numbers the loop's rounds (each DispatchAvailable call is one
	// pass); readEpoch numbers the reader's calls. Memos key off them.
	pass      uint64
	readEpoch uint64
	manual    bool
	// err is the first error of the pass that left records uncommitted.
	err error
	// stalled is set by a round that could not read the WAL or store
	// the checkpoint; Run then waits out failureBackoff (see nextWake).
	stalled bool

	// Per-read memos and caches for the incarnation and delete checks
	// (see produce_commit.go).
	goneIDs      map[string]struct{}
	unsure       map[string]time.Time
	deleted      map[string]bool
	deletedEpoch uint64
	topics       map[string]cachedDispatchTopic
	// rerouteMemo is rerouteFor's answer per destination for the read
	// numbered rerouteEpoch (-1: nowhere to reroute).
	rerouteMemo  map[dispatchDestKey]int
	rerouteEpoch uint64
	// rerouted counts, per original destination, the records rerouted
	// in the current read, for one log line each.
	rerouted map[dispatchDestKey]rerouteNote
}

func (st *produceDispatchState) noteErr(err error) {
	if st.err == nil && err != nil {
		st.err = err
	}
}

// dispatchDest is one destination partition: where its records queue
// for commit, and, as the partition records were accepted for, how many
// of them sit skipped in the WAL.
type dispatchDest struct {
	key dispatchDestKey
	// queue holds this destination's records waiting for the next
	// commit, in WAL-seq order, and origs the partition each was
	// accepted for (it differs for a rerouted record).
	queue []ingress.ProduceRecord
	origs []int
	// inflight is set while a commit to this destination runs, and slow
	// once that commit has run for produceDispatchSlowAfter; inReady
	// while the destination is on state.ready.
	inflight bool
	slow     bool
	inReady  bool
	// queuedAt is when the oldest record now on queue was queued; a
	// small batch lingers from then (see lingerUntil).
	queuedAt time.Time
	// owner is the address commits to this destination last went to
	// ("" for this node), once ownerKnown; it picks the commit latency
	// a small batch lingers by.
	owner      string
	ownerKnown bool
	// held counts this destination's records queued or in flight.
	held int

	// skipped counts records accepted for this partition that sit
	// skipped in the WAL, and firstSkipped is a cursor at or below the
	// lowest of them. blockedRead is the read (state.readEpoch) in which
	// a rescan skipped one of them again, so later ones in that rescan
	// follow suit.
	skipped      int
	firstSkipped wal.Cursor
	blockedRead  uint64

	// failingSince is the start of the first commit attempt in the
	// current run of failures, zero while commits succeed; retryAt is
	// when the next attempt may go. probedPass is the pass whose retry
	// already went out, for DispatchAvailable, which retries once per
	// pass rather than by the clock.
	failingSince time.Time
	retryAt      time.Time
	probedPass   uint64
	// unresolved is set while the destination's owner cannot be
	// resolved and no other partition could take its records.
	unresolved bool
}

func (dest *dispatchDest) failing() bool { return !dest.failingSince.IsZero() }

type rerouteNote struct {
	to      int
	records int
}

// dest returns the destination for key, creating it on first use.
func (st *produceDispatchState) dest(key dispatchDestKey) *dispatchDest {
	dest, ok := st.dests[key]
	if !ok {
		dest = &dispatchDest{key: key}
		st.dests[key] = dest
	}
	return dest
}

// forget drops a destination that holds nothing, skips nothing and is
// healthy, so the map tracks active partitions rather than every one
// ever seen.
func (st *produceDispatchState) forget(dest *dispatchDest) {
	if dest.held == 0 && dest.skipped == 0 && !dest.failing() && !dest.unresolved && !dest.inReady {
		delete(st.dests, dest.key)
		delete(st.waiting, dest)
	}
}

// sweepIdle drops destinations that hold and skip nothing but still
// carry a failure, once their topic is gone from the local replica, so
// deleting topics whose owners were failing cannot grow the map. A live
// topic's destination keeps its failure: that is what makes its next
// record a probe rather than a full batch.
func (d *ProduceDispatcher) sweepIdle(st *produceDispatchState) {
	if d.store == nil {
		return
	}
	for key, dest := range st.dests {
		if dest.held > 0 || dest.skipped > 0 || dest.inReady || dest.inflight {
			continue
		}
		if d.topicInfo(st, key.topic).exists {
			continue
		}
		delete(st.dests, key)
		delete(st.waiting, dest)
	}
}

// perDestCap is the most records one destination may queue for its next
// commit: a quarter of the window, and never less than
// produceDispatchBaseWindow (a quarter of the BatchSize cap when that is
// smaller). A latency-bound destination commits one queue per round
// trip, so a lone hot partition gets as much per commit as the
// pass-based dispatcher gave it.
func (d *ProduceDispatcher) perDestCap(st *produceDispatchState) int {
	return max(st.windowLimit/4, min(produceDispatchBaseWindow, d.batchSize/4), 1)
}

// holdLimit is how many records may be held (queued or in flight, slow
// commits aside) before the reader stops: the window, but at least four
// destinations' caps, within the BatchSize cap. One destination holds at
// most two caps (a batch in flight and a full queue), so a hot or slow
// partition never takes more than half of it and the others' records
// keep being read while its commit runs.
func (d *ProduceDispatcher) holdLimit(st *produceDispatchState) int {
	return min(max(st.windowLimit, 4*d.perDestCap(st)), max(d.batchSize, 1))
}

// read reads newly durable records, or, when a rescan is due, the
// skipped ones first, and places each. It stops at the durable frontier,
// the lookahead horizon, or once holdLimit records are held.
func (d *ProduceDispatcher) read(ctx context.Context, st *produceDispatchState) {
	now := d.now()
	durableNext := d.ingress.DurableProduceNext()
	limit := d.holdLimit(st)
	horizon := st.nextSeq + uint64(st.windowLimit)*produceDispatchLookaheadWindows

	start := st.readCursor
	rescan := false
	if st.skipped > 0 && (st.rescanDue || now.Sub(st.lastRescan) >= produceDispatchRescanInterval) {
		start, rescan = d.rescanStart(st)
	}
	st.rescanDue, st.rescanSet = false, false
	if !rescan && (st.readSeq >= durableNext || st.readSeq >= horizon || st.held-st.slowHeld >= limit) {
		return
	}
	if rescan {
		st.lastRescan = now
	}
	st.readEpoch++
	rescanFrom := start.Seq
	oldReadSeq := st.readSeq
	complete := true
	var stopped wal.Cursor

	peek := func(id wal.RecordID, _ wal.Cursor) (bool, error) {
		seq := id.Seq
		if seq < st.nextSeq {
			return true, nil
		}
		if seq >= durableNext || seq >= horizon || st.held-st.slowHeld >= limit {
			if seq < oldReadSeq {
				complete = false
				stopped = wal.Cursor{SegmentBase: id.SegmentBase, Offset: id.Offset, Seq: seq}
			}
			return false, errProduceReplayBoundary
		}
		if seq < st.readSeq {
			// Already read: only skipped records are read again.
			return st.marks.get(seq) != seqSkipped, nil
		}
		return false, nil
	}
	fn := func(rec ingress.ProduceRecord, next wal.Cursor) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		seq := rec.WAL.Seq
		orig := st.dest(dispatchDestKey{topic: rec.Topic, partition: rec.TargetPartition})
		if seq >= st.readSeq {
			st.marks.extendTo(seq)
			st.readSeq = seq + 1
			st.readCursor = next
			st.epochRecords++
			d.place(ctx, st, rec, orig, orig.skipped > 0, false, rescanFrom, now)
			st.forget(orig)
			return nil
		}
		// A skipped record read again. It stays behind an earlier
		// skipped record of its partition that this rescan re-skipped
		// or does not reach (below its start).
		orig.skipped--
		st.skipped--
		blocked := orig.blockedRead == st.readEpoch || (orig.skipped > 0 && orig.firstSkipped.Seq < rescanFrom)
		d.place(ctx, st, rec, orig, blocked, true, rescanFrom, now)
		st.forget(orig)
		return nil
	}
	err := d.ingress.ReplayProduceFromCursorPeek(start, peek, fn)
	if err != nil && !errors.Is(err, errProduceReplayBoundary) {
		st.noteErr(err)
		st.stalled = ctx.Err() == nil
		complete = false
	}
	if rescan && !complete && stopped.Seq > 0 {
		// Destinations the rescan did not finish with keep their
		// remaining skipped records past where it stopped.
		for _, dest := range st.dests {
			if dest.skipped > 0 && dest.blockedRead != st.readEpoch &&
				dest.firstSkipped.Seq >= rescanFrom && dest.firstSkipped.Seq < stopped.Seq {
				dest.firstSkipped = stopped
			}
		}
	}
	d.resizeWindow(st)
	d.logReroutes(st)
}

// rescanStart picks where a rescan starts: the lowest firstSkipped of
// the destinations that asked for one, or, on the backstop and in
// DispatchAvailable, of all of them.
func (d *ProduceDispatcher) rescanStart(st *produceDispatchState) (wal.Cursor, bool) {
	if st.rescanDue && st.rescanSet && !st.manual {
		return st.rescanFrom, true
	}
	found := false
	var low wal.Cursor
	for _, dest := range st.dests {
		if dest.skipped > 0 && (!found || dest.firstSkipped.Seq < low.Seq) {
			low, found = dest.firstSkipped, true
		}
	}
	return low, found
}

// requestRescan asks the next read to rescan from dest's first skipped
// record.
func (st *produceDispatchState) requestRescan(dest *dispatchDest) {
	if dest.skipped == 0 {
		return
	}
	if !st.rescanSet || dest.firstSkipped.Seq < st.rescanFrom.Seq {
		st.rescanFrom = dest.firstSkipped
		st.rescanSet = true
	}
	st.rescanDue = true
}

// place decides what happens to one record just read: done, held on a
// destination's queue, or skipped. blocked says an earlier record of
// the same partition is still skipped, which keeps this one behind it;
// reseen that a rescan is reading it again.
func (d *ProduceDispatcher) place(ctx context.Context, st *produceDispatchState, rec ingress.ProduceRecord, orig *dispatchDest, blocked, reseen bool, rescanFrom uint64, now time.Time) {
	skip := func() { d.skip(st, rec, orig, reseen, rescanFrom) }
	if blocked {
		skip()
		return
	}
	switch d.incarnationState(ctx, st, rec) {
	case incarnationGone:
		d.logger.Warn("discarding undispatched record of a deleted topic incarnation",
			"topic", rec.Topic, "topic_id", rec.TopicID, "partition", rec.TargetPartition, "seq", rec.WAL.Seq)
		st.marks.set(rec.WAL.Seq, seqDone)
		return
	case incarnationUnconfirmed:
		skip()
		return
	}

	if _, err := d.dispatchTarget(rec); err != nil {
		if d.topicDeleted(ctx, st, rec.Topic) {
			d.logger.Warn("discarding undispatched record for deleted topic",
				"topic", rec.Topic, "partition", rec.TargetPartition,
				"seq", rec.WAL.Seq, "err", err)
			st.marks.set(rec.WAL.Seq, seqDone)
			return
		}
		if alt, ok := d.rerouteFor(st, orig.key); ok {
			d.reroute(st, rec, orig, alt)
			return
		}
		orig.unresolved = true
		st.noteErr(err)
		if orig.held > 0 {
			skip()
			return
		}
		d.hold(st, orig, rec, orig.key.partition)
		return
	}
	orig.unresolved = false

	if orig.failing() {
		// Commits to this destination keep failing. Past the grace,
		// its records go to a sibling while a probe still tries it
		// once per backoff. Within the grace, or with no sibling, it
		// holds a single probe and leaves the rest in the WAL.
		if now.Sub(orig.failingSince) >= produceDispatchRerouteGrace && (orig.inflight || !d.retryDue(st, orig, now)) {
			if alt, ok := d.rerouteFor(st, orig.key); ok {
				d.reroute(st, rec, orig, alt)
				return
			}
		}
		if orig.held > 0 {
			skip()
			return
		}
		d.hold(st, orig, rec, orig.key.partition)
		return
	}
	if orig.slow || len(orig.queue) >= d.perDestCap(st) {
		// Nothing queues behind a commit that has stopped answering:
		// the window stays free for everyone else until it lands.
		skip()
		return
	}
	d.hold(st, orig, rec, orig.key.partition)
}

// hold queues rec on dest; origPartition is the partition it was
// accepted for.
func (d *ProduceDispatcher) hold(st *produceDispatchState, dest *dispatchDest, rec ingress.ProduceRecord, origPartition int) {
	st.marks.set(rec.WAL.Seq, seqHeld)
	if len(dest.queue) == 0 {
		dest.queuedAt = d.now()
	}
	dest.queue = append(dest.queue, rec)
	dest.origs = append(dest.origs, origPartition)
	dest.held++
	st.held++
	st.epochDests[dest.key] = struct{}{}
	st.enqueue(dest)
}

// reroute holds rec on the sibling partition alt instead of orig.
func (d *ProduceDispatcher) reroute(st *produceDispatchState, rec ingress.ProduceRecord, orig, alt *dispatchDest) {
	rec.TargetPartition = alt.key.partition
	d.hold(st, alt, rec, orig.key.partition)
	if st.rerouted == nil {
		st.rerouted = map[dispatchDestKey]rerouteNote{}
	}
	note := st.rerouted[orig.key]
	note.to = alt.key.partition
	note.records++
	st.rerouted[orig.key] = note
}

// skip leaves a record just read in the WAL, accounted to orig, the
// partition it was accepted for.
func (d *ProduceDispatcher) skip(st *produceDispatchState, rec ingress.ProduceRecord, orig *dispatchDest, reseen bool, rescanFrom uint64) {
	at := walCursorAt(rec.WAL)
	st.marks.set(at.Seq, seqSkipped)
	switch {
	case orig.skipped == 0:
		orig.firstSkipped = at
	case reseen && orig.blockedRead != st.readEpoch && orig.firstSkipped.Seq >= rescanFrom:
		// The first record of orig this rescan skips again, with all
		// of orig's skipped records in its range: the rescan reads in
		// order, so it is the lowest one left.
		orig.firstSkipped = at
	case at.Seq < orig.firstSkipped.Seq:
		orig.firstSkipped = at
	}
	orig.skipped++
	st.skipped++
	if reseen {
		orig.blockedRead = st.readEpoch
	}
}

// release returns a held record to the WAL: it is marked skipped,
// accounted to orig, and read again by a later rescan.
func (st *produceDispatchState) release(rec ingress.ProduceRecord, orig *dispatchDest) {
	at := walCursorAt(rec.WAL)
	st.marks.set(at.Seq, seqSkipped)
	if orig.skipped == 0 || at.Seq < orig.firstSkipped.Seq {
		orig.firstSkipped = at
	}
	orig.skipped++
	st.skipped++
}

// walCursorAt is the cursor a replay starts from to read the record id
// itself.
func walCursorAt(id wal.RecordID) wal.Cursor {
	return wal.Cursor{SegmentBase: id.SegmentBase, Offset: id.Offset, Seq: id.Seq}
}

// enqueue puts dest on the ready list if it has queued records and
// nothing in flight.
func (st *produceDispatchState) enqueue(dest *dispatchDest) {
	if dest.inReady || dest.inflight || len(dest.queue) == 0 {
		return
	}
	if _, waiting := st.waiting[dest]; waiting {
		return
	}
	dest.inReady = true
	st.ready = append(st.ready, dest)
}

// resizeWindow adapts the window to the fan-out seen: it grows as soon
// as more destinations show up than it has room for, and is re-sized
// (possibly smaller) once a whole window of records has been read.
func (d *ProduceDispatcher) resizeWindow(st *produceDispatchState) {
	target := d.clampWindow(produceDispatchTargetPerPartition * len(st.epochDests))
	if target > st.windowLimit {
		st.windowLimit = target
	}
	if st.epochRecords >= st.windowLimit {
		if len(st.epochDests) > 0 {
			st.windowLimit = target
		}
		clear(st.epochDests)
		st.epochRecords = 0
	}
}

// logReroutes logs the reroutes of the last read, one line per partition.
func (d *ProduceDispatcher) logReroutes(st *produceDispatchState) {
	for key, note := range st.rerouted {
		d.logger.Warn("rerouting produce records for unavailable partition owner",
			"topic", key.topic, "from_partition", key.partition,
			"to_partition", note.to, "records", note.records)
	}
	clear(st.rerouted)
}

// advanceCheckpoint moves the checkpoint over the done seqs at its
// front, stores it, and compacts the WAL behind the stored value. A
// failed store is retried by the next call; the in-memory checkpoint
// does not wait for it (nothing is read below it again) and compaction
// stays behind the last value that was written. Compaction stops at the
// last synced checkpoint, so a pass whose checkpoint did not move still
// compacts until it has caught up with the stored value; once it has,
// such a pass does nothing.
func (d *ProduceDispatcher) advanceCheckpoint(st *produceDispatchState) error {
	st.nextSeq += st.marks.popDone()
	if st.nextSeq == st.storedSeq && st.compactedSeq >= st.storedSeq {
		return nil
	}
	if st.nextSeq != st.storedSeq {
		if err := d.ingress.StoreProduceCheckpoint(st.nextSeq); err != nil {
			return err
		}
		st.storedSeq = st.nextSeq
	}
	to, err := d.ingress.CompactProduceBefore(st.storedSeq)
	if err != nil {
		// Not reached: the next pass (stalled, so at the failure backoff)
		// tries again.
		return err
	}
	st.compactedSeq = max(st.compactedSeq, to)
	return nil
}
