package cluster

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

var errProduceReplayBoundary = errors.New("produce replay reached durable boundary")

// dispatchWindow is the outcome of one scan pass: the records that still need
// committing plus the bookkeeping the checkpoint advance needs afterwards.
type dispatchWindow struct {
	// records are the window's not-yet-committed records, in WAL-seq order.
	records []ingress.ProduceRecord
	// scanStart and scanEnd bound the seqs examined this pass; scanned is
	// false when the replay saw nothing at or above the checkpoint.
	scanStart uint64
	scanEnd   uint64
	scanned   bool
	// done marks scanned seqs that need no further commit work: committed
	// (this pass or a previous one), discarded for a deleted topic, or
	// successfully rerouted.
	done map[uint64]bool
	// cursorAfterSeq records the resume cursor sitting just after each
	// scanned seq, so the next pass can resume from the new checkpoint.
	cursorAfterSeq map[uint64]wal.Cursor
}

// dispatch drains up to the adaptive window (state.windowLimit) of
// not-yet-committed records from the ingress WAL, groups them by destination
// (topic,partition,owner), commits the groups as large per-partition batches
// concurrently, then advances the checkpoint to the lowest WAL seq not
// yet durably committed and compacts up to it.
//
// Grouping is the throughput lever: the WAL interleaves every partition, so
// flushing on each target change produced ~1-record batches (one fsync
// each). Bucketing a whole window by partition turns N interleaved records
// into a handful of large batches — one fsync per batch — committed in
// parallel across partitions and owners. The window grows with the observed
// fan-out (see windowLimit) so per-partition batches stay fat even when
// hundreds of partitions interleave, keeping the fsync count low.
//
// Checkpoint = the first scanned seq that did not durably commit (records
// discarded for a deleted topic count as done). Compaction never deletes WAL
// records past the checkpoint, so a buffered-but-uncommitted record always
// survives a crash.
//
// A record whose destination cannot take commits must not block delivery:
// because every destination shares this one WAL, a single dead partition
// owner would otherwise stop newer records for ALL topics and partitions
// while producers keep getting 2xx. The dispatcher therefore REROUTES such
// records to another partition of the same topic whose owner is alive —
// deliberately sacrificing per-key partition ordering to preserve
// availability, the exact trade the accept path already makes when it skips
// dead-owner partitions at partition-selection time
// (messaging.Engine.pickProducePartition). Two tiers trigger a reroute:
//
//   - target resolution fails for a live topic (owner dead or missing per
//     membership): membership death is authoritative, so the record is
//     rerouted immediately, with the same authority as the accept-time skip;
//   - the commit RPC itself keeps failing while membership still says the
//     owner is alive: a single failed pass is a transient blip and retries
//     on the original partition, but after a destination has stayed stuck
//     for produceDispatchRerouteAfterPasses consecutive passes it is treated
//     as dead and its records are rerouted too. The commit attempt that
//     keeps failing doubles as the recovery probe, so rerouting is per-pass,
//     never sticky: one successful commit sends new records back to their
//     original partition.
//
// A successfully rerouted record counts as done — the checkpoint advances
// past it exactly as if it had committed to its original partition.
//
// Only when NO live-owner partition of the topic exists does a record stay
// stuck and pin the checkpoint, and only then does the bounded skip-ahead
// matter: the window admits up to windowLimit records that still need
// committing, scanning at most windowLimit*produceDispatchLookaheadWindows
// seqs above the checkpoint (the lookahead horizon). Within the horizon:
//
//   - seqs already committed on an earlier pass (state.committedAhead) are
//     counted done and skipped without consuming window budget, so they are
//     never re-committed and never crowd out fresh records;
//   - destinations that failed on the previous pass (state.stuck) and have
//     nowhere to reroute contribute only their first record as a recovery
//     probe, so a dead owner's growing backlog cannot re-fill the window
//     either.
//
// Records beyond the horizon stay frozen until the stuck record clears —
// the deliberate memory/scan bound: committedAhead and per-pass scan work
// never exceed the horizon. In steady state each record is delivered exactly
// once; delivery degrades to at-least-once on two paths:
//
//   - crash replay: the committedAhead set is in-memory only, so a process
//     crash replays committed-ahead records;
//   - commit-RPC timeout: remote commits carry no idempotency token on the
//     wire, so a commit that exceeds produceCommitRPCTimeout yet succeeds
//     on the remote is retried (and, once the destination has been stuck
//     for produceDispatchRerouteAfterPasses, rerouted) — duplicating the
//     batch. The generous timeout makes this rare, not impossible.
func (d *ProduceDispatcher) dispatch(ctx context.Context, state *produceDispatchState) (int, error) {
	limit := state.windowLimit
	if limit <= 0 {
		limit = d.clampWindow(produceDispatchBaseWindow)
		state.windowLimit = limit
	}
	durableNext := d.ingress.DurableProduceNext()
	if state.nextSeq >= durableNext {
		return 0, nil
	}

	win, err := d.scanWindow(ctx, state, limit, durableNext)
	if err != nil {
		return 0, err
	}
	if !win.scanned {
		return 0, nil
	}

	buckets, nextStuck, firstErr := d.bucketByTarget(ctx, &win, state)

	// Size the next window to this pass's fan-out: aim for ~target records per
	// distinct partition so per-partition commit batches (one fsync each) stay
	// fat no matter how many partitions the WAL interleaves. Converges in one
	// pass — the base window already samples enough records to see the spread.
	if n := len(buckets); n > 0 {
		state.windowLimit = d.clampWindow(produceDispatchTargetPerPartition * n)
	}

	// Commit the buckets concurrently. Different partitions use different
	// logs/locks (safe to parallelise); each bucket keeps WAL-seq order so
	// per-partition offsets stay monotonic. Destinations whose bucket fails
	// are marked stuck for the next pass; a destination that has stayed stuck
	// for produceDispatchRerouteAfterPasses consecutive passes has its failed
	// records rerouted to a live-owner partition instead of pinning the
	// checkpoint.
	failed, commitErr := d.commitBuckets(ctx, buckets, win.done)
	if commitErr != nil && firstErr == nil {
		firstErr = commitErr
	}
	d.rerouteFailedBuckets(ctx, failed, state.stuck, nextStuck, win.done)
	state.stuck = nextStuck

	return d.advanceCheckpoint(state, win, firstErr)
}

// scanWindow drains up to limit records that still need committing (no
// commits yet), recording the resume cursor that sits just after each scanned
// record. Already-committed seqs are marked done without consuming the window
// budget; records of known-stuck destinations beyond their probe are skipped
// entirely. The scan never looks past the lookahead horizon, bounding both
// the work per pass and the skip-set size.
func (d *ProduceDispatcher) scanWindow(ctx context.Context, state *produceDispatchState, limit int, durableNext uint64) (dispatchWindow, error) {
	scanHorizon := state.nextSeq + uint64(limit)*produceDispatchLookaheadWindows
	win := dispatchWindow{
		done:           make(map[uint64]bool),
		cursorAfterSeq: make(map[uint64]wal.Cursor),
	}
	probed := make(map[produceDispatchStuckKey]bool, len(state.stuck))
	rerouteReady := make(map[produceDispatchStuckKey]bool, len(state.stuck))
	err := d.ingress.ReplayProduceFromCursor(state.cursor, func(record ingress.ProduceRecord, cursor wal.Cursor) error {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		seq := record.WAL.Seq
		if seq < state.nextSeq {
			return nil
		}
		if seq >= durableNext || seq >= scanHorizon || len(win.records) >= limit {
			return errProduceReplayBoundary
		}
		if !win.scanned {
			win.scanStart = seq
			win.scanned = true
		}
		win.scanEnd = seq + 1
		win.cursorAfterSeq[seq] = cursor
		// Already committed on an earlier pass but held above the
		// checkpoint by a lower stuck seq: count it done, never re-commit,
		// and keep looking for fresh records.
		if state.committedAhead[seq] {
			win.done[seq] = true
			return nil
		}
		if len(state.stuck) > 0 {
			key := produceDispatchStuckKey{topic: record.Topic, partition: record.TargetPartition}
			if passes := state.stuck[key]; passes > 0 {
				// A destination stuck long enough to reroute flows freely
				// only when a live-owner sibling partition actually exists;
				// otherwise probe-only admission keeps its backlog from
				// re-filling the window.
				canFlow := false
				if passes >= produceDispatchRerouteAfterPasses {
					ready, checked := rerouteReady[key]
					if !checked {
						_, ready = d.rerouteTarget(key.topic, key.partition, state.stuck, nil)
						rerouteReady[key] = ready
					}
					canFlow = ready
				}
				if !canFlow {
					if probed[key] {
						// Beyond the destination's recovery probe: leave it
						// for a later pass without spending window budget.
						return nil
					}
					probed[key] = true
				}
			}
		}
		win.records = append(win.records, record)
		return nil
	})
	if errors.Is(err, errProduceReplayBoundary) {
		err = nil
	}
	if err != nil {
		return dispatchWindow{}, err
	}
	return win, nil
}

// bucketByTarget resolves each window record's target and buckets it by
// destination. A record whose topic is gone from the local replica is
// discarded (done). A target that cannot be resolved for a still-live topic
// (its owner is dead or missing per membership) is rerouted to a live-owner
// partition of the same topic; only when no such partition exists is it left
// uncommitted, bounding the checkpoint and marking its destination stuck for
// the next pass. Returns the buckets, the next pass's stuck set (so far), and
// the first resolution error.
func (d *ProduceDispatcher) bucketByTarget(ctx context.Context, win *dispatchWindow, state *produceDispatchState) (map[produceDispatchTarget][]ingress.ProduceRecord, map[produceDispatchStuckKey]int, error) {
	buckets := make(map[produceDispatchTarget][]ingress.ProduceRecord)
	nextStuck := make(map[produceDispatchStuckKey]int)
	rerouted := make(map[produceDispatchStuckKey]int)
	reroutedTo := make(map[produceDispatchStuckKey]int)

	// Memoize rerouteTarget per destination for this pass (the scan phase's
	// rerouteReady memo is the same pattern): every rerouteTarget call is an
	// uncached store.GetTopic (bbolt tx + JSON unmarshal), and the first
	// pass after an owner dies can admit the full window of one partition's
	// records — thousands of redundant lookups without the memo. Caveat: a
	// fresh call filters candidates against nextStuck, which grows as this
	// loop marks other destinations stuck, so a memoized answer can point at
	// a partition that became stuck later in the SAME pass. That is
	// acceptable — all records of one destination get the same answer within
	// a pass anyway, and a commit to a bad alternative just fails and marks
	// it stuck for the next pass.
	type rerouteResult struct {
		target produceDispatchTarget
		ok     bool
	}
	rerouteMemo := make(map[produceDispatchStuckKey]rerouteResult)
	rerouteTargetFor := func(key produceDispatchStuckKey) (produceDispatchTarget, bool) {
		if res, cached := rerouteMemo[key]; cached {
			return res.target, res.ok
		}
		target, ok := d.rerouteTarget(key.topic, key.partition, state.stuck, nextStuck)
		rerouteMemo[key] = rerouteResult{target: target, ok: ok}
		return target, ok
	}

	var firstErr error
	for _, rec := range win.records {
		target, err := d.dispatchTarget(rec)
		if err == nil {
			buckets[target] = append(buckets[target], rec)
			continue
		}
		if d.topicConfirmedDeleted(ctx, rec.Topic) {
			d.logger.Warn("discarding undispatched record for deleted topic",
				"topic", rec.Topic, "partition", rec.TargetPartition,
				"seq", rec.WAL.Seq, "err", err)
			win.done[rec.WAL.Seq] = true
			continue
		}
		key := produceDispatchStuckKey{topic: rec.Topic, partition: rec.TargetPartition}
		if alt, ok := rerouteTargetFor(key); ok {
			rec.TargetPartition = alt.partition
			buckets[alt] = append(buckets[alt], rec)
			rerouted[key]++
			reroutedTo[key] = alt.partition
			continue
		}
		nextStuck[key] = state.stuck[key] + 1
		if firstErr == nil {
			firstErr = err
		}
	}
	for key, count := range rerouted {
		d.logger.Warn("rerouting produce records for dead partition owner",
			"topic", key.topic, "from_partition", key.partition,
			"to_partition", reroutedTo[key], "records", count)
	}
	return buckets, nextStuck, firstErr
}

// advanceCheckpoint moves the checkpoint to the first scanned seq that is not
// done, persists it, prunes the skip-set, and compacts the WAL behind it.
// firstErr is threaded through so a partially failed pass still reports its
// original cause alongside any checkpoint/compaction failure.
func (d *ProduceDispatcher) advanceCheckpoint(state *produceDispatchState, win dispatchWindow, firstErr error) (int, error) {
	checkpointSeq := win.scanEnd
	for s := win.scanStart; s < win.scanEnd; s++ {
		if !win.done[s] {
			checkpointSeq = s
			break
		}
	}

	// Merge this pass's committed seqs into the carried-forward skip set.
	// Seqs below the checkpoint are pruned only AFTER the checkpoint is
	// durably stored: if the store fails, the next pass replays from the
	// old checkpoint and must still skip everything that already committed,
	// or the whole window would be re-committed as duplicates.
	ahead := make(map[uint64]bool, len(state.committedAhead)+len(win.done))
	for s := range state.committedAhead {
		ahead[s] = true
	}
	for s := range win.done {
		ahead[s] = true
	}
	state.committedAhead = ahead

	processed := int(checkpointSeq - win.scanStart)
	if processed <= 0 {
		return 0, firstErr
	}

	nextCursor := state.cursor
	if c, ok := win.cursorAfterSeq[checkpointSeq-1]; ok {
		nextCursor = c
	}
	if checkpointErr := d.ingress.StoreProduceCheckpoint(checkpointSeq); checkpointErr != nil {
		return processed, errors.Join(firstErr, checkpointErr)
	}
	// The checkpoint is durable: drop skip-set entries it has passed — the
	// next pass starts at checkpointSeq and never re-reads them.
	for s := range ahead {
		if s < checkpointSeq {
			delete(ahead, s)
		}
	}
	state.nextSeq = checkpointSeq
	state.cursor = nextCursor
	if compactErr := d.ingress.CompactProduceBefore(checkpointSeq); compactErr != nil {
		return processed, errors.Join(firstErr, compactErr)
	}
	return processed, firstErr
}
