package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const minTimerFlushAge = time.Second

// fsyncHook, when non-nil, runs on the flusher goroutine right before
// every segment fdatasync; a non-nil result is treated as the fsync's
// error. Nil in production; tests use it to inject a sync failure at
// the exact point of the commit protocol where it would happen.
var fsyncHook func(*segment) error

// flushPassHook, when non-nil, runs at the top of every flusher pass.
// Nil in production; tests use it to count passes, which is the only
// way to assert the property the lazy timer exists for — that an idle
// log runs NO passes at all, rather than cheap ones.
var flushPassHook func()

// flusher is the single goroutine that drains a Log's buffer to the
// active segment file. "Single writer per partition" lives here.
type flusher struct {
	log *Log

	wakeup chan struct{}
	// armReq asks for the timer back WITHOUT asking for a drain. Two
	// senders: notePush when a record enters an empty buffer, and
	// noteHighWatermarkAdvance when the visible boundary moves ahead of
	// the persisted one. Both mean "scheduling is owed", never "here is
	// work to do now" -- needsTimer decides what, if anything, is due.
	armReq   chan struct{}
	syncReqs chan commitRequest
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	interval time.Duration

	mu       *sync.RWMutex
	lastSync time.Time
	// unsyncedBytes counts segment bytes written but not yet fsynced.
	// Written only by the flusher goroutine (writeFrame, syncIfNeeded,
	// discardUncommittedTail) and read from other goroutines through
	// needsTimer's pollers, hence atomic; f.mu does not govern it.
	unsyncedBytes atomic.Int64

	// rollPending marks the active segment as full: it was fsynced when
	// it crossed SegmentBytes, and it is sealed by the next roll, which
	// happens at the end of the next successful commit, at Close, or
	// right before the next frame write, whichever comes first. Never
	// rolling inside a commit before that commit has returned keeps the
	// commit's frames in the active segment, where a failed commit can
	// still truncate them (see discardUncommittedTail); a sealed segment
	// is never touched.
	rollPending bool

	// hwmForce is a per-drain flag: set when this drain must persist the
	// high-watermark unconditionally (a forced sync, a segment roll, or
	// SyncPerWrite). Reset at the start of every drainOnce. A commit's
	// drain never persists it (see CommitDurable), forced or not.
	hwmForce bool

	// closeErr records the final shutdown drain's error so Close can
	// surface it. Written only by run() before done closes; read only
	// after waitDone returns.
	closeErr error
}

// commitRequest is one synchronous flush+fsync request handed to the
// flusher goroutine. A plain Sync carries hwm < 0; a commit carries the
// offset range to verify and the high-watermark to advance to once the
// range is proven durable; rotate asks for the active segment to be
// sealed after the drain (the reaper's time-based rotation).
type commitRequest struct {
	done   chan error
	first  int64
	last   int64
	hwm    int64
	verify bool
	rotate bool
}

// isCommit reports whether the request exposes records: only then does
// a failure discard the uncommitted tail. A plain Sync or a rotation
// leaves the snapshot in place for a later retry.
func (r *commitRequest) isCommit() bool { return r != nil && r.hwm >= 0 }

func newFlusher(log *Log, mu *sync.RWMutex, interval time.Duration) *flusher {
	return &flusher{
		log:      log,
		wakeup:   make(chan struct{}, 1),
		armReq:   make(chan struct{}, 1),
		syncReqs: make(chan commitRequest),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		interval: interval,
		mu:       mu,
		lastSync: time.Now(),
	}
}

// Sync synchronously drains the write buffer to the active segment and
// fsyncs it, blocking until the data is durable on disk. Use it when a
// record must be durable but visibility is managed by the caller (tests,
// transfers). The commit path uses CommitDurable instead so the
// high-watermark advance and its persistence ride the same drain.
// Returns ErrLogClosed if the log is closing.
func (l *Log) Sync() error {
	return l.submitCommit(commitRequest{hwm: -1})
}

// CommitDurable is the owner-side durability boundary for the records at
// offsets [first, last]. On the flusher goroutine it: drains and writes
// any buffered records, fdatasyncs the active segment, re-reads the
// frames covering [first, last] to validate their on-disk CRC (unless
// the log was opened with DisableCommitVerify), and advances the
// high-watermark to last+1 so the records become visible.
//
// After CommitDurable returns, a restart recovers a high-watermark of at
// least last+1: recovery rebuilds the boundary from the CRC-verified
// record tail (see loadHighWatermark), and the records were fsynced
// before they became visible. So the commit does not also fsync the
// boundary file, which used to be a second serial fsync on every commit
// under the produce lock; the file is persisted lazily (HWMSyncInterval)
// and exactly at Close, for readers of a closed log.
//
// A failed commit leaves no trace of the batch: every record above the
// high-watermark (the batch, anything appended after it, and any earlier
// uncommitted tail in the active segment) is discarded from memory and
// truncated from the active segment before the error is returned, and
// the next append is assigned the offset the batch had. The caller owns
// the records (the ingress WAL still holds them) and re-appends them on
// retry, so the retry lands exactly one copy instead of committing a
// hidden first copy next to a second one. See discardUncommittedTail.
//
// A CRC mismatch is returned as a VerifyError so callers can classify it
// separately from a write or sync failure. Returns ErrLogClosed if the
// log is closing and ErrLogPoisoned (wrapping the original fsync error)
// once an fsync has failed on this log.
func (l *Log) CommitDurable(first, last int64) error {
	if last < first {
		return nil
	}
	return l.submitCommit(commitRequest{
		first:  first,
		last:   last,
		hwm:    last + 1,
		verify: !l.opts.DisableCommitVerify,
	})
}

func (l *Log) submitCommit(req commitRequest) error {
	if l.closed.Load() {
		return ErrLogClosed
	}
	req.done = make(chan error, 1)
	select {
	case l.flusher.syncReqs <- req:
		return <-req.done
	case <-l.flusher.done:
		return ErrLogClosed
	}
}

func (f *flusher) signal() {
	select {
	case f.wakeup <- struct{}{}:
	default:
	}
}

// notePush tells the flusher what an append just did to the buffer.
//
// A crossed threshold is work owed now, and wakes the flusher to drain.
// A record landing in an empty buffer is not work owed now: its
// durability rides on the age-based flush, so all it needs is for the
// timer to be armed again. Waking with a forced drain there would turn
// the first append after every drain into its own one-record write and
// destroy batching, which is why the two are separate channels rather
// than one.
//
// Both sends are non-blocking into capacity-one channels. Dropping a
// duplicate is safe because the flusher re-reads the whole buffer and
// snapshot state on every wake: a signal says "look again", never "here
// is the work". What must never happen is a push whose signal is
// dropped while the flusher is on its way to sleep, and that cannot
// happen: the flusher consumes a channel's value BEFORE doing the pass
// that decides whether to disarm, so a push after that consume always
// finds the channel empty and its send always lands.
func (f *flusher) notePush(crossed, wasEmpty bool) {
	if crossed {
		f.signal()
		return
	}
	if wasEmpty {
		f.requestArm()
	}
}

// requestArm asks the flusher for its timer back. Non-blocking into a
// capacity-one channel, so a duplicate is dropped; that is safe because
// the flusher treats the signal as "look again" and re-reads state via
// needsTimer, which is the only source of truth about what is owed.
func (f *flusher) requestArm() {
	if f == nil {
		return
	}
	select {
	case f.armReq <- struct{}{}:
	default:
	}
}

// noteHighWatermarkAdvance asks for the timer back after the visible
// boundary moved ahead of the persisted one.
//
// Like notePush's empty-buffer case this asks for scheduling, not for
// work: syncHighWatermark decides for itself whether HWMSyncInterval
// has elapsed. Safe to call from any goroutine and before the flusher
// has started.
func (f *flusher) noteHighWatermarkAdvance() {
	f.requestArm()
}

// needsTimer reports whether a periodic pass is still owed, and so
// whether the timer has to stay armed. It is the flusher goroutine's
// post-pass arming decision, and it is also polled from test
// goroutines, so every field it reads must be safe to read from another
// goroutine: buffer.pending() and hasPendingFlushing() take their own
// mutexes, and the rest are atomics. A fifth condition has to satisfy
// that same constraint.
//
// These four conditions are exactly what the old always-armed timer
// existed to service; there is deliberately no fifth. A pending segment
// roll is NOT one of them: rollIfPending runs only for a commit or the
// shutdown drain (see drainOnce), and any roll the timer would have
// missed is taken by activeForWrite before the next frame is written.
// The reaper's time-based rotation arrives as its own commit request,
// not on this timer.
func (f *flusher) needsTimer() bool {
	owed, _ := f.owedPass()
	return owed
}

// owedPass is needsTimer's decision, also reporting whether the deferred
// high-watermark persist is the only thing owed (hwmOnly), in which case
// the pass is not due before HWMSyncInterval (see rearm).
func (f *flusher) owedPass() (owed, hwmOnly bool) {
	l := f.log
	switch {
	case l.poisoned() != nil:
		// A poisoned log can never make progress: every pass returns at
		// drainOnce's poison check before touching anything. So nothing is
		// owed, because nothing CAN be done until the owner reopens the
		// log.
		//
		// This case has to come first, because the unsynced counter below
		// is latched above zero on a poisoned log forever: the fsync that
		// poisoned it returned through poison() without clearing the
		// counter, and no later pass gets far enough to clear it. Without
		// this the timer would stay armed for the life of the process,
		// running no-op passes ten times a second on every partition at
		// once after a disk-level failure, which is the exact load this
		// change exists to remove and the worst moment to add it.
		return false, false
	case l.buffer.pending():
		// Records waiting on the age-based flush (shouldFlushByAge).
		return true, false
	case f.unsyncedBytes.Load() > 0:
		// Bytes in the segment file that SyncInterval still owes an
		// fsync.
		return true, false
	case l.hasPendingFlushing():
		// An earlier writeBatch failed; the retry rides the timer.
		return true, false
	case l.highWatermark.Load() > l.persistedHWM.Load():
		// syncHighWatermark deferred the persist behind HWMSyncInterval.
		// Every commit leaves this owed.
		return true, true
	}
	return false, false
}

// hwmPersistDeadline is when syncHighWatermark stops deferring an
// unforced persist: HWMSyncInterval after the last one.
func (f *flusher) hwmPersistDeadline() time.Time {
	l := f.log
	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()
	return l.lastHWMSync.Add(l.opts.HWMSyncInterval)
}

// run is the flusher goroutine.
//
// The timer is armed only while a pass is actually owed. An idle log
// used to wake ten times a second forever to discover it had nothing to
// do; at tens of thousands of open partitions that was the single
// largest source of background CPU on a node, most of it inside the
// runtime's timer heap rather than in this package at all. Now a log
// with an empty buffer, nothing unsynced, no failed write to retry and
// its high-watermark already on disk holds no timer and costs nothing
// until a producer or a commit reaches it.
//
// Disarming is expressed as a nil timerC: a receive from a nil channel
// blocks forever, so that select arm simply stops existing.
func (f *flusher) run() {
	defer close(f.done)

	var timer *time.Timer
	var timerC <-chan time.Time
	var armedFor time.Time // when the armed timer fires
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	// rearm re-evaluates whether a periodic pass is still owed and arms
	// or disarms accordingly. Called after EVERY pass, so the decision is
	// always made against post-pass state.
	rearm := func() {
		// Consume a pending arm request BEFORE deciding, so a token that
		// is already satisfied by this decision cannot cause a second,
		// redundant wake. Every commit produces one, via
		// AdvanceHighWatermark, and without this each commit paid an
		// extra select round plus a needsTimer evaluation and a timer
		// stop/reset.
		//
		// Order matters and is the safe one: draining first means
		// needsTimer below is evaluated AFTER the drain, so a push that
		// lands during the evaluation either is seen by needsTimer or
		// leaves a fresh token behind. Draining afterwards could discard
		// a token whose record the evaluation had already missed.
		select {
		case <-f.armReq:
		default:
		}
		stopTimer := func() {
			if timer != nil && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		owed, hwmOnly := f.owedPass()
		if !owed {
			stopTimer()
			timerC = nil
			return
		}
		// When only the deferred high-watermark persist is owed, which
		// every commit leaves behind, the pass is due exactly when
		// syncHighWatermark stops deferring: HWMSyncInterval after the
		// last persist. Firing every interval until then would wake each
		// partition that committed fifty times for one write, the idle
		// cost this lazy timer removed; and pushing it a full interval
		// out on every pass, as the other cases do, would starve it
		// under a steady stream of commits. Commits in a row keep the
		// same deadline, so the armed timer is left alone rather than
		// reset per commit.
		fireAt := time.Now().Add(f.interval)
		if hwmOnly {
			fireAt = f.hwmPersistDeadline()
			if timerC != nil && armedFor.Equal(fireAt) {
				return
			}
		}
		stopTimer()
		if timer == nil {
			timer = time.NewTimer(time.Until(fireAt))
		} else {
			timer.Reset(time.Until(fireAt))
		}
		timerC = timer.C
		armedFor = fireAt
	}

	rearm()

	for {
		forceDrain := false
		select {
		case <-f.wakeup:
			forceDrain = true
		case <-f.armReq:
			// Scheduling is owed, not necessarily work. Either a record
			// entered an empty buffer (nothing is due yet: it is younger
			// than timerFlushAge) or the high-watermark moved ahead of the
			// persisted one (a pass IS owed, and the timer is the right
			// place for it since syncHighWatermark defers behind
			// HWMSyncInterval anyway). rearm decides which via needsTimer.
			//
			// No pass runs inline either way. Draining here would do
			// nothing for the first case, and for both it would make the
			// flusher goroutine touch the log on every idle-to-active
			// transition, widening what runs concurrently with a commit
			// for no gain.
			rearm()
			continue
		case req := <-f.syncReqs:
			// Synchronous flush+fsync: drain the buffer and force an
			// fsync so the caller can treat the record as durable on
			// return. Stays on the single flusher goroutine so the
			// "one writer per partition" invariant holds.
			req.done <- f.drainOnce(true, true, &req)
			rearm()
			continue
		case <-timerC:
			// Spent: the next rearm must arm it again even for the same
			// deadline (a persist that failed keeps it).
			armedFor = time.Time{}
		case <-f.stop:
			f.closeErr = f.drainOnce(true, true, nil)
			return
		}
		_ = f.drainOnce(false, forceDrain, nil)
		rearm()
	}
}

// drainOnce is one flusher pass: write whatever needs writing, sync if
// required, run the commit's verify + high-watermark advance (if any),
// then persist the high-watermark if this pass owes it. A commit pass
// never does: see CommitDurable.
//
// Any failure of a commit request, at whatever step, discards the
// uncommitted tail before the error is reported (see CommitDurable).
func (f *flusher) drainOnce(forceSync, forceDrain bool, commit *commitRequest) error {
	if h := flushPassHook; h != nil {
		h()
	}
	f.hwmForce = forceSync

	if err := f.log.poisoned(); err != nil {
		// An earlier fsync failed: nothing this process writes to the
		// segment can be trusted until the log is reopened, so do not
		// even try. A commit's records are dropped from memory (the
		// caller re-appends after the reopen); the file is left alone.
		if commit.isCommit() {
			f.discardUncommittedTail(err, false)
		}
		return err
	}

	err := f.drainAndSync(forceSync, forceDrain)
	if err == nil && commit != nil {
		err = f.finishRequest(commit)
	}
	if err != nil {
		if commit.isCommit() {
			f.discardUncommittedTail(err, f.log.poisoned() == nil)
		}
		return err
	}
	if commit.isCommit() || commit == nil && forceSync {
		// The batch is committed (or this is the shutdown drain): a
		// full active segment can be sealed now. A failure here must not
		// fail the commit, which is already durable and visible: the
		// roll stays pending and is retried before the next write, where
		// its failure fails that commit before anything is written.
		f.rollIfPending()
	}
	if commit.isCommit() {
		// The batch is durable and visible, and a restart would recover
		// its boundary from the record tail. The boundary file is left to
		// the timer (armed by the advance, see rearm) and Close, so its
		// fsync never delays a commit and its failure never fails one.
		return nil
	}
	return f.log.syncHighWatermark(f.hwmForce)
}

// rollIfPending seals a full active segment outside any commit's
// critical path. Errors are logged, not returned: see drainOnce.
func (f *flusher) rollIfPending() {
	f.mu.RLock()
	pending := f.rollPending
	var active *segment
	if len(f.log.segments) > 0 {
		active = f.log.segments[len(f.log.segments)-1]
	}
	f.mu.RUnlock()
	if !pending || active == nil {
		return
	}
	if _, err := f.roll(active); err != nil {
		f.log.logger.Warn("storage: segment roll deferred", "dir", f.log.dir, "err", err)
	}
}

// drainAndSync is the write half of a pass: drain the buffer if the
// thresholds (or the caller) say so, write the frames, and sync when
// required.
func (f *flusher) drainAndSync(forceSync, forceDrain bool) error {
	// A pending flushing snapshot means an earlier writeBatch failed;
	// retry it on every drain regardless of buffer thresholds.
	if !forceDrain && !f.log.hasPendingFlushing() && !f.log.buffer.shouldFlushByAge(f.timerFlushAge()) {
		return f.syncIfNeeded(forceSync, nil)
	}
	records, baseOffset := f.log.drainBufferForFlush()
	if len(records) == 0 {
		return f.syncIfNeeded(forceSync, nil)
	}
	return f.writeBatch(records, baseOffset, forceSync)
}

// finishRequest runs the request-specific tail of a pass after the
// drain succeeded: the commit's verify and high-watermark advance, or
// the reaper's rotation.
func (f *flusher) finishRequest(commit *commitRequest) error {
	if commit.verify && commit.last >= commit.first {
		if verr := f.log.VerifyDurable(commit.first, commit.last); verr != nil {
			return VerifyError{First: commit.first, Last: commit.last, Err: verr}
		}
	}
	if commit.hwm >= 0 {
		if aerr := f.log.AdvanceHighWatermark(commit.hwm); aerr != nil {
			return aerr
		}
	}
	if commit.rotate {
		return f.rotateActive()
	}
	return nil
}

func (f *flusher) timerFlushAge() time.Duration {
	if f.log.opts.FlushBytes <= 0 && f.log.opts.FlushRecords <= 0 {
		return f.interval
	}
	age := f.interval * 10
	if age < minTimerFlushAge {
		return minTimerFlushAge
	}
	return age
}

// maxFrameInnerBytes caps the record payload packed into a single frame.
// decodeHeader rejects frames over maxFrameBytes as corrupt, so a larger
// frame would be written but never readable again (a poison frame). Half
// the read limit leaves ample headroom for codec expansion on
// incompressible payloads. Var (not const) only so tests can shrink it.
var maxFrameInnerBytes = maxFrameBytes / 2

// frameSplitLen returns how many leading records fit within
// maxFrameInnerBytes of encoded payload. Always at least 1, so a single
// oversized record surfaces as an encodeFrame error instead of looping.
func frameSplitLen(records [][]byte) int {
	size := 0
	for i, r := range records {
		size += 4 + len(r)
		if size > maxFrameInnerBytes && i > 0 {
			return i
		}
	}
	return len(records)
}

// writeBatch writes the drained batch as one or more frames, splitting
// so every frame stays under the decodeHeader read limit. Offsets stay
// continuous across the split frames.
func (f *flusher) writeBatch(records [][]byte, baseOffset int64, forceSync bool) error {
	cache := f.log.cacheWrittenFrames()
	for len(records) > 0 {
		n := frameSplitLen(records)
		if err := f.writeFrame(records[:n], baseOffset, forceSync, cache); err != nil {
			return err
		}
		baseOffset += int64(n)
		records = records[n:]
	}
	return nil
}

// maxCachedWriteFrameBytes bounds the frames writeFrame puts into the
// decoded-frame cache: a backlog drain can write frames of megabytes,
// and one of those would evict every frame the partition's consumers
// are reading.
const maxCachedWriteFrameBytes = maxDecodeCacheBytes / 8

// cacheWrittenFrames reports whether the batch about to be written also
// goes into the decoded-frame cache. Every frame a consumer reads at the
// frontier used to miss the cache, because the flusher dropped the
// in-memory records at fsync and the first readers woken by the commit
// then re-read and re-decoded the frame from the file, each of them.
// The records are immutable and exactly what the frame holds, so caching
// them as written is free of I/O. Only when something read the log since
// the last batch (readSeen), so partitions nobody reads keep empty
// caches; and only with the commit read-back on, so a record served from
// memory is one whose on-disk CRC the commit verified.
func (l *Log) cacheWrittenFrames() bool {
	return !l.opts.DisableCommitVerify && l.readSeen.CompareAndSwap(true, false)
}

func (f *flusher) writeFrame(records [][]byte, baseOffset int64, forceSync, cache bool) error {
	flushStart := time.Now()
	enc := frameEncoders.Get().(*frameEncoder)
	pos, n, active, err := f.encodeAndWrite(enc, records, baseOffset)
	// Nothing references the encoded frame once it is written, so the
	// buffers go back for the next frame, whichever flusher encodes it.
	frameEncoders.Put(enc)
	if err != nil {
		return err
	}

	entry := indexEntry{
		segmentBaseOffset: active.baseOffset,
		baseOffset:        baseOffset,
		recordCount:       int32(len(records)),
		framePos:          pos,
		frameLen:          int32(n),
	}
	f.mu.Lock()
	now := f.log.now()
	if active.firstWriteAt.IsZero() {
		active.firstWriteAt = now
	}
	active.lastWriteAt = now
	active.sizeBytes = pos + int64(n)
	active.nextOffset = baseOffset + int64(len(records))
	f.log.appendIndexLocked(entry)
	f.log.markFlushingWritten(active.nextOffset)
	if active.sizeBytes >= f.log.opts.SegmentBytes {
		f.rollPending = true
	}
	full := f.rollPending

	if m := f.log.opts.Metrics; m != nil {
		m.ObserveFlush(time.Since(flushStart), int64(n))
	}

	f.unsyncedBytes.Add(int64(n))
	f.mu.Unlock()

	// The exact position of the frame just written. The sparse index
	// keeps only an anchor every 32 KiB, so without this the commit's
	// read-back (and the first consumer) found the frame by walking the
	// whole navigation cache and then reading the previous frame's
	// header from the file. A failed commit that truncates the frame
	// drops the segment's cached positions (discardUncommittedTail), and
	// one that cannot truncate leaves the frame in place, so the entry
	// never outlives its bytes.
	f.log.navCache.put(entry)
	if cache {
		f.cacheWrittenFrame(entry, records)
	}

	// A full segment is synced now so the roll before the next write
	// finds it durable.
	return f.syncIfNeeded(forceSync || full, active)
}

// encodeAndWrite encodes records into a frame with enc and writes it at
// the end of the active segment, rolling first if the segment is due.
func (f *flusher) encodeAndWrite(enc *frameEncoder, records [][]byte, baseOffset int64) (int64, int, *segment, error) {
	frame, err := enc.encodeFrame(records, baseOffset, f.log.codec)
	if err != nil {
		return 0, 0, nil, err
	}

	active, err := f.activeForWrite()
	if err != nil {
		return 0, 0, nil, err
	}

	// The write syscall runs outside rwmu. This goroutine is the only
	// writer of the active segment, so its size and offset fields cannot
	// change underneath us; readers bound their scans by sizeBytes, which
	// is only advanced (under the write lock) after the bytes are in
	// place, so they never observe a partially written frame.
	pos, n, err := active.writeEncodedFrame(frame)
	if err != nil {
		return 0, 0, nil, err
	}
	return pos, n, active, nil
}

// cacheWrittenFrame puts the records of the frame just written into the
// decoded-frame cache (see cacheWrittenFrames). Only after the write and
// the index update succeeded, so a failed write never leaves an entry;
// a failed commit that truncates the frame drops the segment's cached
// frames with it (discardUncommittedTail). records aliases the drain's
// backing array, so the cache gets its own copy of the slice headers:
// caching the sub-slice would pin every record of the drain and escape
// the cache's byte accounting.
func (f *flusher) cacheWrittenFrame(entry indexEntry, records [][]byte) {
	size := 0
	for _, r := range records {
		size += len(r)
	}
	if size > maxCachedWriteFrameBytes {
		return
	}
	recs := make([][]byte, len(records))
	copy(recs, records)
	f.log.frameCache.putSized(frameKey{segmentBase: entry.segmentBaseOffset, framePos: entry.framePos}, recs, size)
}

// activeForWrite returns the segment the next frame goes into, rolling
// first when the current one is full (rollPending) or has held records
// for longer than the retention roll age. The roll fsyncs the old
// active segment, then installs the new one under the write lock.
func (f *flusher) activeForWrite() (*segment, error) {
	f.mu.RLock()
	if len(f.log.segments) == 0 {
		f.mu.RUnlock()
		return nil, fmt.Errorf("storage: flusher: no active segment")
	}
	active := f.log.segments[len(f.log.segments)-1]
	needRoll := f.rollPending || f.log.segmentAgedOutLocked(active)
	f.mu.RUnlock()
	if !needRoll {
		return active, nil
	}
	return f.roll(active)
}

// roll seals active (after making sure it is synced) and installs a
// fresh segment after it. Returns the new active segment.
func (f *flusher) roll(active *segment) (*segment, error) {
	if err := f.syncIfNeeded(true, active); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur := f.log.segments[len(f.log.segments)-1]; cur != active {
		return cur, nil
	}
	newActive, err := createSegment(f.log.dir, active.nextOffset, f.log.now())
	if err != nil {
		return nil, fmt.Errorf("storage: flusher roll: %w", err)
	}
	f.log.segments = append(f.log.segments, newActive)
	f.rollPending = false
	f.hwmForce = true
	return newActive, nil
}

// rotateActive is the reaper's time-based rotation: seal the active
// segment so the sweep can delete it once its records are older than
// the retention age. Only a fully committed segment is rotated (its
// records are all at or below the high-watermark), so a hidden tail
// never moves into a sealed segment where a failed commit could not
// truncate it; a segment that still has an uncommitted tail is
// rotated on a later sweep, after the ingress WAL has re-committed it.
func (f *flusher) rotateActive() error {
	f.mu.RLock()
	if len(f.log.segments) == 0 {
		f.mu.RUnlock()
		return nil
	}
	active := f.log.segments[len(f.log.segments)-1]
	eligible := active.sizeBytes > 0 && active.nextOffset <= f.log.highWatermark.Load()
	f.mu.RUnlock()
	if !eligible {
		return nil
	}
	_, err := f.roll(active)
	return err
}

// syncIfNeeded fdatasyncs the active segment when forced or when the
// batched-sync thresholds say so. It does not persist the high-watermark;
// drainOnce does that once per pass, after any commit advance.
//
// An fsync failure is final for this Log. After a failed fdatasync the
// kernel may have dropped the dirty pages (Linux marks them clean and
// reports the error once), so a second fsync that "succeeds" proves
// nothing about the bytes that failed, and the segment tail past the
// last good sync is of unknown content. The only sound recovery is the
// one a crash would get: reopen the log and rescan the files. So the
// error is latched (poison): every later append and commit fails with
// it until the log is reopened, and the node logs it at error level.
// Records above the last durable tail are not lost; the ingress WAL
// still holds them and re-commits them after the reopen.
func (f *flusher) syncIfNeeded(force bool, active *segment) error {
	if f.unsyncedBytes.Load() <= 0 {
		return nil
	}
	if !force && !f.shouldSync() {
		return nil
	}
	if active == nil {
		f.mu.RLock()
		if len(f.log.segments) > 0 {
			active = f.log.segments[len(f.log.segments)-1]
		}
		f.mu.RUnlock()
	}
	if active == nil {
		return fmt.Errorf("storage: flusher sync: no active segment")
	}

	syncStart := time.Now()
	var err error
	if h := fsyncHook; h != nil {
		err = h(active)
	}
	if err == nil {
		err = active.sync()
	}
	if err != nil {
		return f.log.poison(err)
	}
	f.lastSync = time.Now()
	f.unsyncedBytes.Store(0)
	f.log.durableTail.Store(active.nextOffset)
	// The snapshot is released only now: the records are durable, so
	// nothing can need the in-memory copy again.
	f.log.clearFlushingThrough(active.nextOffset)
	if m := f.log.opts.Metrics; m != nil {
		m.ObserveFsync(time.Since(syncStart))
	}

	if force || f.log.opts.SyncMode == SyncPerWrite {
		f.hwmForce = true
	}
	return nil
}

// shouldSync gates the batched fsync. Returning false defers it to a
// later pass, which is only safe because needsTimer keeps the timer
// armed while unsyncedBytes > 0.
func (f *flusher) shouldSync() bool {
	if f.log.opts.SyncMode == SyncPerWrite {
		return true
	}
	if f.log.opts.SyncBytes > 0 && f.unsyncedBytes.Load() >= f.log.opts.SyncBytes {
		return true
	}
	return time.Since(f.lastSync) >= f.log.opts.SyncInterval
}

func (f *flusher) requestStop() {
	f.once.Do(func() { close(f.stop) })
}

func (f *flusher) waitDone() {
	<-f.done
}
