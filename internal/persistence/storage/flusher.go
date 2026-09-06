package storage

import (
	"fmt"
	"sync"
	"time"
)

const minTimerFlushAge = time.Second

// fsyncHook, when non-nil, runs on the flusher goroutine right before
// every segment fdatasync; a non-nil result is treated as the fsync's
// error. Nil in production; tests use it to inject a sync failure at
// the exact point of the commit protocol where it would happen.
var fsyncHook func(*segment) error

// flusher is the single goroutine that drains a Log's buffer to the
// active segment file. "Single writer per partition" lives here.
type flusher struct {
	log *Log

	wakeup   chan struct{}
	syncReqs chan commitRequest
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	interval time.Duration

	mu            *sync.RWMutex
	lastSync      time.Time
	unsyncedBytes int64

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
	// SyncPerWrite). Reset at the start of every drainOnce.
	hwmForce bool

	// enc holds the frame encode buffers, reused across flushes. Only the
	// flusher goroutine touches them and nothing retains an encoded frame
	// past writeEncodedFrame, so reuse cannot alias live data.
	enc frameEncoder

	// verifyBuf is the reusable CRC read buffer for VerifyDurable on the
	// commit path (no per-frame payload allocation).
	verifyBuf []byte

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
// the log was opened with DisableCommitVerify), advances the
// high-watermark to last+1 so the records become visible, and persists
// the advanced high-watermark before returning.
//
// Persisting the high-watermark inside the same drain closes the window
// where a batch was fsynced and checkpointed upstream but its visibility
// boundary had not reached disk: after CommitDurable returns, a restart
// recovers a high-watermark of at least last+1.
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

func (f *flusher) run() {
	defer close(f.done)

	timer := time.NewTimer(f.interval)
	defer timer.Stop()

	for {
		forceDrain := false
		select {
		case <-f.wakeup:
			forceDrain = true
		case req := <-f.syncReqs:
			// Synchronous flush+fsync: drain the buffer and force an
			// fsync so the caller can treat the record as durable on
			// return. Stays on the single flusher goroutine so the
			// "one writer per partition" invariant holds.
			req.done <- f.drainOnce(true, true, &req)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(f.interval)
			continue
		case <-timer.C:
		case <-f.stop:
			f.closeErr = f.drainOnce(true, true, nil)
			return
		}
		_ = f.drainOnce(false, forceDrain, nil)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(f.interval)
	}
}

// drainOnce is one flusher pass: write whatever needs writing, sync if
// required, run the commit's verify + high-watermark advance (if any),
// then persist the high-watermark once. The HWM persist is the last
// step so a commit's advance lands in the same pass that fsynced its
// records.
//
// Any failure of a commit request, at whatever step, discards the
// uncommitted tail before the error is reported (see CommitDurable).
func (f *flusher) drainOnce(forceSync, forceDrain bool, commit *commitRequest) error {
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
		if verr := f.log.verifyDurable(commit.first, commit.last, &f.verifyBuf); verr != nil {
			return VerifyError{First: commit.first, Last: commit.last, Err: verr}
		}
	}
	if commit.hwm >= 0 {
		// Persist first, then expose: if the persist fails the records
		// stay hidden and the commit reports failure, so the caller
		// (the ingress dispatcher) retries instead of checkpointing past
		// a batch whose visibility boundary never reached disk.
		if perr := f.log.persistHighWatermarkAtLeast(commit.hwm); perr != nil {
			return perr
		}
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
	for len(records) > 0 {
		n := frameSplitLen(records)
		if err := f.writeFrame(records[:n], baseOffset, forceSync); err != nil {
			return err
		}
		baseOffset += int64(n)
		records = records[n:]
	}
	return nil
}

func (f *flusher) writeFrame(records [][]byte, baseOffset int64, forceSync bool) error {
	flushStart := time.Now()
	frame, err := f.enc.encodeFrame(records, baseOffset, f.log.codec)
	if err != nil {
		return err
	}

	active, err := f.activeForWrite()
	if err != nil {
		return err
	}

	// The write syscall runs outside rwmu. This goroutine is the only
	// writer of the active segment, so its size and offset fields cannot
	// change underneath us; readers bound their scans by sizeBytes, which
	// is only advanced (under the write lock) after the bytes are in
	// place, so they never observe a partially written frame.
	pos, n, err := active.writeEncodedFrame(frame)
	if err != nil {
		return err
	}

	f.mu.Lock()
	now := f.log.now()
	if active.firstWriteAt.IsZero() {
		active.firstWriteAt = now
	}
	active.lastWriteAt = now
	active.sizeBytes = pos + int64(n)
	active.nextOffset = baseOffset + int64(len(records))
	f.log.appendIndexLocked(indexEntry{
		segmentBaseOffset: active.baseOffset,
		baseOffset:        baseOffset,
		recordCount:       int32(len(records)),
		framePos:          pos,
		frameLen:          int32(n),
	})
	f.log.markFlushingWritten(active.nextOffset)
	if active.sizeBytes >= f.log.opts.SegmentBytes {
		f.rollPending = true
	}
	full := f.rollPending

	if m := f.log.opts.Metrics; m != nil {
		m.ObserveFlush(time.Since(flushStart), int64(n))
	}

	f.unsyncedBytes += int64(n)
	f.mu.Unlock()

	// A full segment is synced now so the roll before the next write
	// finds it durable.
	return f.syncIfNeeded(forceSync || full, active)
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
	if f.unsyncedBytes <= 0 {
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
	f.unsyncedBytes = 0
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

func (f *flusher) shouldSync() bool {
	if f.log.opts.SyncMode == SyncPerWrite {
		return true
	}
	if f.log.opts.SyncBytes > 0 && f.unsyncedBytes >= f.log.opts.SyncBytes {
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
