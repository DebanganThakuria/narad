package storage

import (
	"fmt"
	"sync"
	"time"
)

const minTimerFlushAge = time.Second

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
// range is proven durable.
type commitRequest struct {
	done   chan error
	first  int64
	last   int64
	hwm    int64
	verify bool
}

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
// A CRC mismatch is returned as a VerifyError so callers can classify it
// separately from a write or sync failure. Returns ErrLogClosed if the
// log is closing.
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
func (f *flusher) drainOnce(forceSync, forceDrain bool, commit *commitRequest) error {
	f.hwmForce = forceSync

	var err error
	// A pending flushing snapshot means an earlier writeBatch failed;
	// retry it on every drain regardless of buffer thresholds.
	if !forceDrain && !f.log.hasPendingFlushing() && !f.log.buffer.shouldFlushByAge(f.timerFlushAge()) {
		err = f.syncIfNeeded(forceSync, nil)
	} else {
		records, baseOffset := f.log.drainBufferForFlush()
		if len(records) == 0 {
			err = f.syncIfNeeded(forceSync, nil)
		} else {
			err = f.writeBatch(records, baseOffset, forceSync)
		}
	}
	if err != nil {
		return err
	}

	if commit != nil {
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
	}
	return f.log.syncHighWatermark(f.hwmForce)
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

	f.mu.RLock()
	if len(f.log.segments) == 0 {
		f.mu.RUnlock()
		return fmt.Errorf("storage: flusher: no active segment")
	}
	active := f.log.segments[len(f.log.segments)-1]
	f.mu.RUnlock()

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
	active.sizeBytes = pos + int64(n)
	active.nextOffset = baseOffset + int64(len(records))
	f.log.appendIndexLocked(indexEntry{
		segmentBaseOffset: active.baseOffset,
		baseOffset:        baseOffset,
		recordCount:       int32(len(records)),
		framePos:          pos,
		frameLen:          int32(n),
	})
	f.log.clearFlushingThrough(baseOffset + int64(len(records)))
	shouldRoll := active.sizeBytes >= f.log.opts.SegmentBytes

	if m := f.log.opts.Metrics; m != nil {
		m.ObserveFlush(time.Since(flushStart), int64(n))
	}

	f.unsyncedBytes += int64(n)
	f.mu.Unlock()

	if err := f.syncIfNeeded(forceSync || shouldRoll, active); err != nil {
		return err
	}

	if shouldRoll {
		f.mu.Lock()
		newActive, err := createSegment(f.log.dir, active.nextOffset)
		if err != nil {
			f.mu.Unlock()
			return fmt.Errorf("storage: flusher roll: %w", err)
		}
		f.log.segments = append(f.log.segments, newActive)
		f.mu.Unlock()
	}

	f.log.notifyAll()
	return nil
}

// syncIfNeeded fdatasyncs the active segment when forced or when the
// batched-sync thresholds say so. It does not persist the high-watermark;
// drainOnce does that once per pass, after any commit advance.
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
	if err := active.sync(); err != nil {
		return fmt.Errorf("storage: flusher fsync: %w", err)
	}
	f.lastSync = time.Now()
	f.unsyncedBytes = 0
	f.log.durableTail.Store(active.nextOffset)
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
