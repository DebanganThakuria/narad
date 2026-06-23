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
	syncReqs chan chan error
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	interval time.Duration

	mu            *sync.RWMutex
	lastSync      time.Time
	unsyncedBytes int64
}

func newFlusher(log *Log, mu *sync.RWMutex, interval time.Duration) *flusher {
	return &flusher{
		log:      log,
		wakeup:   make(chan struct{}, 1),
		syncReqs: make(chan chan error),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		interval: interval,
		mu:       mu,
		lastSync: time.Now(),
	}
}

// Sync synchronously drains the write buffer to the active segment and
// fsyncs it, blocking until the data is durable on disk. Use it on the
// commit path when a record must be durable before it is made visible
// (Narad has no follower replication, so the partition log is the sole
// copy). Returns ErrLogClosed if the log is closing.
func (l *Log) Sync() error {
	if l.closed.Load() {
		return ErrLogClosed
	}
	done := make(chan error, 1)
	select {
	case l.flusher.syncReqs <- done:
		return <-done
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
		case done := <-f.syncReqs:
			// Synchronous flush+fsync: drain the buffer and force an
			// fsync so the caller can treat the record as durable on
			// return. Stays on the single flusher goroutine so the
			// "one writer per partition" invariant holds.
			done <- f.drainOnce(true, true)
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
			_ = f.drainOnce(true, true)
			return
		}
		_ = f.drainOnce(false, forceDrain)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(f.interval)
	}
}

func (f *flusher) drainOnce(forceSync, forceDrain bool) error {
	if !forceDrain && !f.log.buffer.shouldFlushByAge(f.timerFlushAge()) {
		if err := f.syncIfNeeded(forceSync, nil); err != nil {
			return err
		}
		return f.log.syncHighWatermark(forceSync)
	}

	records, baseOffset := f.log.drainBufferForFlush()
	if len(records) == 0 {
		if err := f.syncIfNeeded(forceSync, nil); err != nil {
			return err
		}
		return f.log.syncHighWatermark(forceSync)
	}
	if err := f.writeBatch(records, baseOffset, forceSync); err != nil {
		return err
	}
	return f.log.syncHighWatermark(forceSync)
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

func (f *flusher) writeBatch(records [][]byte, baseOffset int64, forceSync bool) error {
	flushStart := time.Now()
	frame, err := encodeFrame(records, baseOffset, f.log.codec)
	if err != nil {
		return err
	}

	f.mu.Lock()
	if len(f.log.segments) == 0 {
		f.mu.Unlock()
		return fmt.Errorf("storage: flusher: no active segment")
	}
	active := f.log.segments[len(f.log.segments)-1]

	pos, n, err := active.writeEncodedFrame(frame, baseOffset, len(records))
	if err != nil {
		f.mu.Unlock()
		return err
	}

	f.log.appendIndexLocked(indexEntry{
		segmentBaseOffset: active.baseOffset,
		baseOffset:        baseOffset,
		recordCount:       int32(len(records)),
		framePos:          pos,
		frameLen:          int32(n),
	})
	f.log.clearFlushing(baseOffset, len(records))
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
		if m := f.log.opts.Metrics; m != nil {
			m.IncSegmentRolled()
		}
		f.mu.Unlock()
	}

	select {
	case f.log.notify <- struct{}{}:
	default:
	}
	return nil
}

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

	hwmForce := force || f.log.opts.SyncMode == SyncPerWrite
	return f.log.syncHighWatermark(hwmForce)
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
