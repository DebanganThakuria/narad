package storage

import (
	"fmt"
	"sync"
	"time"
)

// flusher is the single goroutine that drains a Log's buffer to the
// active segment file. "Single writer per partition" lives here.
type flusher struct {
	log *Log

	wakeup   chan struct{}
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
	interval time.Duration

	mu *sync.RWMutex
}

func newFlusher(log *Log, mu *sync.RWMutex, interval time.Duration) *flusher {
	return &flusher{
		log:      log,
		wakeup:   make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		interval: interval,
		mu:       mu,
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
		select {
		case <-f.wakeup:
		case <-timer.C:
		case <-f.stop:
			_ = f.drainOnce()
			return
		}
		_ = f.drainOnce()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(f.interval)
	}
}

func (f *flusher) drainOnce() error {
	records, baseOffset := f.log.buffer.drain()
	if len(records) == 0 {
		return nil
	}
	return f.writeBatch(records, baseOffset)
}

func (f *flusher) writeBatch(records [][]byte, baseOffset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.log.segments) == 0 {
		return fmt.Errorf("storage: flusher: no active segment")
	}
	active := f.log.segments[len(f.log.segments)-1]

	pos, n, err := active.writeFrame(records, baseOffset, f.log.codec)
	if err != nil {
		return err
	}
	if err := active.sync(); err != nil {
		return fmt.Errorf("storage: flusher fsync: %w", err)
	}

	for i := range records {
		f.log.index[baseOffset+int64(i)] = indexEntry{
			segmentBaseOffset: active.baseOffset,
			framePos:          pos,
			idx:               int32(i),
			frameLen:          int32(n),
		}
	}

	if active.sizeBytes >= f.log.opts.SegmentBytes {
		newActive, err := createSegment(f.log.dir, active.nextOffset)
		if err != nil {
			return fmt.Errorf("storage: flusher roll: %w", err)
		}
		f.log.segments = append(f.log.segments, newActive)
	}

	select {
	case f.log.notify <- struct{}{}:
	default:
	}
	return nil
}

func (f *flusher) requestStop() {
	f.once.Do(func() { close(f.stop) })
}

func (f *flusher) waitDone() {
	<-f.done
}
