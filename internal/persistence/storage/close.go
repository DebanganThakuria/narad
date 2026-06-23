package storage

// Close stops the reaper and the flusher (the latter does one final
// drain), then closes every segment file. Idempotent.
func (l *Log) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Stop the background goroutines first, WITHOUT holding rwmu: the
	// flusher takes rwmu in writeBatch, so closing segments under rwmu
	// before it stops would deadlock.
	l.reaper.requestStop()
	l.reaper.waitDone()
	l.flusher.requestStop()
	l.flusher.waitDone()

	// Now exclude the read path: Read accesses segment files under
	// rwmu.RLock, so take the write lock before closing the file handles
	// to avoid a read-from-closed-fd data race when a topic is deleted
	// under concurrent traffic.
	l.rwmu.Lock()
	defer l.rwmu.Unlock()

	var firstErr error
	if err := l.syncHighWatermark(true); err != nil {
		firstErr = err
	}
	for _, s := range l.segments {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
