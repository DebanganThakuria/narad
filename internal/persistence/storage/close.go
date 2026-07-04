package storage

// Close stops the reaper and the flusher (the latter does one final
// drain), then closes every segment file. Idempotent.
func (l *Log) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Wait out in-flight appends before signalling the flusher's final
	// drain: an Append holding the gate's read side observed
	// closed=false, so its record MUST land in the buffer before the
	// drain or it would be acked-but-lost (and its offset reassigned
	// after reopen). New appends see closed=true and get ErrLogClosed.
	l.appendGate.Lock()
	l.appendGate.Unlock() //nolint:staticcheck // empty critical section is the barrier
	// Stop the background goroutines first, WITHOUT holding rwmu: the
	// flusher takes rwmu in writeBatch, so closing segments under rwmu
	// before it stops would deadlock.
	l.reaper.requestStop()
	l.reaper.waitDone()
	l.flusher.requestStop()
	l.flusher.waitDone()

	// Wake any long-poll waiters blocked on NotifyC so they re-check
	// and observe the closed log instead of sleeping out their full
	// wait against a channel that will never fire again.
	l.notifyAll()

	// Now exclude the read path: Read accesses segment files under
	// rwmu.RLock, so take the write lock before closing the file handles
	// to avoid a read-from-closed-fd data race when a topic is deleted
	// under concurrent traffic.
	l.rwmu.Lock()
	defer l.rwmu.Unlock()

	var firstErr error
	// The flusher's final shutdown drain may have failed to persist
	// buffered acked records; Close must not report success then.
	if err := l.flusher.closeErr; err != nil {
		firstErr = err
	}
	if err := l.syncHighWatermark(true); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, s := range l.segments {
		if err := s.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
