package storage

// Close deregisters the log from the shared reaper and stops the
// flusher (which does one final drain), then closes every segment
// file. Idempotent.
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
	// Retention runs on the process-wide loop, so stopping it is a
	// deregistration rather than a goroutine join. A sweep already in
	// flight for this log is harmless: it takes rwmu and finds a closed
	// log with nothing to do.
	sharedReaper.unregister(l)
	l.reaper.requestStop()
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
	if err := l.closeHWMFile(); err != nil && firstErr == nil {
		firstErr = err
	}
	for i, s := range l.segments {
		// Only the active segment can hold unsynced bytes; sealed
		// segments were synced when they rolled, and many of them have
		// no open handle anyway.
		var err error
		if i == len(l.segments)-1 {
			err = s.close()
		} else {
			err = s.release()
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// closeSegments releases every segment file handle, ignoring errors.
// Used on NewLog's failure paths, where the recovery error wins.
func (l *Log) closeSegments() {
	for _, s := range l.segments {
		_ = s.release()
	}
	_ = l.closeHWMFile()
}
