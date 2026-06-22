package storage

// Close stops the reaper and the flusher (the latter does one final
// drain), then closes every segment file. Idempotent.
func (l *Log) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	l.reaper.requestStop()
	l.reaper.waitDone()
	l.flusher.requestStop()
	l.flusher.waitDone()

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
