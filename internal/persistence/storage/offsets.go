package storage

// LatestOffset is the offset of the most recently appended record, or
// 0 for an empty log. Use NextOffset to disambiguate "empty" from
// "one record at offset 0".
func (l *Log) LatestOffset() int64 {
	next := l.buffer.nextOffsetSnapshot()
	if next == 0 {
		return 0
	}
	return next - 1
}

// NextOffset is the offset that will be assigned to the next
// successful Append (== total records ever appended, including
// buffered).
func (l *Log) NextOffset() int64 {
	return l.buffer.nextOffsetSnapshot()
}

// HighWatermark is the exclusive upper bound of records visible to consumers.
func (l *Log) HighWatermark() int64 {
	return l.highWatermark.Load()
}

// AdvanceHighWatermark moves the visible tail forward, persists it, and wakes
// long-poll consumers waiting on new committed records.
func (l *Log) AdvanceHighWatermark(newHWM int64) error {
	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()

	cur := l.highWatermark.Load()
	if newHWM <= cur {
		return nil
	}
	if err := l.persistHighWatermark(newHWM); err != nil {
		return err
	}
	l.highWatermark.Store(newHWM)
	select {
	case l.notify <- struct{}{}:
	default:
	}
	return nil
}
