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

// AdvanceHighWatermark moves the visible tail forward and wakes long-poll
// consumers waiting on new committed records. Persistence is batched by the
// storage flusher; the produce path must not fsync this metadata per record.
func (l *Log) AdvanceHighWatermark(newHWM int64) error {
	cur := l.highWatermark.Load()
	if newHWM <= cur {
		return nil
	}
	for newHWM > cur && !l.highWatermark.CompareAndSwap(cur, newHWM) {
		cur = l.highWatermark.Load()
	}
	// Broadcast: one commit can make many records visible, so EVERY
	// long-poll waiter must wake and re-check, not just one.
	l.notifyAll()
	return nil
}
