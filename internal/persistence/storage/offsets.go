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

// OldestOffset returns the base offset of the oldest retained segment,
// or 0 when the partition has no segments yet.
func (l *Log) OldestOffset() int64 {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].baseOffset
}

// SizeBytes returns the total on-disk size of all segment files.
func (l *Log) SizeBytes() int64 {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	var total int64
	for _, s := range l.segments {
		total += s.sizeBytes
	}
	return total
}

// SegmentCount returns the number of segment files, including the
// active one.
func (l *Log) SegmentCount() int {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	return len(l.segments)
}

// OldestSegmentAt returns the Unix-seconds mtime of the oldest
// segment file. ok=false when the partition has no segments yet or
// the file stat fails.
func (l *Log) OldestSegmentAt() (int64, bool) {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	if len(l.segments) == 0 {
		return 0, false
	}
	mt, err := segmentMTime(l.segments[0])
	if err != nil {
		return 0, false
	}
	return mt, true
}

// SegmentMTimeForOffset returns the Unix-seconds mtime of the
// segment file that contains the given offset. ok=false when the
// offset is past the flushed tail (caller is caught up) or before
// LogStartOffset (data was retention-deleted).
//
// Note: per-message timestamps are not stored on disk. A segment's
// mtime is the time of its last write — within a segment, individual
// records may be older than the mtime by up to the segment's
// lifetime. Treat the returned time as an upper bound on "when the
// consumer's next message was last touched", not an exact produce
// timestamp.
func (l *Log) SegmentMTimeForOffset(offset int64) (int64, bool) {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	for i := len(l.segments) - 1; i >= 0; i-- {
		s := l.segments[i]
		if offset >= s.baseOffset && offset < s.nextOffset {
			mt, err := segmentMTime(s)
			if err != nil {
				return 0, false
			}
			return mt, true
		}
	}
	return 0, false
}
