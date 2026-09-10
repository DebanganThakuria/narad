package storage

import "time"

// The flushing snapshot holds records from the moment the flusher
// drains them out of the buffer until their frame is proven durable by
// a successful fdatasync. Three offsets describe it:
//
//	flushingBase                 first record still in the snapshot
//	flushingWritten              records below this are in the segment
//	                             file but not yet synced
//	flushingBase+len(records)    end of the snapshot
//
// A failed segment write leaves flushingWritten where it was, so the
// retry rewrites exactly the unwritten suffix; a successful sync clears
// everything below the durable tail. Keeping written-but-unsynced
// records in memory costs nothing on the commit path (every commit
// forces a sync in the same drain) and means the in-memory copy still
// exists at the moment a sync fails, so the discard of an uncommitted
// tail never has to trust bytes the kernel may already have dropped.

// drainBufferForFlush moves buffered records into the flushing snapshot
// and returns the records that still need writing, with the offset of
// the first of them. If a previous writeBatch failed, the snapshot
// still holds its unwritten records: new records are appended (offsets
// are contiguous) rather than overwriting, so the retry writes the
// whole unwritten run and no already-acked record is ever dropped.
//
// Lock ordering: buffer.mu → flushingMu.
func (l *Log) drainBufferForFlush() ([][]byte, int64) {
	l.buffer.mu.Lock()
	defer l.buffer.mu.Unlock()
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()

	if len(l.buffer.records) == 0 {
		if l.flushingValid {
			return l.unwrittenFlushingLocked()
		}
		return nil, l.buffer.nextOffset
	}

	records := l.buffer.records
	base := l.buffer.baseOffset
	l.buffer.records = nil
	l.buffer.baseOffset = l.buffer.nextOffset
	l.buffer.bytes = 0
	l.buffer.firstAt = time.Time{}

	if l.flushingValid {
		l.flushingRecords = append(l.flushingRecords, records...)
	} else {
		l.flushingBase = base
		l.flushingWritten = base
		l.flushingRecords = records
		l.flushingValid = true
	}
	return l.unwrittenFlushingLocked()
}

// unwrittenFlushingLocked returns the snapshot suffix that has not
// reached the segment file yet. Caller must hold flushingMu.
func (l *Log) unwrittenFlushingLocked() ([][]byte, int64) {
	skip := max(l.flushingWritten-l.flushingBase, 0)
	if skip >= int64(len(l.flushingRecords)) {
		return nil, l.flushingBase + int64(len(l.flushingRecords))
	}
	return l.flushingRecords[skip:], l.flushingWritten
}

// Read by flusher.needsTimer: while this is true the flusher keeps its
// timer armed so the retry actually happens.
//
// hasPendingFlushing reports whether a previous drain's records are
// still waiting to reach the segment file (their writeBatch failed).
// Records that are written but not yet synced do not count: the sync
// is the drain's own business, not a retry.
func (l *Log) hasPendingFlushing() bool {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	return l.flushingValid && l.flushingWritten < l.flushingBase+int64(len(l.flushingRecords))
}

func (l *Log) readFlushing(offset int64) ([]byte, bool) {
	rec, ok := l.readFlushingShared(offset)
	if !ok {
		return nil, false
	}
	out := make([]byte, len(rec))
	copy(out, rec)
	return out, true
}

// readFlushingShared is readFlushing without the copy. Flushing records
// are never mutated in place (clearFlushingThrough only reslices), so
// the returned slice is stable; see Log.ReadShared for the contract.
func (l *Log) readFlushingShared(offset int64) ([]byte, bool) {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	if !l.flushingValid || offset < l.flushingBase {
		return nil, false
	}
	idx := offset - l.flushingBase
	if idx < 0 || int(idx) >= len(l.flushingRecords) {
		return nil, false
	}
	return l.flushingRecords[idx], true
}

// markFlushingWritten records that snapshot records below end
// (exclusive) are now in the segment file. They stay in the snapshot
// until a sync proves them durable (clearFlushingThrough).
func (l *Log) markFlushingWritten(end int64) {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	if !l.flushingValid {
		return
	}
	if end > l.flushingWritten {
		l.flushingWritten = end
	}
}

// clearFlushingThrough drops flushing records below end (exclusive)
// once their frame is durable in the active segment. After a partial
// batch write the unwritten suffix stays in place for the flusher's
// retry.
func (l *Log) clearFlushingThrough(end int64) {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	if !l.flushingValid || end <= l.flushingBase {
		return
	}
	n := end - l.flushingBase
	if n >= int64(len(l.flushingRecords)) {
		l.resetFlushingLocked()
		return
	}
	l.flushingRecords = l.flushingRecords[n:]
	l.flushingBase = end
	if l.flushingWritten < end {
		l.flushingWritten = end
	}
}

// resetFlushingLocked empties the snapshot. Caller must hold flushingMu.
func (l *Log) resetFlushingLocked() {
	l.flushingBase = 0
	l.flushingWritten = 0
	l.flushingRecords = nil
	l.flushingValid = false
}
