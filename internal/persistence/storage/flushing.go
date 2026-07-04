package storage

import "time"

// drainBufferForFlush moves buffered records into the flushing snapshot
// and returns the full snapshot. If a previous writeBatch failed, the
// snapshot still holds its unwritten records: new records are appended
// (offsets are contiguous) rather than overwriting, so the retry writes
// the whole run and no already-acked record is ever dropped.
//
// Lock ordering: buffer.mu → flushingMu.
func (l *Log) drainBufferForFlush() ([][]byte, int64) {
	l.buffer.mu.Lock()
	defer l.buffer.mu.Unlock()
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()

	if len(l.buffer.records) == 0 {
		if l.flushingValid {
			return l.flushingRec, l.flushingBase
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
		l.flushingRec = append(l.flushingRec, records...)
	} else {
		l.flushingBase = base
		l.flushingRec = records
		l.flushingValid = true
	}
	return l.flushingRec, l.flushingBase
}

// hasPendingFlushing reports whether a previous drain's records are
// still waiting to reach disk (their writeBatch failed).
func (l *Log) hasPendingFlushing() bool {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	return l.flushingValid
}

func (l *Log) readFlushing(offset int64) ([]byte, bool) {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	if !l.flushingValid || offset < l.flushingBase {
		return nil, false
	}
	idx := offset - l.flushingBase
	if idx < 0 || int(idx) >= len(l.flushingRec) {
		return nil, false
	}
	rec := l.flushingRec[idx]
	out := make([]byte, len(rec))
	copy(out, rec)
	return out, true
}

// clearFlushingThrough drops flushing records below end (exclusive) once
// their frame is written to the active segment. After a partial batch
// write the unwritten suffix stays in place for the flusher's retry.
func (l *Log) clearFlushingThrough(end int64) {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	if !l.flushingValid || end <= l.flushingBase {
		return
	}
	n := end - l.flushingBase
	if n >= int64(len(l.flushingRec)) {
		l.flushingBase = 0
		l.flushingRec = nil
		l.flushingValid = false
		return
	}
	l.flushingRec = l.flushingRec[n:]
	l.flushingBase = end
}
