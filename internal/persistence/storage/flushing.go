package storage

import "time"

func (l *Log) drainBufferForFlush() ([][]byte, int64) {
	l.buffer.mu.Lock()
	defer l.buffer.mu.Unlock()
	if len(l.buffer.records) == 0 {
		return nil, l.buffer.nextOffset
	}

	records := l.buffer.records
	base := l.buffer.baseOffset
	l.buffer.records = nil
	l.buffer.baseOffset = l.buffer.nextOffset
	l.buffer.bytes = 0
	l.buffer.firstAt = time.Time{}

	l.flushingMu.Lock()
	l.flushingBase = base
	l.flushingRec = records
	l.flushingValid = true
	l.flushingMu.Unlock()

	return records, base
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

func (l *Log) clearFlushing(baseOffset int64, count int) {
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()
	if !l.flushingValid || l.flushingBase != baseOffset || len(l.flushingRec) != count {
		return
	}
	l.flushingBase = 0
	l.flushingRec = nil
	l.flushingValid = false
}
