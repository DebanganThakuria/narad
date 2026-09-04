package storage

// appendGateHook, when non-nil, runs after the closed-check while the
// append gate (read side) is held, before the buffer push. It is nil in
// production and set only by tests to schedule-force the Append/Close
// race: a concurrent Close must block on the gate until the push
// completes, so an append that passed the closed-check can never miss
// the flusher's final drain.
var appendGateHook func()

// Append pushes a single record into the in-memory buffer and returns
// the offset assigned to it. The disk write is deferred to the
// flusher goroutine; if a threshold is now crossed the flusher is
// signalled to flush ASAP.
//
// The appendGate read lock spans the closed-check and the buffer push
// so a concurrent Close cannot slip between them: a successful Append
// is always drained by the flusher's final shutdown drain, and an
// Append that loses the race returns ErrLogClosed instead of acking a
// record that would never be flushed.
func (l *Log) Append(data []byte) (int64, error) {
	l.appendGate.RLock()
	defer l.appendGate.RUnlock()
	if l.closed.Load() {
		return -1, ErrLogClosed
	}
	if h := appendGateHook; h != nil {
		h()
	}
	offset, crossed := l.buffer.push(data)

	// No broadcast here: long-poll waiters (queue consumers, the fan-out
	// reader) gate on the high-watermark, so waking them for a buffered,
	// not-yet-visible record only costs a full re-probe that finds
	// nothing. AdvanceHighWatermark broadcasts when records become
	// visible.
	if crossed {
		l.flusher.signal()
	}
	return offset, nil
}

// AppendBatch pushes records into the in-memory buffer as one
// contiguous run and returns the first and last offsets assigned. An
// empty batch is a no-op returning (0, -1, nil). See Append for the
// Close-race guarantees.
func (l *Log) AppendBatch(records [][]byte) (firstOffset, lastOffset int64, err error) {
	l.appendGate.RLock()
	defer l.appendGate.RUnlock()
	if l.closed.Load() {
		return -1, -1, ErrLogClosed
	}
	if len(records) == 0 {
		return 0, -1, nil
	}
	if h := appendGateHook; h != nil {
		h()
	}
	first, last, crossed := l.buffer.pushBatch(records, true)

	if crossed {
		l.flusher.signal()
	}
	return first, last, nil
}

// AppendBatchOwned is AppendBatch for callers that hand over ownership
// of the record slices: the buffer keeps them as-is instead of copying.
// The caller must not read or modify any of the slices afterwards (the
// flushing snapshot and readers alias them until they reach disk). The
// commit path uses it because its envelopes are freshly built per batch
// and never touched again.
func (l *Log) AppendBatchOwned(records [][]byte) (firstOffset, lastOffset int64, err error) {
	l.appendGate.RLock()
	defer l.appendGate.RUnlock()
	if l.closed.Load() {
		return -1, -1, ErrLogClosed
	}
	if len(records) == 0 {
		return 0, -1, nil
	}
	if h := appendGateHook; h != nil {
		h()
	}
	first, last, crossed := l.buffer.pushBatch(records, false)

	if crossed {
		l.flusher.signal()
	}
	return first, last, nil
}
