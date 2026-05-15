package storage

// Append pushes a single record into the in-memory buffer and returns
// the offset assigned to it. The disk write is deferred to the
// flusher goroutine; if a threshold is now crossed the flusher is
// signalled to flush ASAP.
func (l *Log) Append(data []byte) (int64, error) {
	if l.closed.Load() {
		return -1, ErrLogClosed
	}
	offset, crossed := l.buffer.push(data)

	select {
	case l.notify <- struct{}{}:
	default:
	}

	if crossed {
		l.flusher.signal()
	}
	return offset, nil
}

func (l *Log) AppendBatch(records [][]byte) (firstOffset, lastOffset int64, err error) {
	if l.closed.Load() {
		return -1, -1, ErrLogClosed
	}
	if len(records) == 0 {
		return 0, -1, nil
	}
	first, last, crossed := l.buffer.pushBatch(records)

	select {
	case l.notify <- struct{}{}:
	default:
	}

	if crossed {
		l.flusher.signal()
	}
	return first, last, nil
}
