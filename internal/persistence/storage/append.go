package storage

import "time"

// Append pushes a single record into the in-memory buffer and returns
// the offset assigned to it. The disk write is deferred to the
// flusher goroutine; if a threshold is now crossed the flusher is
// signalled to flush ASAP.
func (l *Log) Append(data []byte) (int64, error) {
	start := time.Now()
	outcome := "ok"
	defer func() {
		if m := l.opts.Metrics; m != nil {
			m.ObserveAppend("append", time.Since(start), outcome, 1, int64(len(data)))
		}
	}()
	if l.closed.Load() {
		outcome = "closed"
		return -1, ErrLogClosed
	}
	offset, crossed, stats := l.buffer.push(data)
	l.observeBufferStats(stats)

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
	start := time.Now()
	outcome := "ok"
	var bytes int64
	for _, record := range records {
		bytes += int64(len(record))
	}
	defer func() {
		if m := l.opts.Metrics; m != nil {
			m.ObserveAppend("append_batch", time.Since(start), outcome, len(records), bytes)
		}
	}()
	if l.closed.Load() {
		outcome = "closed"
		return -1, -1, ErrLogClosed
	}
	if len(records) == 0 {
		return 0, -1, nil
	}
	first, last, crossed, stats := l.buffer.pushBatch(records)
	l.observeBufferStats(stats)

	select {
	case l.notify <- struct{}{}:
	default:
	}

	if crossed {
		l.flusher.signal()
	}
	return first, last, nil
}
