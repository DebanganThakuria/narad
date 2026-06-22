package storage

import (
	"sync"
	"time"
)

// buffer is the per-partition in-memory accumulator producers push
// into. The flusher goroutine drains it on threshold or interval, and
// readers consult it for records that haven't reached disk yet.
type buffer struct {
	mu sync.Mutex

	records    [][]byte
	baseOffset int64
	nextOffset int64
	bytes      int
	firstAt    time.Time

	flushBytes   int
	flushRecords int
}

type bufferStats struct {
	records int
	bytes   int
}

func newBuffer(startOffset int64, flushBytes, flushRecords int) *buffer {
	return &buffer{
		baseOffset:   startOffset,
		nextOffset:   startOffset,
		flushBytes:   flushBytes,
		flushRecords: flushRecords,
	}
}

// push returns the assigned offset and reports whether the
// byte/record threshold is now crossed.
func (b *buffer) push(record []byte) (int64, bool, bufferStats) {
	cp := make([]byte, len(record))
	copy(cp, record)

	b.mu.Lock()
	if len(b.records) == 0 {
		b.firstAt = time.Now()
	}
	off := b.nextOffset
	b.records = append(b.records, cp)
	b.nextOffset++
	b.bytes += len(cp)
	cross := b.crossedThresholdLocked()
	stats := b.statsLocked()
	b.mu.Unlock()
	return off, cross, stats
}

func (b *buffer) pushBatch(records [][]byte) (int64, int64, bool, bufferStats) {
	if len(records) == 0 {
		return 0, -1, false, bufferStats{}
	}
	cps := make([][]byte, len(records))
	for i, r := range records {
		cp := make([]byte, len(r))
		copy(cp, r)
		cps[i] = cp
	}

	b.mu.Lock()
	if len(b.records) == 0 {
		b.firstAt = time.Now()
	}
	first := b.nextOffset
	b.records = append(b.records, cps...)
	b.nextOffset += int64(len(cps))
	for _, cp := range cps {
		b.bytes += len(cp)
	}
	last := b.nextOffset - 1
	cross := b.crossedThresholdLocked()
	stats := b.statsLocked()
	b.mu.Unlock()
	return first, last, cross, stats
}

// drain hands the buffered records to the flusher and resets the
// buffer. After drain, readBuffered won't find them — the flusher
// must finish writing the frame and updating the index before
// releasing rwmu, or readers will see a temporary gap.
func (b *buffer) drain() ([][]byte, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.records) == 0 {
		return nil, b.nextOffset
	}
	recs := b.records
	base := b.baseOffset
	b.records = nil
	b.baseOffset = b.nextOffset
	b.bytes = 0
	b.firstAt = time.Time{}
	return recs, base
}

func (b *buffer) shouldFlushByAge(maxAge time.Duration) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.records) == 0 {
		return false
	}
	if maxAge <= 0 {
		return true
	}
	return !b.firstAt.IsZero() && time.Since(b.firstAt) >= maxAge
}

func (b *buffer) stats() bufferStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.statsLocked()
}

func (b *buffer) readBuffered(offset int64) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if offset < b.baseOffset || offset >= b.nextOffset {
		return nil, false
	}
	idx := offset - b.baseOffset
	if idx < 0 || int(idx) >= len(b.records) {
		return nil, false
	}
	return b.records[idx], true
}

func (b *buffer) nextOffsetSnapshot() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.nextOffset
}

func (b *buffer) crossedThresholdLocked() bool {
	if b.flushBytes > 0 && b.bytes >= b.flushBytes {
		return true
	}
	if b.flushRecords > 0 && len(b.records) >= b.flushRecords {
		return true
	}
	return false
}

func (b *buffer) statsLocked() bufferStats {
	return bufferStats{
		records: len(b.records),
		bytes:   b.bytes,
	}
}
