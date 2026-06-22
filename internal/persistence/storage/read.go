package storage

import "time"

// Read returns the payload bytes stored at the given offset. Lookup
// order: in-memory buffer, decoded-frame cache, on-disk segment.
func (l *Log) Read(offset int64) ([]byte, error) {
	start := time.Now()
	source := "unknown"
	outcome := "ok"
	defer func() {
		if m := l.opts.Metrics; m != nil {
			m.ObserveRead(time.Since(start), source, outcome)
		}
	}()
	if rec, ok := l.buffer.readBuffered(offset); ok {
		source = "buffer"
		out := make([]byte, len(rec))
		copy(out, rec)
		return out, nil
	}
	if rec, ok := l.readFlushing(offset); ok {
		source = "flushing"
		return rec, nil
	}

	entry, idx, unlock, ok, err := l.indexEntryForRead(offset)
	if err != nil {
		source = "index"
		outcome = "error"
		return nil, err
	}
	if !ok {
		source = "index"
		outcome = "not_found"
		return nil, ErrOffsetNotFound
	}
	defer unlock()

	l.cacheMu.Lock()
	if l.cacheValid &&
		l.cacheSegmentBase == entry.segmentBaseOffset &&
		l.cachePos == entry.framePos &&
		idx < int32(len(l.cacheRec)) {
		rec := l.cacheRec[int(idx)]
		out := make([]byte, len(rec))
		copy(out, rec)
		l.cacheMu.Unlock()
		source = "cache"
		return out, nil
	}
	l.cacheMu.Unlock()

	seg := l.findSegmentLocked(entry.segmentBaseOffset)
	if seg == nil {
		source = "segment"
		outcome = "corrupt"
		return nil, ErrCorruptRecord
	}
	_, records, _, err := readFrameAt(seg.file, entry.framePos, l)
	if err != nil {
		source = "disk"
		outcome = "error"
		return nil, err
	}
	if idx < 0 || int(idx) >= len(records) {
		source = "disk"
		outcome = "corrupt"
		return nil, ErrCorruptRecord
	}

	// Some codecs reuse internal buffers; copy out before caching.
	cached := make([][]byte, len(records))
	for i, r := range records {
		cp := make([]byte, len(r))
		copy(cp, r)
		cached[i] = cp
	}

	l.cacheMu.Lock()
	l.cacheSegmentBase = entry.segmentBaseOffset
	l.cachePos = entry.framePos
	l.cacheRec = cached
	l.cacheValid = true
	l.cacheMu.Unlock()

	out := make([]byte, len(cached[int(idx)]))
	copy(out, cached[int(idx)])
	source = "disk"
	return out, nil
}

func (l *Log) indexEntryForRead(offset int64) (indexEntry, int32, func(), bool, error) {
	l.rwmu.RLock()
	entry, idx, ok, err := l.findIndexLocked(offset)
	if err != nil {
		l.rwmu.RUnlock()
		return indexEntry{}, 0, nil, false, err
	}
	if ok {
		return entry, idx, l.rwmu.RUnlock, true, nil
	}
	if l.findSegmentForOffsetLocked(offset) == nil {
		l.rwmu.RUnlock()
		return indexEntry{}, 0, nil, false, nil
	}
	l.rwmu.RUnlock()

	l.rwmu.Lock()
	seg := l.findSegmentForOffsetLocked(offset)
	if seg == nil {
		l.rwmu.Unlock()
		return indexEntry{}, 0, nil, false, nil
	}
	if l.segmentIndexes[seg.baseOffset] == nil {
		if err := l.loadSegmentIndexLocked(seg); err != nil {
			l.rwmu.Unlock()
			return indexEntry{}, 0, nil, false, err
		}
	}
	entry, idx, ok, err = l.findIndexLocked(offset)
	if err != nil {
		l.rwmu.Unlock()
		return indexEntry{}, 0, nil, false, err
	}
	if !ok {
		l.rwmu.Unlock()
		return indexEntry{}, 0, nil, false, nil
	}
	return entry, idx, l.rwmu.Unlock, true, nil
}
