package storage

// Read returns the payload bytes stored at the given offset. Lookup
// order: in-memory buffer, decoded-frame cache, on-disk segment.
func (l *Log) Read(offset int64) ([]byte, error) {
	if rec, ok := l.buffer.readBuffered(offset); ok {
		out := make([]byte, len(rec))
		copy(out, rec)
		return out, nil
	}

	l.rwmu.RLock()
	entry, ok := l.index[offset]
	if !ok {
		l.rwmu.RUnlock()
		return nil, ErrOffsetNotFound
	}

	l.cacheMu.Lock()
	if l.cacheValid &&
		l.cacheSegmentBase == entry.segmentBaseOffset &&
		l.cachePos == entry.framePos &&
		entry.idx < int32(len(l.cacheRec)) {
		rec := l.cacheRec[entry.idx]
		out := make([]byte, len(rec))
		copy(out, rec)
		l.cacheMu.Unlock()
		l.rwmu.RUnlock()
		return out, nil
	}
	l.cacheMu.Unlock()

	seg := l.findSegmentLocked(entry.segmentBaseOffset)
	if seg == nil {
		l.rwmu.RUnlock()
		return nil, ErrCorruptRecord
	}
	_, records, _, err := readFrameAt(seg.file, entry.framePos, l)
	l.rwmu.RUnlock()
	if err != nil {
		return nil, err
	}
	if entry.idx < 0 || int(entry.idx) >= len(records) {
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

	out := make([]byte, len(cached[entry.idx]))
	copy(out, cached[entry.idx])
	return out, nil
}
