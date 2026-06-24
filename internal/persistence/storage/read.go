package storage

// Read returns the payload bytes stored at the given offset. Lookup order:
// in-memory write buffer, flushing buffer, per-partition decoded-frame LRU,
// on-disk segment (decoded once and inserted into the LRU).
func (l *Log) Read(offset int64) ([]byte, error) {
	if rec, ok := l.buffer.readBuffered(offset); ok {
		out := make([]byte, len(rec))
		copy(out, rec)
		return out, nil
	}
	if rec, ok := l.readFlushing(offset); ok {
		return rec, nil
	}

	entry, idx, unlock, ok, err := l.indexEntryForRead(offset)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrOffsetNotFound
	}
	defer unlock()

	key := frameKey{segmentBase: entry.segmentBaseOffset, framePos: entry.framePos}
	if recs, ok := l.frameCache.get(key); ok {
		if int(idx) < 0 || int(idx) >= len(recs) {
			return nil, ErrCorruptRecord
		}
		out := make([]byte, len(recs[idx]))
		copy(out, recs[idx])
		return out, nil
	}

	seg := l.findSegmentLocked(entry.segmentBaseOffset)
	if seg == nil {
		return nil, ErrCorruptRecord
	}
	_, records, _, err := readFrameAt(seg.file, entry.framePos, l)
	if err != nil {
		return nil, err
	}
	if idx < 0 || int(idx) >= len(records) {
		return nil, ErrCorruptRecord
	}

	// Some codecs reuse internal buffers; copy out before caching.
	cached := make([][]byte, len(records))
	for i, r := range records {
		cp := make([]byte, len(r))
		copy(cp, r)
		cached[i] = cp
	}
	l.frameCache.put(key, cached)

	out := make([]byte, len(cached[int(idx)]))
	copy(out, cached[int(idx)])
	return out, nil
}

// VerifyDurable re-reads the frames covering [first,last] and validates each
// frame's CRC against the on-disk bytes, without decoding. Called right after
// Sync on the commit path to confirm the durable copy is intact before the
// high-watermark advances and the WAL source is dropped. The CRC was computed
// over the stored (possibly compressed) payload at write time, so this is a
// full torn/corrupt-write check with zero decode — one CRC per frame instead
// of one decode per record.
func (l *Log) VerifyDurable(first, last int64) error {
	for off := first; off <= last; {
		entry, _, unlock, ok, err := l.indexEntryForRead(off)
		if err != nil {
			return err
		}
		if !ok {
			return ErrOffsetNotFound
		}
		seg := l.findSegmentLocked(entry.segmentBaseOffset)
		if seg == nil {
			unlock()
			return ErrCorruptRecord
		}
		recordCount, _, verr := verifyFrameAt(seg.file, entry.framePos)
		unlock()
		if verr != nil {
			return verr
		}
		if recordCount <= 0 {
			return ErrCorruptRecord
		}
		off = entry.baseOffset + int64(recordCount)
	}
	return nil
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
