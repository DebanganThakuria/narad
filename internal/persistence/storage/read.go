package storage

import (
	"errors"
	"os"
)

// Read returns the payload bytes stored at the given offset. Lookup order:
// in-memory write buffer, flushing buffer, per-partition decoded-frame LRU,
// on-disk segment (decoded once and inserted into the LRU). The returned
// slice is a private copy the caller may keep or modify.
func (l *Log) Read(offset int64) ([]byte, error) {
	rec, err := l.ReadShared(offset)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(rec))
	copy(out, rec)
	return out, nil
}

// ReadShared is Read without the final copy: the returned slice aliases
// the log's buffer, flushing snapshot, or decoded-frame cache and is
// READ-ONLY. Those stores never mutate a record in place (buffered
// records are owned by the buffer, cache entries are immutable and are
// dropped, never rewritten, when their segment is deleted), so the slice
// stays valid and stable for as long as the caller holds it; it just
// pins that memory. The consume path uses it because a delivery only
// reads the payload while encoding the response, so the copy per
// message was pure overhead.
func (l *Log) ReadShared(offset int64) ([]byte, error) {
	if rec, ok := l.buffer.readBuffered(offset); ok {
		return rec, nil
	}
	if rec, ok := l.readFlushingShared(offset); ok {
		return rec, nil
	}

	entry, idx, unlock, ok, err := l.indexEntryForRead(offset)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrOffsetNotFound
	}
	// Capture the segment's file handle, then release the log lock
	// BEFORE the disk read and decode. Go's RWMutex blocks new readers
	// once a writer is queued, so one slow pread under the read lock
	// stalled every consumer on the partition and the flusher's next
	// frame write. The handle stays valid: retention closes a deleted
	// segment's file (ReadAt then fails with os.ErrClosed, handled
	// below) but never reuses the *os.File, and segment base offsets
	// are never reused, so the cache key cannot collide.
	seg := l.findSegmentLocked(entry.segmentBaseOffset)
	var file *os.File
	if seg != nil {
		file = seg.file
	}
	unlock()
	if seg == nil {
		return nil, ErrCorruptRecord
	}

	key := frameKey{segmentBase: entry.segmentBaseOffset, framePos: entry.framePos}
	if recs, ok := l.frameCache.get(key); ok {
		if int(idx) < 0 || int(idx) >= len(recs) {
			return nil, ErrCorruptRecord
		}
		return recs[idx], nil
	}

	_, records, _, err := readFrameAt(file, entry.framePos, l)
	if err != nil {
		if errors.Is(err, os.ErrClosed) {
			// Retention deleted the segment between the index lookup and
			// the read. Report the offset as gone when it really is, and
			// as a transient failure otherwise (a reopen race).
			if offset < l.OldestOffset() {
				return nil, ErrOffsetNotFound
			}
			return nil, err
		}
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
	return cached[int(idx)], nil
}

// VerifyDurable re-reads the frames covering [first,last] and validates each
// frame's CRC against the on-disk bytes, without decoding. Called right after
// Sync on the commit path to confirm the durable copy is intact before the
// high-watermark advances and the WAL source is dropped. The CRC was computed
// over the stored (possibly compressed) payload at write time, so this is a
// full torn/corrupt-write check with zero decode: one CRC per frame instead
// of one decode per record.
func (l *Log) VerifyDurable(first, last int64) error {
	var buf []byte
	return l.verifyDurable(first, last, &buf)
}

// verifyDurable is VerifyDurable with a caller-owned CRC buffer, so the
// commit path (which verifies on every batch) never allocates per frame.
func (l *Log) verifyDurable(first, last int64, buf *[]byte) error {
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
		h, _, verr := verifyFrameAtBuffered(seg.file, entry.framePos, buf)
		unlock()
		if verr != nil {
			return verr
		}
		if h.recordCount <= 0 {
			return ErrCorruptRecord
		}
		off = entry.baseOffset + int64(h.recordCount)
	}
	return nil
}

// indexEntryForRead resolves the frame containing offset and returns it
// with the Log lock held (the unlock func releases it). A sealed
// segment whose sparse index was pruned is re-scanned OUTSIDE the lock:
// the scan is tens of thousands of header preads on a 64 MiB segment,
// and holding the write lock for it stalled every reader and the
// flusher. Sealed segments are immutable, so the unlocked scan is
// race-free; the result is installed under the write lock only if the
// segment is still present and nobody installed an index first.
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
	seg := l.findSegmentForOffsetLocked(offset)
	if seg == nil {
		l.rwmu.RUnlock()
		return indexEntry{}, 0, nil, false, nil
	}
	sealed := seg != l.segments[len(l.segments)-1]
	needScan := l.segmentIndexes[seg.baseOffset] == nil
	l.rwmu.RUnlock()

	var scanned []indexEntry
	if sealed && needScan {
		// Immutable file: scan without any lock. If retention closes the
		// file meanwhile the scan errors and the caller sees a read error
		// (transient), never a wrong index.
		scanned, err = l.scanSegmentIndex(seg)
		if err != nil {
			return indexEntry{}, 0, nil, false, err
		}
	}

	l.rwmu.Lock()
	if l.findSegmentLocked(seg.baseOffset) != seg {
		// Deleted by retention between the two lock sections.
		l.rwmu.Unlock()
		return indexEntry{}, 0, nil, false, nil
	}
	if l.segmentIndexes[seg.baseOffset] == nil {
		if scanned != nil {
			l.setSegmentIndexLocked(seg.baseOffset, scanned)
		} else if err := l.loadSegmentIndexLocked(seg); err != nil {
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
