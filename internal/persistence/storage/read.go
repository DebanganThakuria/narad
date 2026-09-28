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
	// A closed log answers ErrLogClosed and nothing else. A consume still
	// holding the closed log of a retired incarnation can read an offset
	// it reserved on the successor's shard (shards are keyed by topic
	// name); "not found" or "corrupt" there reads as a gap in the
	// successor and skips its frontier past records it never delivered.
	// Every retire closes the old logs before it drops their shards, so a
	// reservation on a shard made after the drop is read here after the
	// close.
	if l.closed.Load() {
		return nil, ErrLogClosed
	}
	if !l.readSeen.Load() {
		l.readSeen.Store(true)
	}
	// Below the durable tail a record is in a synced, indexed frame, and
	// the flusher has already released it from the write buffer and the
	// flushing snapshot (syncIfNeeded clears them right after moving the
	// durable tail, and the high-watermark only advances after that).
	// Every consume read is below the high-watermark, which never passes
	// the durable tail, so it skips two probes that cannot hit and whose
	// mutexes it would share with the appenders and the flusher. A stale
	// (lower) durable tail only means probing anyway.
	if offset >= l.durableTail.Load() {
		if rec, ok := l.buffer.readBuffered(offset); ok {
			return rec, nil
		}
		if rec, ok := l.readFlushingShared(offset); ok {
			return rec, nil
		}
	}
	// A sealed segment's file handle can be released between the index
	// lookup and the read (its index left the hot set, or retention
	// deleted it). The read then fails with os.ErrClosed; re-resolving
	// reopens the file or reports the offset as gone.
	const maxAttempts = 3
	for attempt := 1; ; attempt++ {
		rec, err := l.readSegmentShared(offset)
		if err == nil || !errors.Is(err, os.ErrClosed) || attempt == maxAttempts {
			return rec, err
		}
	}
}

func (l *Log) readSegmentShared(offset int64) ([]byte, error) {
	entry, idx, writeLocked, ok, err := l.lockIndexEntry(offset)
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
	// frame write. The handle stays valid until the segment is deleted
	// or its handle released (ReadAt then fails with os.ErrClosed,
	// handled by the caller's retry); segment base offsets are never
	// reused, so the cache key cannot collide.
	seg := l.findSegmentLocked(entry.segmentBaseOffset)
	var file *os.File
	if seg != nil {
		if l.closed.Load() {
			// Close released the handles; a lazy reopen here would serve
			// a closed log (and leak the descriptor).
			err = ErrLogClosed
		} else {
			file, err = seg.handle()
		}
	}
	l.unlockIndex(writeLocked)
	if seg == nil {
		return nil, ErrCorruptRecord
	}
	if err != nil {
		return nil, err
	}

	key := frameKey{segmentBase: entry.segmentBaseOffset, framePos: entry.framePos}
	if recs, ok := l.frameCache.get(key); ok {
		if int(idx) < 0 || int(idx) >= len(recs) {
			return nil, ErrCorruptRecord
		}
		return recs[idx], nil
	}

	h, records, _, err := readFrameAt(file, entry.framePos, l)
	if err != nil {
		if errors.Is(err, os.ErrClosed) {
			// Retention deleted the segment between the index lookup and
			// the read. Report the offset as gone when it really is, and
			// let the caller re-resolve otherwise.
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

	// The records are slices of the buffer this read decoded into, which
	// nothing else holds (see readFrameAt), so they are cached as they
	// are. The cache accounts for the whole buffer they pin.
	l.frameCache.putSized(key, records, int(h.uncompressed))
	return records[idx], nil
}

// VerifyDurable re-reads the frames covering [first,last] and validates each
// frame's CRC against the on-disk bytes, without decoding. Called right after
// Sync on the commit path to confirm the durable copy is intact before the
// high-watermark advances and the WAL source is dropped. The CRC was computed
// over the stored (possibly compressed) payload at write time, so this is a
// full torn/corrupt-write check with zero decode: one CRC per frame instead
// of one decode per record.
func (l *Log) VerifyDurable(first, last int64) error {
	buf := verifyChunks.get()
	defer verifyChunks.put(buf)
	return l.verifyDurable(first, last, buf)
}

// verifyDurable is VerifyDurable with a caller-provided CRC buffer, so
// the commit path (which verifies on every batch) never allocates per
// frame.
func (l *Log) verifyDurable(first, last int64, buf *[]byte) error {
	for off := first; off <= last; {
		entry, _, writeLocked, ok, err := l.lockIndexEntry(off)
		if err != nil {
			return err
		}
		if !ok {
			return ErrOffsetNotFound
		}
		seg := l.findSegmentLocked(entry.segmentBaseOffset)
		if seg == nil {
			l.unlockIndex(writeLocked)
			return ErrCorruptRecord
		}
		file, err := seg.handle()
		if err != nil {
			l.unlockIndex(writeLocked)
			return err
		}
		h, _, verr := verifyFrameAtBuffered(file, entry.framePos, buf)
		l.unlockIndex(writeLocked)
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

// indexEntryForRead is lockIndexEntry with the release as a func, for
// callers off the hot path.
func (l *Log) indexEntryForRead(offset int64) (indexEntry, int32, func(), bool, error) {
	entry, idx, writeLocked, ok, err := l.lockIndexEntry(offset)
	if err != nil || !ok {
		return indexEntry{}, 0, nil, ok, err
	}
	if writeLocked {
		return entry, idx, l.rwmu.Unlock, true, nil
	}
	return entry, idx, l.rwmu.RUnlock, true, nil
}

// unlockIndex releases the Log lock lockIndexEntry returned with.
func (l *Log) unlockIndex(writeLocked bool) {
	if writeLocked {
		l.rwmu.Unlock()
		return
	}
	l.rwmu.RUnlock()
}

// lockIndexEntry resolves the frame containing offset and returns it
// with the Log lock held: the write side when writeLocked, else the read
// side, released by unlockIndex. It reports which side instead of
// returning the unlock method as a func value, because that value
// escapes and cost every read an allocation. Nothing is held when ok is
// false or err is set.
//
// A sealed segment whose sparse index was pruned is re-scanned OUTSIDE
// the lock: the scan is tens of thousands of header preads on a 64 MiB
// segment, and holding the write lock for it stalled every reader and
// the flusher. Sealed segments are immutable, so the unlocked scan is
// race-free; the result is installed under the write lock only if the
// segment is still present and nobody installed an index first.
func (l *Log) lockIndexEntry(offset int64) (entry indexEntry, idx int32, writeLocked, ok bool, err error) {
	l.rwmu.RLock()
	entry, idx, ok, err = l.findIndexLocked(offset)
	if err != nil {
		l.rwmu.RUnlock()
		return indexEntry{}, 0, false, false, err
	}
	if ok {
		return entry, idx, false, true, nil
	}
	seg := l.findSegmentForOffsetLocked(offset)
	if seg == nil {
		l.rwmu.RUnlock()
		return indexEntry{}, 0, false, false, nil
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
			return indexEntry{}, 0, false, false, err
		}
	}

	l.rwmu.Lock()
	if l.findSegmentLocked(seg.baseOffset) != seg {
		// Deleted by retention between the two lock sections.
		l.rwmu.Unlock()
		return indexEntry{}, 0, false, false, nil
	}
	if l.segmentIndexes[seg.baseOffset] == nil {
		if scanned != nil {
			l.setSegmentIndexLocked(seg.baseOffset, scanned)
		} else if err = l.loadSegmentIndexLocked(seg); err != nil {
			l.rwmu.Unlock()
			return indexEntry{}, 0, false, false, err
		}
	}
	entry, idx, ok, err = l.findIndexLocked(offset)
	if err != nil {
		l.rwmu.Unlock()
		return indexEntry{}, 0, false, false, err
	}
	if !ok {
		l.rwmu.Unlock()
		return indexEntry{}, 0, false, false, nil
	}
	return entry, idx, true, true, nil
}
