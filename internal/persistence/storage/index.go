package storage

import (
	"errors"
	"io"
	"sort"
)

func (l *Log) appendIndexLocked(entry indexEntry) {
	idx := l.segmentIndexes[entry.segmentBaseOffset]
	if idx == nil {
		idx = &segmentIndex{}
		l.segmentIndexes[entry.segmentBaseOffset] = idx
	}
	if l.shouldKeepIndexAnchor(idx.entries, entry) {
		idx.entries = append(idx.entries, entry)
	}
	l.touchSegmentIndexLocked(entry.segmentBaseOffset)
	l.pruneSegmentIndexesLocked(entry.segmentBaseOffset)
}

func (l *Log) setSegmentIndexLocked(segmentBaseOffset int64, entries []indexEntry) {
	entries = l.sparseIndexEntries(entries)
	if len(entries) == 0 {
		delete(l.segmentIndexes, segmentBaseOffset)
		return
	}
	l.segmentIndexes[segmentBaseOffset] = &segmentIndex{entries: entries}
	l.touchSegmentIndexLocked(segmentBaseOffset)
	l.pruneSegmentIndexesLocked(segmentBaseOffset)
}

func (l *Log) sparseIndexEntries(entries []indexEntry) []indexEntry {
	sparse := make([]indexEntry, 0, min(len(entries), targetMaxSegmentIndexEntries))
	for _, entry := range entries {
		sparse = l.appendSparseIndexEntry(sparse, entry)
	}
	return sparse
}

func (l *Log) appendSparseIndexEntry(entries []indexEntry, entry indexEntry) []indexEntry {
	if l.shouldKeepIndexAnchor(entries, entry) {
		return append(entries, entry)
	}
	return entries
}

func (l *Log) shouldKeepIndexAnchor(entries []indexEntry, entry indexEntry) bool {
	if len(entries) == 0 {
		return true
	}
	last := entries[len(entries)-1]
	return entry.framePos-last.framePos >= l.indexStrideBytes()
}

func (l *Log) indexStrideBytes() int64 {
	stride := int64(segmentIndexStrideBytes)
	if l.opts.SegmentBytes <= 0 {
		return stride
	}
	segmentStride := l.opts.SegmentBytes / targetMaxSegmentIndexEntries
	if l.opts.SegmentBytes%targetMaxSegmentIndexEntries != 0 {
		segmentStride++
	}
	if segmentStride > stride {
		return segmentStride
	}
	return stride
}

// findIndexLocked returns the exact frame containing offset. The
// in-memory index is sparse, so this may scan forward from the nearest
// retained anchor under the caller's segment lock.
func (l *Log) findIndexLocked(offset int64) (indexEntry, int32, bool, error) {
	seg := l.findSegmentForOffsetLocked(offset)
	if seg == nil {
		return indexEntry{}, 0, false, nil
	}

	// A recently-resolved frame near this offset lets us skip re-walking from
	// the sparse anchor — the consume pread storm. Sequential reads either land
	// inside the cached frame (zero header reads) or one frame past it.
	cached, cachedOK := l.navCache.bestAnchor(seg.baseOffset, offset)
	if cachedOK && offset < cached.baseOffset+int64(cached.recordCount) {
		return cached, int32(offset - cached.baseOffset), true, nil
	}

	idx := l.segmentIndexes[seg.baseOffset]
	if idx == nil || len(idx.entries) == 0 {
		// Sparse index not loaded yet. If the cache gives a start anchor below
		// the target, walk from it without forcing the (expensive) full index
		// load; on failure fall through to not-found so the caller lazily loads.
		if cachedOK {
			return l.resolveFromAnchorLocked(seg, cached, offset)
		}
		return indexEntry{}, 0, false, nil
	}

	anchor, ok := indexAnchorForOffset(idx.entries, offset)
	if !ok {
		if cachedOK {
			return l.resolveFromAnchorLocked(seg, cached, offset)
		}
		return indexEntry{}, 0, false, nil
	}
	// Start from whichever anchor sits closer below the target.
	if cachedOK && cached.baseOffset > anchor.baseOffset {
		anchor = cached
	}
	return l.resolveFromAnchorLocked(seg, anchor, offset)
}

// resolveFromAnchorLocked returns the entry for offset starting at anchor:
// directly when the anchor's own frame covers it, otherwise by walking forward.
// Every resolved frame is cached so the next sequential read starts from it.
func (l *Log) resolveFromAnchorLocked(seg *segment, anchor indexEntry, offset int64) (indexEntry, int32, bool, error) {
	if offset >= anchor.baseOffset && offset < anchor.baseOffset+int64(anchor.recordCount) {
		l.navCache.put(anchor)
		return anchor, int32(offset - anchor.baseOffset), true, nil
	}
	entry, idx, ok, err := l.scanSegmentFromIndexAnchorLocked(seg, anchor, offset)
	if ok && err == nil {
		l.navCache.put(entry)
	}
	return entry, idx, ok, err
}

func indexAnchorForOffset(entries []indexEntry, offset int64) (indexEntry, bool) {
	i := sort.Search(len(entries), func(i int) bool {
		return entries[i].baseOffset > offset
	})
	if i == 0 {
		return indexEntry{}, false
	}
	return entries[i-1], true
}

func (l *Log) scanSegmentFromIndexAnchorLocked(seg *segment, anchor indexEntry, offset int64) (indexEntry, int32, bool, error) {
	pos := anchor.framePos
	if anchor.frameLen > 0 {
		pos += int64(anchor.frameLen)
	}
	size := seg.sizeBytes
	for pos < size {
		// Navigation only needs frame headers to step frame-to-frame, so this
		// walks header-only (no payload read, no CRC, no decode). Decoding here
		// was the dominant consume-CPU cost (hundreds of zstd decodes per offset
		// lookup with small frames + a sparse index); even reading the payload
		// to CRC every skipped frame was the next-largest cost. The target frame
		// is CRC-validated by readFrameAt when it is actually read.
		h, end, err := frameHeaderAt(seg.file, pos)
		switch {
		case err == nil:
			if end > size {
				// Torn/incomplete tail frame: not navigable (and not yet
				// readable). Treat as not found, like a short read.
				return indexEntry{}, 0, false, nil
			}
			frameEndOffset := h.baseOffset + int64(h.recordCount)
			if offset >= h.baseOffset && offset < frameEndOffset {
				return indexEntry{
					segmentBaseOffset: seg.baseOffset,
					baseOffset:        h.baseOffset,
					recordCount:       h.recordCount,
					framePos:          pos,
					frameLen:          int32(end - pos),
				}, int32(offset - h.baseOffset), true, nil
			}
			if h.baseOffset > offset {
				return indexEntry{}, 0, false, nil
			}
			pos = end

		case errors.Is(err, io.ErrUnexpectedEOF):
			return indexEntry{}, 0, false, nil

		case errors.Is(err, errBadMagic),
			errors.Is(err, errCorrupt),
			errors.Is(err, ErrCorruptRecord):
			pos = nextMagicInSegment(seg.file, pos+1, size)

		default:
			return indexEntry{}, 0, false, err
		}
	}
	return indexEntry{}, 0, false, nil
}

func (l *Log) deleteSegmentIndexLocked(segmentBaseOffset int64) {
	delete(l.segmentIndexes, segmentBaseOffset)
}

func (l *Log) touchSegmentIndexLocked(segmentBaseOffset int64) {
	idx := l.segmentIndexes[segmentBaseOffset]
	if idx == nil {
		return
	}
	l.indexClock++
	idx.lastUsed = l.indexClock
}

func (l *Log) pruneSegmentIndexesLocked(preserveSegmentBaseOffset int64) {
	for len(l.segmentIndexes) > maxHotSegmentIndexes {
		victim, ok := l.oldestPrunableSegmentIndexLocked(preserveSegmentBaseOffset)
		if !ok {
			return
		}
		delete(l.segmentIndexes, victim)
	}
}

func (l *Log) oldestPrunableSegmentIndexLocked(preserveSegmentBaseOffset int64) (int64, bool) {
	activeBaseOffset := int64(-1)
	if len(l.segments) > 0 {
		activeBaseOffset = l.segments[len(l.segments)-1].baseOffset
	}

	var victim int64
	var oldest uint64
	found := false
	for baseOffset, idx := range l.segmentIndexes {
		if baseOffset == preserveSegmentBaseOffset || baseOffset == activeBaseOffset {
			continue
		}
		if !found || idx.lastUsed < oldest {
			victim = baseOffset
			oldest = idx.lastUsed
			found = true
		}
	}
	return victim, found
}
