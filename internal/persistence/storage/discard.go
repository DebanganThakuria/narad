package storage

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// discardUncommittedTail is what a failed commit leaves behind: nothing.
//
// Every record above the high-watermark is acked to a producer only by
// the ingress WAL, never by this log, and the WAL re-commits any batch
// whose CommitDurable did not return success. If the failed batch's
// records stayed in the log (in the flushing snapshot for a write
// failure, or already written and even fsynced for an fsync, verify,
// roll or high-watermark persist failure), the retry would append a
// second copy at fresh offsets and its commit would advance the
// high-watermark past both: every record of the batch delivered twice,
// permanently, without any crash. So, on the flusher goroutine and
// before the error reaches the caller:
//
//  1. the active segment is truncated back to the first frame at or
//     above the high-watermark (and the truncate fsynced), unless the
//     log is poisoned, in which case the file is left for the reopen to
//     rescan;
//  2. the segment's tail, its index anchors and cached frames above the
//     cut are dropped, and the durable tail moves back to the cut;
//  3. the flushing snapshot and the write buffer are emptied and the
//     buffer's next offset rewound to the cut, so the retry is assigned
//     the offsets the batch had.
//
// A hidden tail that a crash left in an earlier, sealed segment is out
// of reach (sealed segments are immutable) and keeps the documented
// crash semantics: duplicates, never loss. The lazy segment roll
// guarantees that a batch committed by this process is never in that
// position.
//
// Lock ordering: rwmu → buffer.mu → flushingMu.
func (f *flusher) discardUncommittedTail(cause error, truncateDisk bool) {
	l := f.log
	l.rwmu.Lock()
	defer l.rwmu.Unlock()
	l.buffer.mu.Lock()
	defer l.buffer.mu.Unlock()
	l.flushingMu.Lock()
	defer l.flushingMu.Unlock()

	if len(l.segments) == 0 {
		return
	}
	active := l.segments[len(l.segments)-1]
	hwm := l.highWatermark.Load()
	cut := active.nextOffset
	truncated := false

	if truncateDisk && hwm < active.nextOffset {
		boundary, pos := l.frameBoundaryAtOrAboveLocked(active, hwm)
		if pos < active.sizeBytes {
			if err := active.truncate(pos); err != nil {
				// The frames stay on disk above the high-watermark as a
				// hidden tail (crash semantics); offsets cannot move
				// back below them.
				l.logger.Error("storage: truncate of uncommitted tail failed; tail stays hidden on disk",
					"dir", l.dir, "offset", boundary, "pos", pos, "err", err)
				l.countError("discard_truncate")
			} else {
				truncated = true
				cut = boundary
				active.nextOffset = boundary
				if boundary == active.baseOffset {
					active.firstWriteAt = time.Time{}
					active.lastWriteAt = time.Time{}
				}
				l.truncateIndexLocked(active.baseOffset, pos)
				l.frameCache.invalidateSegment(active.baseOffset)
				l.navCache.invalidateSegment(active.baseOffset)
			}
		}
	}

	if truncated || (truncateDisk && f.unsyncedBytes.Load() == 0) {
		// Everything left in the file is synced: the truncate fsynced it,
		// or nothing unsynced was ever written.
		l.durableTail.Store(cut)
		f.unsyncedBytes.Store(0)
	} else if l.durableTail.Load() > cut {
		l.durableTail.Store(cut)
	}

	dropped := l.buffer.nextOffset - cut
	l.resetFlushingLocked()
	l.buffer.records = nil
	l.buffer.baseOffset = cut
	l.buffer.nextOffset = cut
	l.buffer.bytes = 0
	l.buffer.firstAt = time.Time{}

	if dropped > 0 {
		l.logger.Warn("storage: commit failed; discarded uncommitted tail for the ingress WAL to re-commit",
			"dir", l.dir, "from_offset", cut, "records", dropped, "truncated_file", truncated, "err", cause)
		l.countError("commit_discard")
	}
}

// frameBoundaryAtOrAboveLocked returns the offset and file position of
// the first frame in seg whose base offset is at or above offset, or
// (seg.nextOffset, seg.sizeBytes) when no such frame exists. A frame
// that straddles offset stays whole: frames are the unit of truncation.
// Caller must hold rwmu.
func (l *Log) frameBoundaryAtOrAboveLocked(seg *segment, offset int64) (int64, int64) {
	if offset <= seg.baseOffset {
		return seg.baseOffset, 0
	}
	if offset >= seg.nextOffset {
		return seg.nextOffset, seg.sizeBytes
	}
	pos := int64(0)
	if idx := l.segmentIndexes[seg.baseOffset]; idx != nil {
		if anchor, ok := indexAnchorForOffset(idx.entries, offset); ok {
			pos = anchor.framePos
		}
	}
	file, err := seg.handle()
	if err != nil {
		return seg.nextOffset, seg.sizeBytes
	}
	for pos < seg.sizeBytes {
		h, end, err := frameHeaderAt(file, pos)
		if err != nil || end > seg.sizeBytes {
			if errors.Is(err, io.ErrUnexpectedEOF) || end > seg.sizeBytes {
				// A torn frame at the tail is itself uncommitted.
				return seg.nextOffset, pos
			}
			return seg.nextOffset, seg.sizeBytes
		}
		if h.baseOffset >= offset {
			return h.baseOffset, pos
		}
		pos = end
	}
	return seg.nextOffset, seg.sizeBytes
}

// truncateIndexLocked drops the sparse index anchors of a segment at or
// past file position pos. Caller must hold rwmu.
func (l *Log) truncateIndexLocked(segmentBaseOffset, pos int64) {
	idx := l.segmentIndexes[segmentBaseOffset]
	if idx == nil {
		return
	}
	n := len(idx.entries)
	for n > 0 && idx.entries[n-1].framePos >= pos {
		n--
	}
	idx.entries = idx.entries[:n]
}

// ErrLogPoisoned reports that an fdatasync on this log's active segment
// failed earlier. The error is latched: the log refuses every later
// append and commit until it is reopened, because the failed pages may
// already be gone from the page cache and a later "successful" fsync
// would not bring them back. Reads of committed records keep working.
var ErrLogPoisoned = errors.New("storage: log poisoned by fsync failure")

// poison latches err as the log's permanent failure and returns the
// wrapped error every later operation reports. Idempotent: the first
// failure wins.
func (l *Log) poison(err error) error {
	l.poisonMu.Lock()
	defer l.poisonMu.Unlock()
	if l.poisonErr != nil {
		return l.poisonErr
	}
	l.poisonErr = fmt.Errorf("%w: %w", ErrLogPoisoned, err)
	l.logger.Error("storage: fsync failed; log poisoned until reopened",
		"dir", l.dir, "durable_tail", l.durableTail.Load(), "err", err)
	l.countError("fsync_poisoned")
	return l.poisonErr
}

// poisoned returns the latched fsync error, or nil.
func (l *Log) poisoned() error {
	l.poisonMu.Lock()
	defer l.poisonMu.Unlock()
	return l.poisonErr
}

// Poisoned reports the latched fsync error of this log, or nil. The
// owner of the log (the partition-log registry) can use it to decide to
// close and reopen the log.
func (l *Log) Poisoned() error { return l.poisoned() }
