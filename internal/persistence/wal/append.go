package wal

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// appendLocked stages a size-byte payload, produced by fill, into the
// write buffer, assigns it the next seq, and returns the batch the
// caller must wait on for durability.
// If the frame would overflow the segment, the current buffer is synced
// and the log rolls to a fresh segment first. Caller must hold mu.
func (l *Log) appendLocked(size int, fill func(dst []byte) []byte) (RecordID, *syncBatch, error) {
	if err := l.appendableLocked(); err != nil {
		return RecordID{}, nil, err
	}

	frameSize := frameHeaderSize + size
	if l.segmentSize > 0 && l.segmentSize+int64(frameSize) > l.opts.SegmentBytes {
		batch, err := l.syncLocked()
		completeBatch(batch, err)
		if err != nil {
			return RecordID{}, nil, err
		}
		if err := l.rollLocked(true); err != nil {
			return RecordID{}, nil, err
		}
	}

	if l.pending == nil {
		l.pending = &syncBatch{done: make(chan struct{})}
	}
	batch := l.pending
	seq := l.nextSeq
	id := RecordID{SegmentBase: l.segmentBase, Offset: l.segmentSize, Seq: seq}
	buffer, err := appendFrameWith(l.writeBuffer, seq, size, fill)
	if err != nil {
		return RecordID{}, nil, err
	}
	l.writeBuffer = buffer
	l.segmentSize += int64(frameSize)
	l.nextSeq++
	l.requestPrepLocked()
	return id, batch, nil
}

// appendableLocked reports why the log cannot take an append now, or
// nil. Caller must hold mu.
func (l *Log) appendableLocked() error {
	if l.closed {
		return errors.New("wal: log closed")
	}
	if l.syncErr != nil {
		return l.syncErr
	}
	if l.file == nil {
		return errors.New("wal: active file closed")
	}
	return nil
}

// appendManyLocked is appendLocked for len(sizes) records, record i
// produced by fill(i, ...), staged back to back in slice order. It
// returns the batch the last record went into: every record before it
// is in that batch too, or was synced inline by a segment roll between
// two records (which appendLocked's rule triggers exactly as it would
// for single appends), so that batch completing means the whole slice
// is durable.
//
// A fill that fails or panics withdraws the records staged since the
// start of the call or its last inline sync, whichever is later: they
// were never written, so their bytes and seqs are handed back. Records
// an inline sync already wrote cannot be withdrawn; see AppendManyWith.
// Caller must hold mu.
func (l *Log) appendManyLocked(sizes []int, fill func(i int, dst []byte) []byte) ([]RecordID, *syncBatch, error) {
	if err := l.appendableLocked(); err != nil {
		return nil, nil, err
	}

	ids := make([]RecordID, len(sizes))
	mark := l.markLocked()
	staged := false
	defer func() {
		if !staged {
			l.withdrawLocked(mark)
		}
	}()
	// One allocation for the whole batch instead of the doublings of
	// staging frame by frame (usually none: the recycled buffers are
	// already sized to earlier batches).
	need := 0
	for _, size := range sizes {
		need += frameHeaderSize + size
	}
	l.writeBuffer = slices.Grow(l.writeBuffer, need)

	for i, size := range sizes {
		frameSize := frameHeaderSize + size
		if l.segmentSize > 0 && l.segmentSize+int64(frameSize) > l.opts.SegmentBytes {
			synced, err := l.syncLocked()
			completeBatch(synced, err)
			// Written, or latched as failed: nothing staged so far can
			// be withdrawn any more.
			mark = l.markLocked()
			if err != nil {
				return nil, nil, err
			}
			if err := l.rollLocked(true); err != nil {
				return nil, nil, err
			}
			mark = l.markLocked()
		}

		if l.pending == nil {
			l.pending = &syncBatch{done: make(chan struct{})}
		}
		seq := l.nextSeq
		ids[i] = RecordID{SegmentBase: l.segmentBase, Offset: l.segmentSize, Seq: seq}
		buffer, err := appendFrameWith(l.writeBuffer, seq, size, func(dst []byte) []byte { return fill(i, dst) })
		if err != nil {
			return nil, nil, fmt.Errorf("record %d: %w", i, err)
		}
		l.writeBuffer = buffer
		l.segmentSize += int64(frameSize)
		l.nextSeq++
		l.requestPrepLocked()
	}
	staged = true
	return ids, l.pending, nil
}

// stagedMark is the append state appendManyLocked can roll back to:
// how much of the write buffer, the segment and the seq space was taken,
// and the pending batch, before it staged anything unwritten.
type stagedMark struct {
	buffered    int
	segmentSize int64
	nextSeq     uint64
	pending     *syncBatch
}

// markLocked records the append state for withdrawLocked. Caller must
// hold mu.
func (l *Log) markLocked() stagedMark {
	return stagedMark{
		buffered:    len(l.writeBuffer),
		segmentSize: l.segmentSize,
		nextSeq:     l.nextSeq,
		pending:     l.pending,
	}
}

// withdrawLocked drops everything staged since m. Valid only while mu
// has been held since m was taken and nothing was written or rolled in
// between: the bytes after m.buffered are then exactly the withdrawn
// frames, and no one else holds their seqs. Caller must hold mu.
func (l *Log) withdrawLocked(m stagedMark) {
	l.writeBuffer = l.writeBuffer[:m.buffered]
	l.segmentSize = m.segmentSize
	l.nextSeq = m.nextSeq
	l.pending = m.pending
}

// rollLocked closes the active segment and creates the next one, whose
// base is the next unassigned seq. With usePrepared set (the append
// path) a prepared spare becomes the new segment when one is ready; the
// compaction rotation passes false so an idle log does not trade a small
// empty segment for a full-size prepared one. Caller must hold mu;
// fileOps is taken so an in-flight flush cannot race the file swap.
func (l *Log) rollLocked(usePrepared bool) error {
	l.fileOps.Lock()
	defer l.fileOps.Unlock()

	// A prepared segment is cut back to its data, and the new size
	// synced, before its successor exists, so a sealed segment never
	// keeps a zero tail: binaries that predate preparation read a bad
	// header in a sealed segment as corruption and refuse to open. Until
	// the successor exists this is still the last segment, whose zero
	// tail every binary truncates on open. Once per SegmentBytes.
	if l.activePrepared {
		if err := syncfile.Truncate(l.file, l.segmentSize); err != nil {
			return fmt.Errorf("wal: trim prepared segment: %w", err)
		}
		if err := syncfile.Sync(l.file); err != nil {
			return fmt.Errorf("wal: sync trimmed segment: %w", err)
		}
		l.activePrepared = false
	}

	// Create the next segment before touching the current one: if the
	// create or the directory sync fails (ENOSPC, EIO) the active file
	// stays open and the failed append is the only casualty; the next
	// append retries the roll. Closing first left a closed descriptor
	// as the active file, so every later append failed and latched the
	// log until a restart, long after the disk had space again.
	path := segmentPath(l.dir, l.nextSeq)
	file, prepared, err := l.createSegmentLocked(path, usePrepared)
	if err != nil {
		return err
	}
	// Make the new segment file durable before any appends can target it.
	if err := syncDir(l.dir); err != nil {
		// Nothing left behind: the retry's O_EXCL create would
		// otherwise fail on this very file.
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := l.file.Close(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("wal: close rolled segment: %w", err)
	}
	l.file = file
	l.segmentBase = l.nextSeq
	l.segmentSize = 0
	l.durableSize = 0
	l.activePrepared = prepared
	// The new segment asks for its own successor at its half-way point.
	l.prepRequested = false
	// The segment just sealed may become deletable: force the next
	// CompactBefore to list the directory again.
	l.compactFloor = 0
	return nil
}

// signalSync wakes the sync loop without blocking; a wakeup already in
// flight covers this record too.
func (l *Log) signalSync() {
	select {
	case l.wakeup <- struct{}{}:
	default:
	}
}
