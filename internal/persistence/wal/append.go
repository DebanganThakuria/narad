package wal

import (
	"errors"
	"fmt"
	"os"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// appendLocked stages a size-byte payload, produced by fill, into the
// write buffer, assigns it the next seq, and returns the batch the
// caller must wait on for durability.
// If the frame would overflow the segment, the current buffer is synced
// and the log rolls to a fresh segment first. Caller must hold mu.
func (l *Log) appendLocked(size int, fill func(dst []byte) []byte) (RecordID, *syncBatch, error) {
	if l.closed {
		return RecordID{}, nil, errors.New("wal: log closed")
	}
	if l.syncErr != nil {
		return RecordID{}, nil, l.syncErr
	}
	if l.file == nil {
		return RecordID{}, nil, errors.New("wal: active file closed")
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
