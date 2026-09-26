package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// Replay invokes fn for every record with seq >= from, in order.
func Replay(dir string, from uint64, maxRecord int, fn func(Record) error) error {
	if fn == nil {
		return nil
	}
	return ReplayFromCursor(dir, Cursor{Seq: from}, maxRecord, func(record Record, _ Cursor) error {
		return fn(record)
	})
}

// ReplayFromCursor invokes fn for every record at or after cursor, in
// order, passing alongside each record the cursor to resume just past it.
// It reads the directory as it finds it, which is right for a log that is
// not open. For a log that is being appended to use Log.ReplayFromCursor,
// which never reads past what has been synced.
func ReplayFromCursor(dir string, cursor Cursor, maxRecord int, fn func(Record, Cursor) error) error {
	if fn == nil {
		return nil
	}
	if maxRecord <= 0 {
		maxRecord = defaultMaxRecord
	}
	segments, err := listSegments(dir)
	if err != nil {
		return err
	}
	for i, segment := range segments {
		if shouldSkipSegment(segments, i, cursor) {
			continue
		}
		if err := replaySegmentFrom(segment, cursor.Seq, cursorOffset(segment, cursor), noLimit, maxRecord, nil, fn); err != nil {
			return err
		}
	}
	return nil
}

// Peek is a replay filter consulted for each record at or after the
// replay's cursor before its payload is read, with the record's id and
// the cursor just past it. Returning skip passes over the record: its
// payload is checksummed as it streams through the read buffer, exactly
// as a delivered record's is, but never allocated, and fn is not called
// for it. A non-nil error stops the replay and is returned. Peek only
// ever sees complete frames.
type Peek func(id RecordID, next Cursor) (skip bool, err error)

// ReplayFromCursor is the package-level ReplayFromCursor for this open
// log. It reads the active segment only up to its last synced byte, so a
// frame still being written is never parsed: in a prepared segment the
// bytes past the data are zeros or a frame half-copied into the page
// cache, not an end of file. When the cursor is already in the active
// segment, the dispatcher's steady state, it also skips listing the
// directory.
func (l *Log) ReplayFromCursor(cursor Cursor, fn func(Record, Cursor) error) error {
	return l.ReplayFromCursorPeek(cursor, nil, fn)
}

// ReplayFromCursorPeek is ReplayFromCursor with a Peek filter (nil for
// none). A caller that already knows it will pass over a record (the
// dispatcher, for records it committed on an earlier pass) skips it
// before paying for its allocation and decoding.
func (l *Log) ReplayFromCursorPeek(cursor Cursor, peek Peek, fn func(Record, Cursor) error) error {
	if fn == nil {
		return nil
	}
	l.mu.Lock()
	active, durable := l.segmentBase, l.durableSize
	l.mu.Unlock()
	maxRecord := l.opts.MaxRecord

	if cursor.SegmentBase == active {
		segment := segmentInfo{base: active, path: segmentPath(l.dir, active)}
		offset := cursorOffset(segment, cursor)
		file, err := openSegmentAt(segment.path, offset)
		if err == nil {
			defer file.Close()
			return replayOpenSegment(file, segment, cursor.Seq, offset, durable, maxRecord, peek, fn)
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		// Rolled and compacted away since the snapshot: list the
		// directory to find the records after the cursor.
	}

	segments, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	for i, segment := range segments {
		if segment.base > active {
			// Rolled in after the snapshot, so nothing about its synced
			// size is known; the next call reads it.
			break
		}
		if shouldSkipSegment(segments, i, cursor) {
			continue
		}
		limit := noLimit
		if segment.base == active {
			limit = durable
		}
		if err := replaySegmentFrom(segment, cursor.Seq, cursorOffset(segment, cursor), limit, maxRecord, peek, fn); err != nil {
			return err
		}
	}
	return nil
}

// cursorOffset is where replay of segment starts for cursor.
func cursorOffset(segment segmentInfo, cursor Cursor) int64 {
	if cursor.SegmentBase == segment.base && cursor.Offset > 0 {
		return cursor.Offset
	}
	return 0
}

// shouldSkipSegment reports whether segment i cannot contain any record
// at or after cursor: it either precedes the cursor's segment, or the
// next segment's base proves all its records are below cursor.Seq.
func shouldSkipSegment(segments []segmentInfo, i int, cursor Cursor) bool {
	segment := segments[i]
	if cursor.SegmentBase > 0 && segment.base < cursor.SegmentBase {
		return true
	}
	if i+1 < len(segments) && segments[i+1].base <= cursor.Seq {
		return true
	}
	return false
}

// noLimit is replaySegmentFrom's limit for a segment read to its end.
const noLimit int64 = -1

// replaySegmentFrom hands fn the records of one segment from offset on,
// passing over those below from and those peek (if not nil) skips, all
// without allocating their payloads. With a limit (not noLimit) it stops
// at that byte offset, the synced size of an active segment: every frame
// below it is complete, so a zero header there is zeroed data, reported
// as corruption. Without one it reads to the end of the data, where an
// all-zero header (a prepared segment's unused space) ends the segment
// like end of file does.
func replaySegmentFrom(segment segmentInfo, from uint64, offset, limit int64, maxRecord int, peek Peek, fn func(Record, Cursor) error) error {
	file, err := openSegmentAt(segment.path, offset)
	if err != nil {
		return err
	}
	defer file.Close()
	return replayOpenSegment(file, segment, from, offset, limit, maxRecord, peek, fn)
}

// openSegmentAt opens a segment for reading, positioned at offset.
func openSegmentAt(path string, offset int64) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open segment: %w", err)
	}
	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("wal: seek segment: %w", err)
		}
	}
	return file, nil
}

// replayOpenSegment is replaySegmentFrom over a file already positioned
// at offset.
func replayOpenSegment(file *os.File, segment segmentInfo, from uint64, offset, limit int64, maxRecord int, peek Peek, fn func(Record, Cursor) error) error {
	// Peek must only see complete frames. Below a limit every frame is;
	// without one, a frame is complete if it ends within the file (a
	// sealed segment can end in a torn frame after a lying-disk crash).
	end := limit
	if peek != nil && end == noLimit {
		info, err := file.Stat()
		if err != nil {
			return fmt.Errorf("wal: stat segment: %w", err)
		}
		end = info.Size()
	}
	reader := getFrameReader(file)
	defer putFrameReader(reader)
	for {
		if limit != noLimit && offset >= limit {
			return nil
		}
		h, ok, err := reader.readHeader(offset, maxRecord)
		if errors.Is(err, errZeroHeader) {
			if limit == noLimit {
				return nil
			}
			return fmt.Errorf("wal: zeroed frame at offset %d of %s, below its synced size %d: %w", offset, segment.path, limit, errCorruptFrame)
		}
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		id := RecordID{SegmentBase: segment.base, Offset: offset, Seq: h.seq}
		next := Cursor{SegmentBase: segment.base, Offset: offset + frameHeaderSize + int64(h.size), Seq: h.seq + 1}
		skip := h.seq < from
		if !skip && peek != nil {
			if end != noLimit && next.Offset > end {
				return nil // a torn final frame: the end of the data
			}
			if skip, err = peek(id, next); err != nil {
				return err
			}
		}
		if skip {
			ok, err = reader.skipPayload(h, offset)
		} else {
			var payload []byte
			if payload, ok, err = reader.readPayload(h, offset); err == nil && ok {
				err = fn(Record{ID: id, Payload: payload}, next)
			}
		}
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		offset = next.Offset
	}
}
