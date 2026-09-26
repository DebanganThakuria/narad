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
		if err := replaySegmentFrom(segment, cursor.Seq, cursorOffset(segment, cursor), noLimit, maxRecord, fn); err != nil {
			return err
		}
	}
	return nil
}

// ReplayFromCursor is the package-level ReplayFromCursor for this open
// log. It reads the active segment only up to its last synced byte, so a
// frame still being written is never parsed: in a prepared segment the
// bytes past the data are zeros or a frame half-copied into the page
// cache, not an end of file. When the cursor is already in the active
// segment, the dispatcher's steady state, it also skips listing the
// directory.
func (l *Log) ReplayFromCursor(cursor Cursor, fn func(Record, Cursor) error) error {
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
			return replayOpenSegment(file, segment, cursor.Seq, offset, durable, maxRecord, fn)
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
		if err := replaySegmentFrom(segment, cursor.Seq, cursorOffset(segment, cursor), limit, maxRecord, fn); err != nil {
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
// skipping those below from. With a limit (not noLimit) it stops at that
// byte offset, the synced size of an active segment: every frame below it
// is complete, so a zero header there is zeroed data, reported as
// corruption. Without one it reads to the end of the data, where an
// all-zero header (a prepared segment's unused space) ends the segment
// like end of file does.
func replaySegmentFrom(segment segmentInfo, from uint64, offset, limit int64, maxRecord int, fn func(Record, Cursor) error) error {
	file, err := openSegmentAt(segment.path, offset)
	if err != nil {
		return err
	}
	defer file.Close()
	return replayOpenSegment(file, segment, from, offset, limit, maxRecord, fn)
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
func replayOpenSegment(file *os.File, segment segmentInfo, from uint64, offset, limit int64, maxRecord int, fn func(Record, Cursor) error) error {
	reader := getFrameReader(file)
	defer putFrameReader(reader)
	for {
		if limit != noLimit && offset >= limit {
			return nil
		}
		record, ok, err := reader.readFrame(segment.base, offset, maxRecord)
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
		offset += frameHeaderSize + int64(len(record.Payload))
		if record.ID.Seq >= from {
			if err := fn(record, CursorAfter(record)); err != nil {
				return err
			}
		}
	}
}
