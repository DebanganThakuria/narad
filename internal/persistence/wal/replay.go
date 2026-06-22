package wal

import (
	"fmt"
	"io"
	"os"
)

func Replay(dir string, from uint64, maxRecord int, fn func(Record) error) error {
	if fn == nil {
		return nil
	}
	return ReplayFromCursor(dir, Cursor{Seq: from}, maxRecord, func(record Record, _ Cursor) error {
		return fn(record)
	})
}

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
		offset := int64(0)
		if cursor.SegmentBase == segment.base && cursor.Offset > 0 {
			offset = cursor.Offset
		}
		if err := replaySegmentFrom(segment, cursor.Seq, offset, maxRecord, fn); err != nil {
			return err
		}
	}
	return nil
}

func replaySegmentFrom(segment segmentInfo, from uint64, offset int64, maxRecord int, fn func(Record, Cursor) error) error {
	file, err := os.Open(segment.path)
	if err != nil {
		return fmt.Errorf("wal: open segment: %w", err)
	}
	defer file.Close()
	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("wal: seek segment: %w", err)
		}
	}

	for {
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("wal: segment offset: %w", err)
		}
		record, ok, err := readFrame(file, segment.base, offset, maxRecord)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if record.ID.Seq >= from {
			if err := fn(record, CursorAfter(record)); err != nil {
				return err
			}
		}
	}
}

func CursorAfter(record Record) Cursor {
	return Cursor{
		SegmentBase: record.ID.SegmentBase,
		Offset:      record.ID.Offset + frameHeaderSize + int64(len(record.Payload)),
		Seq:         record.ID.Seq + 1,
	}
}
