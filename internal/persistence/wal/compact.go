package wal

import (
	"errors"
	"fmt"
	"os"
)

// CompactBefore deletes segment files that contain only records with
// sequence numbers below seq. A segment is deletable when it is not the
// active segment and the next segment's base proves every record in it
// is below seq; the last listed segment is therefore always kept.
func (l *Log) CompactBefore(seq uint64) error {
	l.mu.Lock()
	active := l.segmentBase
	l.mu.Unlock()

	segments, err := listSegments(l.dir)
	if err != nil {
		return err
	}
	for i, segment := range segments {
		if segment.base == active {
			continue
		}
		if i+1 >= len(segments) || segments[i+1].base > seq {
			continue
		}
		if err := os.Remove(segment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: compact segment %s: %w", segment.path, err)
		}
	}
	return nil
}
