package wal

import (
	"errors"
	"fmt"
	"os"
)

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
		if i+1 >= len(segments) {
			continue
		}
		if segments[i+1].base > seq {
			continue
		}
		if err := os.Remove(segment.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: compact segment %s: %w", segment.path, err)
		}
	}
	return nil
}
