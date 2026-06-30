package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const segmentSuffix = ".wal"

type segmentInfo struct {
	base uint64
	path string
}

// shouldSkipSegment reports whether segments[i] lies entirely before the
// replay cursor and can be skipped. It takes the whole slice rather than a
// single segment because the second test peeks at the next segment's base:
// when even that base is at or below cursor.Seq, every record in segments[i]
// predates the cursor.
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

func listSegments(dir string) ([]segmentInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: list segments: %w", err)
	}
	segments := make([]segmentInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), segmentSuffix) {
			continue
		}
		baseText := strings.TrimSuffix(entry.Name(), segmentSuffix)
		base, err := strconv.ParseUint(baseText, 10, 64)
		if err != nil {
			continue
		}
		segments = append(segments, segmentInfo{base: base, path: filepath.Join(dir, entry.Name())})
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].base < segments[j].base })
	return segments, nil
}

func segmentPath(dir string, base uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, segmentSuffix))
}

func createEmptySegment(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create first segment: %w", err)
	}
	return file.Close()
}
