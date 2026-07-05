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

// Segment files are named <base zero-padded to 20 digits>.wal, where
// base is the seq of the first record the segment may hold.
const segmentSuffix = ".wal"

type segmentInfo struct {
	base uint64
	path string
}

// listSegments returns the segment files in dir sorted by base seq.
// A missing directory yields an empty list, not an error.
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
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// syncDir fsyncs a directory so that a newly created segment file survives a
// power loss. Without it a freshly rolled segment (and its fsynced records)
// could vanish and seqs would regress after a restart.
func syncDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open dir for sync: %w", err)
	}
	if err := handle.Sync(); err != nil {
		_ = handle.Close()
		return fmt.Errorf("wal: sync dir: %w", err)
	}
	return handle.Close()
}
