package storage

// Partition stats from the directory alone. A topic GET, a fan-out
// cursor stats listing, or a monitoring loop must never open an idle
// partition log just to describe it: opening spawns the flusher and
// reaper goroutines, takes file descriptors, and stamps the log as
// active so idle eviction can never fire. Everything a describe needs
// is on disk: the segment files (count, sizes, oldest base offset and
// mtime) and the durable high-watermark file, which Close force-syncs,
// so for a closed log it is exact.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// PartitionDirStats describes a partition directory without an open
// Log. The zero value describes a partition that was never written.
type PartitionDirStats struct {
	// Segments is the number of segment files on disk.
	Segments int
	// OldestOffset is the base offset of the oldest segment (0 when
	// there are no segments).
	OldestOffset int64
	// SizeBytes is the total size of every segment file.
	SizeBytes int64
	// OldestSegmentAt is the Unix-seconds mtime of the oldest segment
	// file (0 when there are no segments).
	OldestSegmentAt int64
	// HighWatermark is the durable committed frontier from the HWM
	// file (0 when none was written). For a cleanly closed log this is
	// the exact visibility boundary; the record tail of a closed log is
	// not recorded separately, so callers report it as the HWM.
	HighWatermark int64
}

// StatPartitionDir reads a partition directory's stats without opening
// its log. A directory that does not exist is an empty partition, not
// an error. A segment the reaper deletes between the listing and the
// stat is skipped rather than failing the whole read.
func StatPartitionDir(dir string) (PartitionDirStats, error) {
	var st PartitionDirStats
	names, err := listSegmentFileNames(dir)
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, fmt.Errorf("storage: list segments: %w", err)
	}
	for _, name := range names {
		base, ok := parseSegmentFileName(name)
		if !ok {
			continue
		}
		fi, err := os.Stat(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return st, fmt.Errorf("storage: stat segment %s: %w", name, err)
		}
		if st.Segments == 0 {
			st.OldestOffset = base
			st.OldestSegmentAt = fi.ModTime().Unix()
		}
		st.Segments++
		st.SizeBytes += fi.Size()
	}
	hwm, _, err := ReadPersistedHighWatermark(dir)
	if err != nil {
		return st, err
	}
	st.HighWatermark = hwm
	return st, nil
}
