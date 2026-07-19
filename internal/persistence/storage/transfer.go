package storage

// Partition transfer primitives — the read side of rebalance/decommission.
// A partition move ships a partition's segment files verbatim to a new
// owner, preserving offsets (the filename and frame headers encode the
// base offset). These functions let the source node enumerate and read
// its segments without disturbing the live Log, and the destination
// install them into a staging directory that recovers into a
// byte-identical copy.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SegmentInfo describes one on-disk segment for the transfer protocol.
// Sealed segments are immutable and can be copied once; the single
// unsealed (active) segment is still being appended to and must be
// tailed to the source's high-watermark.
type SegmentInfo struct {
	BaseOffset int64 `json:"base_offset"`
	SizeBytes  int64 `json:"size_bytes"`
	Sealed     bool  `json:"sealed"`
}

// ListPartitionSegments enumerates a partition directory's segments in
// base-offset order. The highest-base-offset segment is the active
// (unsealed) one; all earlier segments are sealed and immutable. Reads
// the directory directly — it does not require or disturb an open Log,
// so the owner can serve it while still writing.
func ListPartitionSegments(partitionDir string) ([]SegmentInfo, error) {
	names, err := listSegmentFileNames(partitionDir)
	if err != nil {
		return nil, err
	}
	out := make([]SegmentInfo, 0, len(names))
	for i, name := range names {
		base, ok := parseSegmentFileName(name)
		if !ok {
			continue
		}
		fi, err := os.Stat(filepath.Join(partitionDir, name))
		if err != nil {
			return nil, fmt.Errorf("storage: stat segment %s: %w", name, err)
		}
		out = append(out, SegmentInfo{
			BaseOffset: base,
			SizeBytes:  fi.Size(),
			// The last (highest base offset) segment is the active one.
			Sealed: i < len(names)-1,
		})
	}
	return out, nil
}

// ReadSegmentRange reads len bytes at offset from the segment file with
// the given base offset in partitionDir. Used to stream a segment to a
// destination in bounded chunks. A read past EOF returns the available
// bytes (short read), never an error — the active segment grows under a
// concurrent writer, so callers re-list to learn the final size.
func ReadSegmentRange(partitionDir string, baseOffset, at, length int64) ([]byte, error) {
	path := filepath.Join(partitionDir, segmentFileName(baseOffset))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, at)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

// WriteSegmentFile installs received bytes as a segment file in
// partitionDir (creating the directory if needed). The destination
// writes each fetched segment here, then opens the directory as a Log
// to recover a byte-identical copy. Written 0644 to match openSegment.
func WriteSegmentFile(partitionDir string, baseOffset int64, data []byte) error {
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(partitionDir, segmentFileName(baseOffset))
	return os.WriteFile(path, data, 0o644)
}

// AppendToSegmentFile appends bytes to an existing staging segment file
// (used while tailing the source's active segment as it grows). Creates
// the file if absent.
func AppendToSegmentFile(partitionDir string, baseOffset int64, data []byte) error {
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(partitionDir, segmentFileName(baseOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}
