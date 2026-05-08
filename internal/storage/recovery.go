package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// recover walks every segment file in the partition directory in
// base-offset order, populating the offset → frame index. Returns
// the next offset to assign for new records.
//
// Per-segment scan rules:
//
//   - Mid-file corruption: resync to next 0xCAFE magic and continue.
//     Bad frames' offsets become permanent gaps. The file is NOT
//     truncated.
//
//   - Torn tail at EOF on the active (last) segment: truncate to the
//     last valid frame boundary so future appends start clean.
//
//   - Torn tail on a sealed segment: leave the file alone. A torn
//     tail there means a crash mid-roll; bytes past the tear are
//     lost either way and truncating would destroy a clean later
//     segment's invariants.
//
// An empty directory is initialised with one fresh segment at base
// offset 0.
func (l *Log) recover() (int64, error) {
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return 0, fmt.Errorf("storage: ensure partition dir: %w", err)
	}

	names, err := listSegmentFileNames(l.dir)
	if err != nil {
		return 0, fmt.Errorf("storage: list segments: %w", err)
	}

	if len(names) == 0 {
		seg, err := createSegment(l.dir, 0)
		if err != nil {
			return 0, err
		}
		l.segments = []*segment{seg}
		return 0, nil
	}

	var nextOffset int64
	for i, name := range names {
		baseOffset, _ := parseSegmentFileName(name)
		path := l.dir + "/" + name
		seg, err := openSegment(path, baseOffset)
		if err != nil {
			return 0, fmt.Errorf("storage: open segment %s: %w", name, err)
		}
		isActive := i == len(names)-1
		if err := l.walkSegment(seg, isActive, &nextOffset); err != nil {
			_ = seg.close()
			return 0, err
		}
		l.segments = append(l.segments, seg)
	}
	return nextOffset, nil
}

func (l *Log) walkSegment(seg *segment, isActive bool, nextOffset *int64) error {
	pos := int64(0)
	size := seg.sizeBytes

	for pos < size {
		h, records, end, err := readFrameAt(seg.file, pos, l)

		switch {
		case err == nil:
			for i := range records {
				offset := h.baseOffset + int64(i)
				l.index[offset] = indexEntry{
					segmentBaseOffset: seg.baseOffset,
					framePos:          pos,
					idx:               int32(i),
					frameLen:          int32(end - pos),
				}
				if offset+1 > *nextOffset {
					*nextOffset = offset + 1
				}
			}
			seg.nextOffset = h.baseOffset + int64(h.recordCount)
			pos = end

		case errors.Is(err, io.ErrUnexpectedEOF):
			if isActive {
				if err := seg.truncate(pos); err != nil {
					return err
				}
			}
			if seg.nextOffset > *nextOffset {
				*nextOffset = seg.nextOffset
			}
			return nil

		case errors.Is(err, errBadMagic),
			errors.Is(err, errCorrupt),
			errors.Is(err, ErrCorruptRecord):
			pos = nextMagicInSegment(seg.file, pos+1, size)

		default:
			return err
		}
	}

	if seg.nextOffset > *nextOffset {
		*nextOffset = seg.nextOffset
	}
	return nil
}

// nextMagicInSegment scans forward in 4 KiB chunks; overlaps by 1
// byte so a magic spanning a chunk boundary isn't missed.
func nextMagicInSegment(f *os.File, start, size int64) int64 {
	const chunk = 4096
	buf := make([]byte, chunk)
	for pos := start; pos < size; {
		end := min(pos+chunk, size)
		readStart := pos
		if pos > start {
			readStart = pos - 1
		}
		n, err := f.ReadAt(buf[:end-readStart], readStart)
		if err != nil && err != io.EOF {
			return size
		}
		if n < 2 {
			return size
		}
		for i := 0; i+1 < n; i++ {
			if buf[i] == magicByte0 && buf[i+1] == magicByte1 {
				return readStart + int64(i)
			}
		}
		pos = end
	}
	return size
}
