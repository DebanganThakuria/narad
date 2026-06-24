package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// recover walks every segment file in the partition directory in
// base-offset order, recovering segment bounds and indexing only the
// active segment. Old sealed-segment indexes are loaded lazily by
// reads, which keeps retained history from becoming live heap.
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
		if m := l.opts.Metrics; m != nil {
			m.IncSegmentScanned()
		}
	}
	return nextOffset, nil
}

func (l *Log) walkSegment(seg *segment, isActive bool, nextOffset *int64) error {
	pos := int64(0)
	size := seg.sizeBytes
	entries := make([]indexEntry, 0)

	for pos < size {
		// Recovery only needs frame headers + CRC to find the durable tail and
		// build the index; decoding every frame on startup is pure waste (and a
		// cold-start CPU spike on a large log). verifyFrameAt validates each
		// frame's CRC over the raw bytes, so corruption is still caught — an
		// intact CRC means the compressed payload is byte-good and would decode.
		h, end, err := verifyFrameAt(seg.file, pos)

		switch {
		case err == nil:
			entry := indexEntry{
				segmentBaseOffset: seg.baseOffset,
				baseOffset:        h.baseOffset,
				recordCount:       h.recordCount,
				framePos:          pos,
				frameLen:          int32(end - pos),
			}
			if isActive {
				entries = l.appendSparseIndexEntry(entries, entry)
			}
			if frameNext := h.baseOffset + int64(h.recordCount); frameNext > *nextOffset {
				*nextOffset = frameNext
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
			if isActive {
				l.setSegmentIndexLocked(seg.baseOffset, entries)
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
	if isActive {
		l.setSegmentIndexLocked(seg.baseOffset, entries)
	}
	return nil
}

func (l *Log) loadSegmentIndexLocked(seg *segment) error {
	entries, err := l.scanSegmentIndexLocked(seg)
	if err != nil {
		return err
	}
	l.setSegmentIndexLocked(seg.baseOffset, entries)
	return nil
}

func (l *Log) scanSegmentIndexLocked(seg *segment) ([]indexEntry, error) {
	pos := int64(0)
	size := seg.sizeBytes
	entries := make([]indexEntry, 0)

	for pos < size {
		// Building the index only needs frame headers (no decode); see
		// verifyFrameAt. This keeps cold-start index loads off the zstd path.
		h, end, err := verifyFrameAt(seg.file, pos)
		switch {
		case err == nil:
			entries = l.appendSparseIndexEntry(entries, indexEntry{
				segmentBaseOffset: seg.baseOffset,
				baseOffset:        h.baseOffset,
				recordCount:       h.recordCount,
				framePos:          pos,
				frameLen:          int32(end - pos),
			})
			pos = end

		case errors.Is(err, io.ErrUnexpectedEOF):
			return entries, nil

		case errors.Is(err, errBadMagic),
			errors.Is(err, errCorrupt),
			errors.Is(err, ErrCorruptRecord):
			pos = nextMagicInSegment(seg.file, pos+1, size)

		default:
			return nil, err
		}
	}
	return entries, nil
}

// nextMagicInSegment scans forward in 4 KiB chunks; overlaps by 1
// byte so a magic spanning a chunk boundary isn't missed.
func nextMagicInSegment(f *os.File, start, size int64) int64 {
	const chunk = 4096
	// One extra byte so the 1-byte overlap read (readStart = pos-1) on
	// chunks after the first still fits: end-readStart can be chunk+1.
	buf := make([]byte, chunk+1)
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
