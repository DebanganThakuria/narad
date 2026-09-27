package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// recover rebuilds the partition's segment list from its directory. Only
// the active (last) segment is read: it is walked frame by frame, CRC
// included, and indexed. A sealed segment is not read at all. Its range
// ends where its successor's begins, so its bounds come from the file
// names, and an open costs O(active segment) rather than O(retained
// bytes). Old sealed-segment indexes are loaded lazily by reads, which
// keeps retained history from becoming live heap.
//
// Taking a sealed segment's end from its successor is exact, not an
// estimate: a roll fsyncs the active segment and only then creates the
// next one, named by the offset the sealed one ended at, and nothing
// writes to a sealed segment again. So every offset below the
// successor's base that the segment ever held is in it, and none past.
//
// What recovery no longer does is prove each sealed frame intact at
// open. Nothing depended on that: the scan never repaired or reported a
// sealed segment (a bad frame was skipped in silence, and a torn tail
// left alone), and every read CRC-checks the frame it serves, so damage
// in a sealed segment surfaces when a record in it is read, as corrupt or
// not found, which the consume path skips as recorded loss. Only the
// errors that are not corruption move: an I/O error reading a sealed
// segment used to fail the open, and now fails the reads that need it.
//
// Scan rules for the active segment:
//
//   - Mid-file corruption: resync to the next frame that passes its CRC
//     and continue. Bad frames' offsets become permanent gaps. The file
//     is NOT truncated.
//
//   - Torn tail at EOF (a short last frame, or a last frame whose bytes
//     do not check out with nothing valid after it): truncate to the
//     last valid frame boundary, and fsync the truncate, so future
//     appends start clean.
//
// A sealed segment with a torn tail (its last sync lied and the roll
// after it survived) is left alone: nothing appends to it again, so the
// tear shadows nothing, and the bytes past it are lost either way. Its
// range still ends at its successor's base, so the lost offsets read as
// unreadable.
//
// An empty directory is initialised with one fresh segment at base
// offset 0.
func (l *Log) recover() (int64, error) {
	if err := os.MkdirAll(l.dir, dataDirMode); err != nil {
		return 0, fmt.Errorf("storage: ensure partition dir: %w", err)
	}

	names, err := listSegmentFileNames(l.dir)
	if err != nil {
		return 0, fmt.Errorf("storage: list segments: %w", err)
	}

	if len(names) == 0 {
		seg, err := createSegment(l.dir, 0, l.now())
		if err != nil {
			return 0, err
		}
		l.segments = []*segment{seg}
		return 0, nil
	}

	var nextOffset int64
	for i, name := range names {
		baseOffset, _ := parseSegmentFileName(name)
		seg, err := openSegment(filepath.Join(l.dir, name), baseOffset)
		if err != nil {
			return 0, fmt.Errorf("storage: open segment %s: %w", name, err)
		}
		if i < len(names)-1 {
			// Sealed: bounded by its successor, and nothing reads it
			// yet. The first read reopens it (segment.handle), so a
			// partition with days of history does not pin a descriptor
			// per file.
			seg.nextOffset, _ = parseSegmentFileName(names[i+1])
			_ = seg.release()
			l.segments = append(l.segments, seg)
			continue
		}
		if err := l.walkActiveSegment(seg, &nextOffset); err != nil {
			_ = seg.release()
			return 0, err
		}
		l.segments = append(l.segments, seg)
	}
	return nextOffset, nil
}

// walkActiveSegment CRC-walks the active segment to its durable tail,
// builds its sparse index, and truncates a torn tail (see recover). The
// log's next offset is the end of its last valid frame, or the segment's
// base when it holds none: every sealed segment ends at or below that
// base.
func (l *Log) walkActiveSegment(seg *segment, nextOffset *int64) error {
	pos := int64(0)
	size := seg.sizeBytes
	entries := make([]indexEntry, 0)

	for pos < size {
		// Recovery only needs frame headers + CRC to find the durable tail and
		// build the index; decoding every frame on startup is pure waste (and a
		// cold-start CPU spike on a large log). verifyFrameAt validates each
		// frame's CRC over the raw bytes, so corruption is still caught: an
		// intact CRC means the compressed payload is byte-good and would decode.
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
			if frameNext := h.baseOffset + int64(h.recordCount); frameNext > *nextOffset {
				*nextOffset = frameNext
			}
			seg.nextOffset = h.baseOffset + int64(h.recordCount)
			pos = end

		case errors.Is(err, io.ErrUnexpectedEOF),
			errors.Is(err, errBadMagic),
			errors.Is(err, errCorrupt),
			errors.Is(err, ErrCorruptRecord):
			// A short frame read is a torn tail only when the tear runs
			// to EOF. A corrupted length field mid-file lands here too;
			// if a later valid frame exists this is mid-file corruption,
			// so resync and keep scanning instead of truncating away
			// fsynced frames.
			//
			// Likewise a frame whose bytes are all there but do not
			// check out, with nothing valid after it, is a torn tail:
			// the crash landed after the file size moved and before the
			// data did (a zero-filled or scrambled last sector). Left in
			// place, its intact-looking header would shadow the frames
			// the next commits write at the same offsets (navigation is
			// header-only), so the first commit after recovery failed
			// its CRC read-back and, with the read-back disabled, its
			// records would read as corrupt for good. Cut it like any
			// other torn tail; mid-file corruption (a valid frame
			// follows) is still kept.
			if next := nextValidFramePos(seg.file, pos+1, size); next < size {
				pos = next
				continue
			}
			return l.finishTornTail(seg, pos, entries, nextOffset)

		default:
			return err
		}
	}

	if seg.nextOffset > *nextOffset {
		*nextOffset = seg.nextOffset
	}
	l.setSegmentIndexLocked(seg.baseOffset, entries)
	return nil
}

// finishTornTail ends the active segment's walk at a tear that runs to
// EOF: the segment is truncated at pos (and the truncate fsynced), and
// the recovered bounds and index are installed.
func (l *Log) finishTornTail(seg *segment, pos int64, entries []indexEntry, nextOffset *int64) error {
	if err := seg.truncate(pos); err != nil {
		return err
	}
	if seg.nextOffset > *nextOffset {
		*nextOffset = seg.nextOffset
	}
	l.setSegmentIndexLocked(seg.baseOffset, entries)
	return nil
}

func (l *Log) loadSegmentIndexLocked(seg *segment) error {
	entries, err := l.scanSegmentIndex(seg)
	if err != nil {
		return err
	}
	l.setSegmentIndexLocked(seg.baseOffset, entries)
	return nil
}

// scanSegmentIndex walks a segment's frame headers to rebuild its sparse
// index. It touches only the segment file and its recorded size, so for
// a SEALED segment (immutable) it may run without the Log lock; for the
// active segment the caller must hold the lock because the flusher
// advances sizeBytes under it.
func (l *Log) scanSegmentIndex(seg *segment) ([]indexEntry, error) {
	pos := int64(0)
	size := seg.sizeBytes
	entries := make([]indexEntry, 0)
	file, err := seg.handle()
	if err != nil {
		return nil, err
	}

	for pos < size {
		// Building the index of a sealed segment only needs frame headers to
		// find frame boundaries — no payload read, no CRC, no decode (see
		// frameHeaderAt). Sealed segments are complete, so a frame whose end
		// runs past the segment is a torn/corrupt tail: stop there.
		h, end, err := frameHeaderAt(file, pos)
		switch {
		case err == nil:
			if end > size {
				return entries, nil
			}
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
			pos = nextMagicInSegment(file, pos+1, size)

		default:
			return nil, err
		}
	}
	return entries, nil
}

// nextValidFramePos scans forward from start for the next position
// holding a frame that passes CRC verification, so callers can tell a
// mid-file tear (resyncable) from one that runs to EOF. Returns size
// when no later valid frame exists.
func nextValidFramePos(f *os.File, start, size int64) int64 {
	for pos := nextMagicInSegment(f, start, size); pos < size; pos = nextMagicInSegment(f, pos+1, size) {
		if _, _, err := verifyFrameAt(f, pos); err == nil {
			return pos
		}
	}
	return size
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
