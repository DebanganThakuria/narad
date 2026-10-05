package storage

// What a partition move may copy. A segment file can hold more than the
// committed records: the frames a commit wrote and has not made visible
// yet, and the hidden tail a failed commit or a crash leaves behind the
// high watermark. A failed commit truncates such frames and hands their
// offsets to other records, so a copy that took them could hold records
// the source never committed, at offsets the source later committed
// others at. A copy therefore lists and reads each segment only up to
// its committed boundary: the first frame at or above the high
// watermark. Every frame below it was fsynced before the high watermark
// advanced over it, and the discard never cuts below it.
//
// The destination side cuts its staged copy back to the same boundary
// (CutStagedCopy), for a source on an older release that lists file
// sizes.

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CommittedBoundary reports the byte position in this log's segment with
// base offset base below which every frame holds only records below hwm:
// the position of the first frame at or above hwm (a frame that
// straddles hwm stays whole), or the segment's size when every frame is
// below it. hwm is lowered to the log's high watermark first, so the
// position never covers a record that is not committed. ok is false for
// a closed log and for a segment the log no longer has.
func (l *Log) CommittedBoundary(base, hwm int64) (pos int64, ok bool) {
	l.rwmu.RLock()
	defer l.rwmu.RUnlock()
	if l.closed.Load() {
		return 0, false
	}
	seg := l.findSegmentLocked(base)
	if seg == nil {
		return 0, false
	}
	_, pos = l.frameBoundaryAtOrAboveLocked(seg, min(hwm, l.highWatermark.Load()))
	return pos, true
}

// committedScanBuffer sizes the buffered reader of the closed-segment
// walk: large enough that a segment of small frames is read in few
// system calls.
const committedScanBuffer = 64 << 10

// CommittedSegmentBytes is CommittedBoundary for a segment whose log is
// not open here: it walks the frame headers of the segment file with
// base offset base in partitionDir from the start and returns the
// position of the first frame at or above hwm, or the file's size when
// every frame is below it. A torn frame at the end of the file ends the
// walk before it. A frame header that does not decode below hwm is
// damage that recovery steps over (it resyncs to the next good frame),
// so the walk cannot place the boundary and reports the file's size, as
// a listing did before this boundary existed.
func CommittedSegmentBytes(partitionDir string, base, hwm int64) (int64, error) {
	pos, size, stop, err := walkFramesBelow(partitionDir, base, hwm)
	if err != nil {
		return 0, err
	}
	if stop == walkDamaged {
		return size, nil
	}
	return pos, nil
}

// walkStop says why walkFramesBelow stopped.
type walkStop int

const (
	// walkReached: at the first frame at or above the offset, or at the
	// end of the file with every frame below it.
	walkReached walkStop = iota
	// walkTorn: a frame that runs past the end of the file.
	walkTorn
	// walkDamaged: a frame header that does not decode.
	walkDamaged
)

// walkFramesBelow walks the frame headers of a segment file from the
// start up to the first frame whose base offset is at or above offset,
// and reports the position it stopped at, the file's size and why it
// stopped.
func walkFramesBelow(partitionDir string, base, offset int64) (pos, size int64, stop walkStop, err error) {
	f, err := os.Open(filepath.Join(partitionDir, segmentFileName(base)))
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, 0, 0, err
	}
	size = st.Size()
	if offset <= base {
		return 0, size, walkReached, nil
	}
	br := bufio.NewReaderSize(f, committedScanBuffer)
	var hdr [headerSize]byte
	for pos < size {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return pos, size, walkTorn, nil
			}
			return 0, 0, 0, err
		}
		h, err := decodeHeader(hdr[:])
		if err != nil {
			return pos, size, walkDamaged, nil
		}
		if h.baseOffset >= offset {
			return pos, size, walkReached, nil
		}
		end := pos + int64(headerSize) + int64(h.compressed)
		if end > size {
			return pos, size, walkTorn, nil
		}
		if _, err := br.Discard(int(h.compressed)); err != nil {
			if errors.Is(err, io.EOF) {
				return pos, size, walkTorn, nil
			}
			return 0, 0, 0, err
		}
		pos = end
	}
	return pos, size, walkReached, nil
}

// ListCommittedSegments lists partitionDir's segments for a copy of the
// partition at high watermark hwm: ListPartitionSegments without the
// segments that start above hwm (they hold no record below it), and the
// last one left cut to its committed boundary. boundary reports that
// boundary from the open log (Log.CommittedBoundary) and may be nil;
// when it does not know the segment, the file's frames are walked
// (CommittedSegmentBytes). Segments before the last one end below their
// successor's base, which is at most hwm, so they hold only committed
// records. The last listed segment is reported unsealed: it is the one
// a copy keeps tailing.
func ListCommittedSegments(partitionDir string, hwm int64, boundary func(base int64) (int64, bool)) ([]SegmentInfo, error) {
	segs, err := ListPartitionSegments(partitionDir)
	if err != nil || len(segs) == 0 {
		return segs, err
	}
	keep := len(segs)
	for keep > 0 && segs[keep-1].BaseOffset > hwm {
		keep--
	}
	segs = segs[:keep]
	if len(segs) == 0 {
		return segs, nil
	}
	last := &segs[len(segs)-1]
	last.Sealed = false
	pos, ok := int64(0), false
	if boundary != nil {
		pos, ok = boundary(last.BaseOffset)
	}
	if !ok {
		pos, err = CommittedSegmentBytes(partitionDir, last.BaseOffset, hwm)
		if errors.Is(err, os.ErrNotExist) {
			// Reaped by retention since the listing: the fetch fails the
			// same way and the copy lists again.
			return segs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("storage: committed boundary of segment %d: %w", last.BaseOffset, err)
		}
	}
	last.SizeBytes = min(last.SizeBytes, max(pos, 0))
	return segs, nil
}

// CutStagedCopy cuts a staged partition copy in partitionDir back to the
// records below hwm: segment files that start above hwm are removed, and
// the last one left is truncated before its first frame at or above hwm.
// It reports whether it changed anything. A segment whose frames cannot
// be walked to hwm (a torn or damaged frame before it) is left as it
// is: the verify that follows decides about it.
func CutStagedCopy(partitionDir string, hwm int64) (bool, error) {
	segs, err := ListPartitionSegments(partitionDir)
	if err != nil {
		return false, err
	}
	cut := false
	for len(segs) > 0 && segs[len(segs)-1].BaseOffset > hwm {
		path := filepath.Join(partitionDir, segmentFileName(segs[len(segs)-1].BaseOffset))
		if err := os.Remove(path); err != nil {
			return cut, fmt.Errorf("storage: remove staged segment past the high watermark: %w", err)
		}
		cut = true
		segs = segs[:len(segs)-1]
	}
	if len(segs) == 0 {
		return cut, nil
	}
	last := segs[len(segs)-1]
	pos, size, stop, err := walkFramesBelow(partitionDir, last.BaseOffset, hwm)
	if err != nil {
		return cut, err
	}
	if stop != walkReached || pos >= size {
		return cut, nil
	}
	if err := TruncateSegmentFile(partitionDir, last.BaseOffset, pos); err != nil {
		return cut, err
	}
	return true, nil
}

// TruncateSegmentFile cuts the segment file with base offset base in
// partitionDir back to size bytes.
func TruncateSegmentFile(partitionDir string, base, size int64) error {
	return os.Truncate(filepath.Join(partitionDir, segmentFileName(base)), size)
}

// SegmentFileSize reports the size of the segment file with base offset
// base in partitionDir; ok is false when it does not exist.
func SegmentFileSize(partitionDir string, base int64) (size int64, ok bool, err error) {
	st, err := os.Stat(filepath.Join(partitionDir, segmentFileName(base)))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return st.Size(), true, nil
}
