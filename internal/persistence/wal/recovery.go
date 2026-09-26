package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// scanForOpen walks every segment to recover Open's starting state: the
// next sequence number to assign and the end of valid data in the last
// (active) segment, past which Open truncates a torn tail.
func scanForOpen(segments []segmentInfo, maxRecord int) (nextSeq uint64, lastValidEnd int64, err error) {
	for i, segment := range segments {
		validEnd, maxSeq, sawRecord, err := scanSegment(segment, maxRecord, i == len(segments)-1)
		if err != nil {
			return 0, 0, err
		}
		if sawRecord && maxSeq >= nextSeq {
			nextSeq = maxSeq + 1
		}
		if i == len(segments)-1 {
			lastValidEnd = validEnd
		}
	}
	return nextSeq, lastValidEnd, nil
}

// scanSegment reads frames until EOF or corruption, returning the end
// offset of the last valid frame, the highest seq seen, and whether any
// record was read at all.
func scanSegment(segment segmentInfo, maxRecord int, tolerateCorruptTail bool) (int64, uint64, bool, error) {
	file, err := os.Open(segment.path)
	if err != nil {
		return 0, 0, false, fmt.Errorf("wal: open segment: %w", err)
	}
	defer file.Close()

	var validEnd int64
	var maxSeq uint64
	var sawRecord bool
	// Buffered sequential read with an arithmetic position; the corrupt
	// tail probe below uses ReadAt on the raw file, independent of the
	// buffered reader's position.
	reader := getFrameReader(file)
	defer putFrameReader(reader)
	var offset int64
	for {
		record, ok, err := reader.readFrame(segment.base, offset, maxRecord)
		if err != nil {
			// A corrupt frame in the last (active) segment is only a
			// torn tail if the corruption runs all the way to EOF. If a
			// later valid frame exists, this is mid-file corruption of
			// already-fsynced (acked) data: truncating would silently
			// destroy those acked records and regress nextSeq, and
			// resyncing past the gap would break the dense-seq invariant
			// the dispatcher checkpoint relies on. That case stays a
			// loud Open failure, like corruption in earlier segments,
			// with one exception: in a prepared segment a crash can tear
			// the last, unsynced write into a hole followed by valid
			// frames of the same write (tornPreparedWrite).
			if tolerateCorruptTail && errors.Is(err, errCorruptFrame) {
				laterValid, scanErr := hasLaterValidFrame(file, offset+1, maxRecord)
				if scanErr != nil {
					return 0, 0, false, scanErr
				}
				torn := !laterValid
				if laterValid {
					// The frame at offset carries the seq after the
					// last valid one, or the segment's base.
					firstSeq := segment.base
					if sawRecord {
						firstSeq = maxSeq + 1
					}
					if torn, scanErr = tornPreparedWrite(file, offset, firstSeq, maxRecord); scanErr != nil {
						return 0, 0, false, scanErr
					}
				}
				if torn {
					// Genuine torn tail: stop here so Open truncates
					// to the last valid frame end.
					return validEnd, maxSeq, sawRecord, nil
				}
			}
			return 0, 0, false, err
		}
		if !ok {
			return validEnd, maxSeq, sawRecord, nil
		}

		sawRecord = true
		offset += frameHeaderSize + int64(len(record.Payload))
		validEnd = offset
		if record.ID.Seq > maxSeq {
			maxSeq = record.ID.Seq
		}
	}
}

// hasLaterValidFrame reports whether a complete frame passing magic,
// length, and CRC checks starts at or after start. Open-time recovery
// uses it to tell a torn tail (no later valid frame: corruption runs
// to EOF, safe to truncate) from mid-file corruption of fsynced data
// (later valid frames exist and truncation would destroy them).
func hasLaterValidFrame(file *os.File, start int64, maxRecord int) (bool, error) {
	info, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("wal: stat segment: %w", err)
	}
	size := info.Size()
	for pos := nextMagicPos(file, start, size); pos < size; pos = nextMagicPos(file, pos+1, size) {
		if frameValidAt(file, pos, size, maxRecord) {
			return true, nil
		}
	}
	return false, nil
}

// nextMagicPos scans forward in 4 KiB chunks for the 4-byte frame
// magic; chunks after the first overlap the previous one by 3 bytes so
// a magic spanning a chunk boundary is not missed. Returns size when
// no magic is found (treating read errors as "no magic" keeps recovery
// on the conservative truncate-the-tail path).
func nextMagicPos(f *os.File, start, size int64) int64 {
	const chunk = 4096
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], frameMagic)
	// Three extra bytes so the overlap read (readStart = pos-3) on
	// chunks after the first still fits: end-readStart can be chunk+3.
	buf := make([]byte, chunk+3)
	for pos := start; pos < size; {
		end := min(pos+chunk, size)
		readStart := pos
		if pos > start {
			readStart = pos - 3
		}
		n, err := f.ReadAt(buf[:end-readStart], readStart)
		if err != nil && err != io.EOF {
			return size
		}
		if idx := bytes.Index(buf[:n], magic[:]); idx >= 0 {
			return readStart + int64(idx)
		}
		pos = end
	}
	return size
}

// frameValidAt reports whether a complete frame passing magic, length,
// and CRC verification starts at pos.
func frameValidAt(f *os.File, pos, size int64, maxRecord int) bool {
	_, ok := frameAt(f, pos, size, maxRecord)
	return ok
}

// frameAt returns the header of the frame starting at pos if it is
// complete below size and passes magic, length, and CRC verification.
func frameAt(f *os.File, pos, size int64, maxRecord int) (frameHeader, bool) {
	if pos+frameHeaderSize > size {
		return frameHeader{}, false
	}
	var header [frameHeaderSize]byte
	if _, err := f.ReadAt(header[:], pos); err != nil {
		return frameHeader{}, false
	}
	if binary.BigEndian.Uint32(header[0:4]) != frameMagic {
		return frameHeader{}, false
	}
	n := binary.BigEndian.Uint32(header[4:8])
	if n == 0 || int(n) > maxRecord {
		return frameHeader{}, false
	}
	if pos+frameHeaderSize+int64(n) > size {
		return frameHeader{}, false
	}
	payload := make([]byte, int(n))
	if _, err := f.ReadAt(payload, pos+frameHeaderSize); err != nil {
		return frameHeader{}, false
	}
	h := frameHeader{
		seq:  binary.BigEndian.Uint64(header[8:16]),
		size: int(n),
		crc:  binary.BigEndian.Uint32(header[16:20]),
	}
	return h, crc32.ChecksumIEEE(payload) == h.crc
}

// tornPreparedWrite reports whether the damage at offset bad of the last
// segment, with valid frames after it, is what a crash leaves of a write
// into a prepared segment that was not yet synced. There the device may
// persist any subset of the write's pages, so a hole of zeros can sit in
// front of valid frames of the same write (see prealloc.go). Truncating
// at bad then drops only frames whose write never completed its sync, so
// none of them was acked. That is the case only if:
//
//   - the segment ends in the prepTrailer, so it was zero-filled up to
//     the trailer and no write into it exceeded the limit the trailer
//     records. The write that tore started at or before bad, at the end
//     of the synced data, so it ended within limit bytes of bad;
//   - every byte from there to the trailer is still zero;
//   - every valid frame after bad ends within that window and carries a
//     seq that continues the run: after firstSeq (the seq of the frame
//     at bad), increasing, and no higher than the frames that fit
//     between bad and it allow.
//
// Anything else is corruption of synced data and stays a loud failure.
// Within the window the two cannot be told apart: synced frames there
// damaged after the fact are read as a torn write too, just as damage
// to the last frame of any active segment always has been.
func tornPreparedWrite(file *os.File, bad int64, firstSeq uint64, maxRecord int) (bool, error) {
	info, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("wal: stat segment: %w", err)
	}
	zerosEnd := info.Size() - prepTrailerSize
	if zerosEnd <= bad {
		return false, nil
	}
	var trailer [prepTrailerSize]byte
	if _, err := file.ReadAt(trailer[:], zerosEnd); err != nil {
		return false, fmt.Errorf("wal: read segment trailer: %w", err)
	}
	if [8]byte(trailer[:8]) != prepTrailerMagic {
		return false, nil
	}
	limit := binary.BigEndian.Uint64(trailer[8:])
	if limit == 0 {
		return false, nil
	}
	windowEnd := zerosEnd
	if limit < uint64(zerosEnd-bad) {
		windowEnd = bad + int64(limit)
	}
	zero, err := allZero(file, windowEnd, zerosEnd)
	if err != nil || !zero {
		return false, err
	}
	// Frame magic has no zero byte, so with the zeros checked every
	// magic that could start a frame lies below windowEnd.
	prevSeq := firstSeq
	for pos := nextMagicPos(file, bad+1, windowEnd); pos < windowEnd; {
		h, ok := frameAt(file, pos, zerosEnd, maxRecord)
		if !ok {
			pos = nextMagicPos(file, pos+1, windowEnd)
			continue
		}
		end := pos + frameHeaderSize + int64(h.size)
		// Each frame from bad on is at least frameHeaderSize+1 bytes, so
		// at most (pos-bad)/(frameHeaderSize+1) of them precede this one.
		if end > windowEnd || h.seq <= prevSeq || h.seq-firstSeq > uint64(pos-bad)/(frameHeaderSize+1) {
			return false, nil
		}
		prevSeq = h.seq
		pos = nextMagicPos(file, end, windowEnd)
	}
	return true, nil
}

// allZero reports whether every byte of f in [start, end) is zero.
func allZero(f *os.File, start, end int64) (bool, error) {
	if start >= end {
		return true, nil
	}
	buf := make([]byte, min(end-start, replayReadBufferSize))
	for pos := start; pos < end; {
		n := min(end-pos, int64(len(buf)))
		if _, err := f.ReadAt(buf[:n], pos); err != nil {
			return false, fmt.Errorf("wal: read segment: %w", err)
		}
		if !bytes.Equal(buf[:n], prepZeros[:n]) {
			return false, nil
		}
		pos += n
	}
	return true, nil
}
