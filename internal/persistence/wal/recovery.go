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
	for {
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, 0, false, fmt.Errorf("wal: segment offset: %w", err)
		}
		record, ok, err := readFrame(file, segment.base, offset, maxRecord)
		if err != nil {
			// A corrupt frame in the last (active) segment is only a
			// torn tail if the corruption runs all the way to EOF. If a
			// later valid frame exists, this is mid-file corruption of
			// already-fsynced (acked) data: truncating would silently
			// destroy those acked records and regress nextSeq, and
			// resyncing past the gap would break the dense-seq invariant
			// the dispatcher checkpoint relies on. That case stays a
			// loud Open failure, like corruption in earlier segments.
			if tolerateCorruptTail && errors.Is(err, errCorruptFrame) {
				laterValid, scanErr := hasLaterValidFrame(file, offset+1, maxRecord)
				if scanErr != nil {
					return 0, 0, false, scanErr
				}
				if !laterValid {
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
		validEnd = offset + frameHeaderSize + int64(len(record.Payload))
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
	if pos+frameHeaderSize > size {
		return false
	}
	var header [frameHeaderSize]byte
	if _, err := f.ReadAt(header[:], pos); err != nil {
		return false
	}
	if binary.BigEndian.Uint32(header[0:4]) != frameMagic {
		return false
	}
	n := binary.BigEndian.Uint32(header[4:8])
	if n == 0 || int(n) > maxRecord {
		return false
	}
	if pos+frameHeaderSize+int64(n) > size {
		return false
	}
	payload := make([]byte, int(n))
	if _, err := f.ReadAt(payload, pos+frameHeaderSize); err != nil {
		return false
	}
	return crc32.ChecksumIEEE(payload) == binary.BigEndian.Uint32(header[16:20])
}
