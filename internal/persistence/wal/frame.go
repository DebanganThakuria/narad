package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	frameMagic      uint32 = 0x4e57414c // NWAL
	frameHeaderSize        = 20
)

func appendFrame(dst []byte, seq uint64, payload []byte) []byte {
	start := len(dst)
	size := frameHeaderSize + len(payload)
	end := start + size
	if cap(dst) < end {
		nextCap := end
		if doubled := cap(dst) * 2; doubled > nextCap {
			nextCap = doubled
		}
		next := make([]byte, end, nextCap)
		copy(next, dst)
		dst = next
	} else {
		dst = dst[:end]
	}
	frame := dst[start:end]
	binary.BigEndian.PutUint32(frame[0:4], frameMagic)
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint64(frame[8:16], seq)
	binary.BigEndian.PutUint32(frame[16:20], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderSize:], payload)
	return dst
}

func readFrame(r io.Reader, segmentBase uint64, offset int64, maxRecord int) (Record, bool, error) {
	var header [frameHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("wal: read frame header: %w", err)
	}
	if got := binary.BigEndian.Uint32(header[0:4]); got != frameMagic {
		return Record{}, false, fmt.Errorf("wal: bad frame magic at offset %d", offset)
	}
	n := binary.BigEndian.Uint32(header[4:8])
	if n == 0 {
		return Record{}, false, fmt.Errorf("wal: empty frame at offset %d", offset)
	}
	if int(n) > maxRecord {
		return Record{}, false, fmt.Errorf("wal: frame size %d exceeds max %d", n, maxRecord)
	}
	seq := binary.BigEndian.Uint64(header[8:16])
	wantCRC := binary.BigEndian.Uint32(header[16:20])
	payload := make([]byte, int(n))
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("wal: read frame payload: %w", err)
	}
	if got := crc32.ChecksumIEEE(payload); got != wantCRC {
		return Record{}, false, fmt.Errorf("wal: checksum mismatch at offset %d", offset)
	}
	return Record{
		ID:      RecordID{SegmentBase: segmentBase, Offset: offset, Seq: seq},
		Payload: payload,
	}, true, nil
}
