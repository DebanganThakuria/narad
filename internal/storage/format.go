package storage

import (
	"encoding/binary"
	"fmt"
)

// On-disk frame format. CRC excludes the magic so the recovery scanner
// can scan for the magic cheaply.
//
//	offset 0   [magic:        2 bytes]   0xCA 0xFE
//	offset 2   [flags:        1 byte]    bits 0-2 codec; bits 3-7 reserved
//	offset 3   [recordCount:  4 bytes]   big-endian int32
//	offset 7   [baseOffset:   8 bytes]   big-endian int64
//	offset 15  [uncompressed: 4 bytes]   big-endian int32
//	offset 19  [compressed:   4 bytes]   big-endian int32
//	offset 23  [crc32c:       4 bytes]   CRC over [2..23) + payload
//	offset 27  [payload]                 records: [length:4 BE][bytes] xN
const (
	magicByte0 byte = 0xCA
	magicByte1 byte = 0xFE

	headerSize = 27

	codecNone uint8 = 0
	codecZstd uint8 = 1

	codecMask uint8 = 0b0000_0111

	// Bound to prevent a corrupt header from making us allocate
	// gigabytes of buffer during recovery.
	maxFrameBytes = 256 * 1024 * 1024
)

type frameHeader struct {
	flags        uint8
	recordCount  int32
	baseOffset   int64
	uncompressed int32
	compressed   int32
	crc          uint32
}

func (h frameHeader) codec() uint8 { return h.flags & codecMask }

func encodeHeader(buf []byte, h frameHeader) {
	if len(buf) < headerSize {
		panic(fmt.Sprintf("storage: encodeHeader buf too small: %d", len(buf)))
	}
	buf[0] = magicByte0
	buf[1] = magicByte1
	buf[2] = h.flags
	binary.BigEndian.PutUint32(buf[3:7], uint32(h.recordCount))
	binary.BigEndian.PutUint64(buf[7:15], uint64(h.baseOffset))
	binary.BigEndian.PutUint32(buf[15:19], uint32(h.uncompressed))
	binary.BigEndian.PutUint32(buf[19:23], uint32(h.compressed))
	binary.BigEndian.PutUint32(buf[23:27], h.crc)
}

// decodeHeader returns errBadMagic on a 2-byte mismatch so callers can
// resync; out-of-range values are returned as ErrCorruptRecord.
func decodeHeader(buf []byte) (frameHeader, error) {
	if len(buf) < headerSize {
		return frameHeader{}, fmt.Errorf("%w: header buf %d < %d", ErrCorruptRecord, len(buf), headerSize)
	}
	if buf[0] != magicByte0 || buf[1] != magicByte1 {
		return frameHeader{}, errBadMagic
	}
	h := frameHeader{
		flags:        buf[2],
		recordCount:  int32(binary.BigEndian.Uint32(buf[3:7])),
		baseOffset:   int64(binary.BigEndian.Uint64(buf[7:15])),
		uncompressed: int32(binary.BigEndian.Uint32(buf[15:19])),
		compressed:   int32(binary.BigEndian.Uint32(buf[19:23])),
		crc:          binary.BigEndian.Uint32(buf[23:27]),
	}
	if h.recordCount <= 0 || h.uncompressed <= 0 || h.compressed <= 0 || h.baseOffset < 0 {
		return h, fmt.Errorf("%w: header out of range", ErrCorruptRecord)
	}
	if h.compressed > maxFrameBytes || h.uncompressed > maxFrameBytes {
		return h, fmt.Errorf("%w: frame too large: %d / %d", ErrCorruptRecord, h.compressed, h.uncompressed)
	}
	return h, nil
}
