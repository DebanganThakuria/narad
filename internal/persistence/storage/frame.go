package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func crc32cOf(parts ...[]byte) uint32 {
	c := crc32.New(crc32cTable)
	for _, p := range parts {
		c.Write(p)
	}
	return c.Sum32()
}

func encodeRecordsPayload(dst []byte, records [][]byte) []byte {
	for _, r := range records {
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(r)))
		dst = append(dst, lb[:]...)
		dst = append(dst, r...)
	}
	return dst
}

// decodeRecordsPayload returns slices that reference the input. Caller
// must copy if it needs to retain them past the codec buffer's
// lifetime.
func decodeRecordsPayload(payload []byte, recordCount int32) ([][]byte, error) {
	out := make([][]byte, 0, recordCount)
	pos := 0
	for i := range recordCount {
		if pos+4 > len(payload) {
			return nil, fmt.Errorf("%w: record %d header truncated", ErrCorruptRecord, i)
		}
		l := int(binary.BigEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if l < 0 || pos+l > len(payload) {
			return nil, fmt.Errorf("%w: record %d length %d overruns payload", ErrCorruptRecord, i, l)
		}
		out = append(out, payload[pos:pos+l])
		pos += l
	}
	if pos != len(payload) {
		return nil, fmt.Errorf("%w: %d trailing bytes after %d records", ErrCorruptRecord, len(payload)-pos, recordCount)
	}
	return out, nil
}

func encodeFrame(records [][]byte, baseOffset int64, c codec.Codec) ([]byte, error) {
	if len(records) == 0 {
		return nil, errors.New("storage: encodeFrame: empty batch")
	}

	innerSize := 0
	for _, r := range records {
		innerSize += 4 + len(r)
	}
	inner := make([]byte, 0, innerSize)
	inner = encodeRecordsPayload(inner, records)
	encoded := c.Encode(nil, inner)

	if len(inner) > (1<<31)-1 || len(encoded) > (1<<31)-1 {
		return nil, fmt.Errorf("storage: frame too large: uncompressed=%d compressed=%d", len(inner), len(encoded))
	}

	frame := make([]byte, headerSize+len(encoded))
	encodeHeader(frame[:headerSize], frameHeader{
		flags:        c.Flag() & codecMask,
		recordCount:  int32(len(records)),
		baseOffset:   baseOffset,
		uncompressed: int32(len(inner)),
		compressed:   int32(len(encoded)),
	})
	copy(frame[headerSize:], encoded)

	crc := crc32cOf(frame[2:23], frame[headerSize:])
	binary.BigEndian.PutUint32(frame[23:27], crc)
	return frame, nil
}

// readFrameAt errors:
//   - errBadMagic: header magic mismatch (caller resyncs)
//   - errCorrupt:  CRC mismatch or inner record stream invalid
//   - io.ErrUnexpectedEOF: torn tail
func readFrameAt(r io.ReaderAt, pos int64, log *Log) (frameHeader, [][]byte, int64, error) {
	var hdrBuf [headerSize]byte
	n, err := r.ReadAt(hdrBuf[:], pos)
	if err != nil && err != io.EOF {
		return frameHeader{}, nil, pos, err
	}
	if n < headerSize {
		return frameHeader{}, nil, pos, io.ErrUnexpectedEOF
	}
	h, err := decodeHeader(hdrBuf[:])
	if err != nil {
		return h, nil, pos, err
	}

	payload := make([]byte, h.compressed)
	n, err = r.ReadAt(payload, pos+headerSize)
	if err != nil && err != io.EOF {
		return h, nil, pos, err
	}
	if n < int(h.compressed) {
		return h, nil, pos, io.ErrUnexpectedEOF
	}

	if want, got := h.crc, crc32cOf(hdrBuf[2:23], payload); want != got {
		return h, nil, pos, fmt.Errorf("%w: crc want=0x%x got=0x%x at pos=%d", errCorrupt, want, got, pos)
	}

	c, err := codecForFlag(h.codec(), log.codec)
	if err != nil {
		return h, nil, pos, err
	}
	decoded, err := c.Decode(nil, payload, int(h.uncompressed))
	if err != nil {
		return h, nil, pos, fmt.Errorf("%w: decode: %v", errCorrupt, err)
	}
	if len(decoded) != int(h.uncompressed) {
		return h, nil, pos, fmt.Errorf("%w: decoded size %d != header.uncompressed %d", errCorrupt, len(decoded), h.uncompressed)
	}

	records, err := decodeRecordsPayload(decoded, h.recordCount)
	if err != nil {
		return h, nil, pos, fmt.Errorf("%w: split: %v", errCorrupt, err)
	}

	return h, records, pos + int64(headerSize) + int64(h.compressed), nil
}

// verifyFrameAt re-reads the frame at pos and validates its CRC over the raw
// (possibly compressed) on-disk bytes, WITHOUT decoding. It returns the frame
// header (record count, base offset) and the position just after the frame.
//
// Two hot, decode-free paths use it:
//   - the durability read-back (VerifyDurable) — one CRC check per frame
//     instead of a full decode per record;
//   - index navigation (scanSegmentFromIndexAnchorLocked / the index build) —
//     locating an offset only needs frame headers to step frame-to-frame, so
//     decoding the payload there is pure waste. With small frames and a sparse
//     index, that waste dominated consume CPU (hundreds of decodes per lookup).
//
// CRC is still checked so navigation detects corruption exactly as the old
// decode-based walk did; only the (expensive) zstd decode is skipped.
func verifyFrameAt(r io.ReaderAt, pos int64) (frameHeader, int64, error) {
	var hdrBuf [headerSize]byte
	n, err := r.ReadAt(hdrBuf[:], pos)
	if err != nil && err != io.EOF {
		return frameHeader{}, pos, err
	}
	if n < headerSize {
		return frameHeader{}, pos, io.ErrUnexpectedEOF
	}
	h, err := decodeHeader(hdrBuf[:])
	if err != nil {
		return h, pos, err
	}

	payload := make([]byte, h.compressed)
	n, err = r.ReadAt(payload, pos+headerSize)
	if err != nil && err != io.EOF {
		return h, pos, err
	}
	if n < int(h.compressed) {
		return h, pos, io.ErrUnexpectedEOF
	}

	if want, got := h.crc, crc32cOf(hdrBuf[2:23], payload); want != got {
		return h, pos, fmt.Errorf("%w: crc want=0x%x got=0x%x at pos=%d", errCorrupt, want, got, pos)
	}
	return h, pos + int64(headerSize) + int64(h.compressed), nil
}
