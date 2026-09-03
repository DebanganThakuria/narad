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

// encodeFrame builds one frame with fresh buffers. The flusher uses a
// frameEncoder to reuse its buffers across flushes; this form serves
// tests and one-off callers.
func encodeFrame(records [][]byte, baseOffset int64, c codec.Codec) ([]byte, error) {
	var enc frameEncoder
	frame, err := enc.encodeFrame(records, baseOffset, c)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(frame))
	copy(out, frame)
	return out, nil
}

// maxRetainedFrameBuffer bounds the encode buffers a frameEncoder keeps
// between flushes, so one oversized batch does not pin memory forever.
const maxRetainedFrameBuffer = 4 << 20

// frameEncoder owns reusable buffers for building frames. The returned
// frame aliases enc.frame and is valid only until the next encodeFrame
// call; callers must finish writing it before encoding again.
type frameEncoder struct {
	inner []byte
	frame []byte
}

// encodeFrame encodes records into a frame: header, then the codec's
// output. The codec appends straight into the frame buffer after the
// header slot (both codecs append to dst), so there is no intermediate
// "encoded" buffer and no copy.
func (enc *frameEncoder) encodeFrame(records [][]byte, baseOffset int64, c codec.Codec) ([]byte, error) {
	if len(records) == 0 {
		return nil, errors.New("storage: encodeFrame: empty batch")
	}

	innerSize := 0
	for _, r := range records {
		innerSize += 4 + len(r)
	}
	if cap(enc.inner) < innerSize {
		enc.inner = make([]byte, 0, innerSize)
	}
	inner := encodeRecordsPayload(enc.inner[:0], records)

	if cap(enc.frame) < headerSize {
		enc.frame = make([]byte, 0, headerSize+innerSize)
	}
	frame := c.Encode(enc.frame[:headerSize], inner)
	encodedLen := len(frame) - headerSize

	// Mirror decodeHeader's read-time bound: a frame past maxFrameBytes
	// would be written but rejected as corrupt on every read (a poison
	// frame), so refuse it at write time instead.
	if len(inner) > maxFrameBytes || encodedLen > maxFrameBytes {
		enc.release()
		return nil, fmt.Errorf("storage: frame too large: uncompressed=%d compressed=%d", len(inner), encodedLen)
	}

	encodeHeader(frame[:headerSize], frameHeader{
		flags:        c.Flag() & codecMask,
		recordCount:  int32(len(records)),
		baseOffset:   baseOffset,
		uncompressed: int32(len(inner)),
		compressed:   int32(encodedLen),
	})

	crc := crc32cOf(frame[2:23], frame[headerSize:])
	binary.BigEndian.PutUint32(frame[23:27], crc)

	// Keep the (possibly grown) buffers for the next flush, within bounds.
	if cap(inner) <= maxRetainedFrameBuffer {
		enc.inner = inner[:0]
	} else {
		enc.inner = nil
	}
	if cap(frame) <= maxRetainedFrameBuffer {
		enc.frame = frame[:0]
	} else {
		enc.frame = nil
	}
	return frame, nil
}

func (enc *frameEncoder) release() {
	enc.inner = nil
	enc.frame = nil
}

// readFrameRaw reads the header and raw (still-encoded) payload of the
// frame at pos and validates the CRC — no decode. Shared by readFrameAt
// (which goes on to decode) and verifyFrameAt (which deliberately does
// not).
//
// Errors:
//   - errBadMagic: header magic mismatch (caller resyncs)
//   - errCorrupt:  CRC mismatch
//   - io.ErrUnexpectedEOF: torn tail
func readFrameRaw(r io.ReaderAt, pos int64) (frameHeader, []byte, error) {
	var hdrBuf [headerSize]byte
	n, err := r.ReadAt(hdrBuf[:], pos)
	if err != nil && err != io.EOF {
		return frameHeader{}, nil, err
	}
	if n < headerSize {
		return frameHeader{}, nil, io.ErrUnexpectedEOF
	}
	h, err := decodeHeader(hdrBuf[:])
	if err != nil {
		return h, nil, err
	}

	payload := make([]byte, h.compressed)
	n, err = r.ReadAt(payload, pos+headerSize)
	if err != nil && err != io.EOF {
		return h, nil, err
	}
	if n < int(h.compressed) {
		return h, nil, io.ErrUnexpectedEOF
	}

	if want, got := h.crc, crc32cOf(hdrBuf[2:23], payload); want != got {
		return h, nil, fmt.Errorf("%w: crc want=0x%x got=0x%x at pos=%d", errCorrupt, want, got, pos)
	}
	return h, payload, nil
}

// readFrameAt reads, CRC-checks, and decodes the frame at pos, returning
// its records and the position just after the frame. Errors are those of
// readFrameRaw, plus errCorrupt when the decoded record stream is invalid.
func readFrameAt(r io.ReaderAt, pos int64, log *Log) (frameHeader, [][]byte, int64, error) {
	h, payload, err := readFrameRaw(r, pos)
	if err != nil {
		return h, nil, pos, err
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

// frameHeaderReadHook, when non-nil, is invoked on every frame-header read.
// It is nil in production (a single predicted-not-taken branch) and set only by
// tests to count header reads — used to prove navigation no longer re-walks
// frames from the sparse anchor on each sequential read.
var frameHeaderReadHook func()

// frameHeaderAt reads ONLY the frame header at pos — no payload read, no CRC,
// no decode — and returns it plus the position just after the frame. It is for
// pure navigation: locating an offset by stepping frame-to-frame needs only
// baseOffset, recordCount and the compressed length to advance, all of which
// live in the header. Skipping the payload read is what makes a consume offset
// lookup cheap when small frames and a sparse index force a multi-frame walk.
//
// Safety: navigation that lands on the target hands off to readFrameAt, which
// validates the CRC, so a corrupt target is still caught on read. Frames merely
// skipped over are validated when they are themselves read (or by recovery /
// VerifyDurable). A corrupt header is caught by decodeHeader (magic/size), so a
// bad header triggers the caller's magic-resync rather than a wrong step. The
// caller must reject a frame whose computed end exceeds the segment size (a
// torn tail), since this read does not touch the payload to detect truncation.
func frameHeaderAt(r io.ReaderAt, pos int64) (frameHeader, int64, error) {
	if frameHeaderReadHook != nil {
		frameHeaderReadHook()
	}
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
	return h, pos + int64(headerSize) + int64(h.compressed), nil
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
	h, _, err := readFrameRaw(r, pos)
	if err != nil {
		return h, pos, err
	}
	return h, pos + int64(headerSize) + int64(h.compressed), nil
}

// verifyChunkBytes is the read granularity of verifyFrameAtBuffered: big
// enough to make a page-cache read cheap, small enough that the commit
// path never allocates a whole frame just to hash it.
const verifyChunkBytes = 64 << 10

// verifyFrameAtBuffered is verifyFrameAt without the frame-sized payload
// allocation: it streams the payload through *buf (grown once, reused
// across calls) while computing the CRC. Same errors as readFrameRaw.
func verifyFrameAtBuffered(r io.ReaderAt, pos int64, buf *[]byte) (frameHeader, int64, error) {
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

	if cap(*buf) < verifyChunkBytes {
		*buf = make([]byte, verifyChunkBytes)
	}
	chunk := (*buf)[:verifyChunkBytes]

	c := crc32.New(crc32cTable)
	c.Write(hdrBuf[2:23])
	remaining := int64(h.compressed)
	at := pos + headerSize
	for remaining > 0 {
		want := min(remaining, int64(len(chunk)))
		got, rerr := r.ReadAt(chunk[:want], at)
		if rerr != nil && rerr != io.EOF {
			return h, pos, rerr
		}
		if int64(got) < want {
			return h, pos, io.ErrUnexpectedEOF
		}
		c.Write(chunk[:got])
		remaining -= want
		at += want
	}
	if want, got := h.crc, c.Sum32(); want != got {
		return h, pos, fmt.Errorf("%w: crc want=0x%x got=0x%x at pos=%d", errCorrupt, want, got, pos)
	}
	return h, pos + int64(headerSize) + int64(h.compressed), nil
}
