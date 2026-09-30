package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"

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

// decodeRecordsPayload splits a record stream into slices of payload,
// not copies: they share its lifetime and its memory.
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

// encodeFrame builds one frame with fresh buffers. The flusher borrows
// a frameEncoder (frameEncoders) to reuse buffers across flushes; this
// form serves tests and one-off callers.
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

// frameEncoders lends encoders to the flushers for one frame at a time.
// Each Log used to own one, so the encode buffers of the largest batch a
// partition ever wrote (up to 2 x maxRetainedFrameBuffer) stayed pinned
// for the life of the Log on every partition that ever committed; the
// pool bounds them by the frames being encoded at once, and the GC
// drops idle ones.
var frameEncoders = sync.Pool{New: func() any { return new(frameEncoder) }}

// encodeFrame encodes records into a frame: header, then the codec's
// output. The codec appends straight into the frame buffer after the
// header slot (both codecs append to dst), so there is no intermediate
// "encoded" buffer and no copy. An uncompressed frame's payload is the
// record stream itself, so it is built in place after the header with
// no staging buffer at all.
func (enc *frameEncoder) encodeFrame(records [][]byte, baseOffset int64, c codec.Codec) ([]byte, error) {
	if len(records) == 0 {
		return nil, errors.New("storage: encodeFrame: empty batch")
	}

	innerSize := 0
	for _, r := range records {
		innerSize += 4 + len(r)
	}
	var frame []byte
	if c.Flag() == codec.FlagNone {
		if cap(enc.frame) < headerSize+innerSize {
			enc.frame = make([]byte, 0, headerSize+innerSize)
		}
		frame = encodeRecordsPayload(enc.frame[:headerSize], records)
	} else {
		if cap(enc.inner) < innerSize {
			enc.inner = make([]byte, 0, innerSize)
		}
		inner := encodeRecordsPayload(enc.inner[:0], records)
		if cap(enc.frame) < headerSize {
			enc.frame = make([]byte, 0, headerSize+innerSize)
		}
		frame = c.Encode(enc.frame[:headerSize], inner)
		if cap(inner) <= maxRetainedFrameBuffer {
			enc.inner = inner[:0]
		} else {
			enc.inner = nil
		}
	}
	encodedLen := len(frame) - headerSize

	// Mirror decodeHeader's read-time bound: a frame past maxFrameBytes
	// would be written but rejected as corrupt on every read (a poison
	// frame), so refuse it at write time instead.
	if innerSize > maxFrameBytes || encodedLen > maxFrameBytes {
		enc.release()
		return nil, fmt.Errorf("storage: frame too large: uncompressed=%d compressed=%d", innerSize, encodedLen)
	}

	encodeHeader(frame[:headerSize], frameHeader{
		flags:        c.Flag() & codecMask,
		recordCount:  int32(len(records)),
		baseOffset:   baseOffset,
		uncompressed: int32(innerSize),
		compressed:   int32(encodedLen),
	})

	crc := crc32cOf(frame[2:23], frame[headerSize:])
	binary.BigEndian.PutUint32(frame[23:27], crc)

	// Keep the (possibly grown) frame buffer for the next flush, within
	// bounds.
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

// readAt is r.ReadAt that lets b stay on the caller's stack. A slice
// passed through the io.ReaderAt interface escapes, which cost every
// frame header read on the commit and read paths a heap allocation; a
// segment is always an *os.File, whose ReadAt does not retain b. Other
// readers (tests) read into a copy.
func readAt(r io.ReaderAt, b []byte, off int64) (int, error) {
	if f, ok := r.(*os.File); ok {
		return f.ReadAt(b, off)
	}
	tmp := make([]byte, len(b))
	n, err := r.ReadAt(tmp, off)
	copy(b, tmp[:n])
	return n, err
}

// readFrameRaw reads the header and raw (still-encoded) payload of the
// frame at pos and validates the CRC, without decoding. readFrameAt
// goes on to decode; a caller that only needs the CRC check uses
// verifyFrameAtBuffered, which does not hold the whole frame.
//
// Errors:
//   - errBadMagic: header magic mismatch (caller resyncs)
//   - errCorrupt:  CRC mismatch
//   - io.ErrUnexpectedEOF: torn tail
func readFrameRaw(r io.ReaderAt, pos int64) (frameHeader, []byte, error) {
	var hdrBuf [headerSize]byte
	n, err := readAt(r, hdrBuf[:], pos)
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

	// The header rides in front of the payload in one allocation: the CRC
	// covers both, and hashing hdrBuf itself would move it to the heap
	// (crc32 leaks its input), a second allocation per frame read.
	buf := make([]byte, headerSize+int(h.compressed))
	copy(buf, hdrBuf[:])
	payload := buf[headerSize:]
	n, err = r.ReadAt(payload, pos+headerSize)
	if err != nil && err != io.EOF {
		return h, nil, err
	}
	if n < int(h.compressed) {
		return h, nil, io.ErrUnexpectedEOF
	}

	if want, got := h.crc, crc32cOf(buf[2:23], payload); want != got {
		return h, nil, fmt.Errorf("%w: crc want=0x%x got=0x%x at pos=%d", errCorrupt, want, got, pos)
	}
	return h, payload, nil
}

// readFrameAt reads, CRC-checks, and decodes the frame at pos, returning
// its records and the position just after the frame. Errors are those of
// readFrameRaw, plus errCorrupt when the decoded record stream is invalid.
//
// The records are slices of one buffer this call allocated (the payload
// read from the file for an uncompressed frame, the codec's output
// otherwise) that nothing else references, so a caller may keep them
// without copying; they pin that buffer while it does.
func readFrameAt(r io.ReaderAt, pos int64, log *Log) (frameHeader, [][]byte, int64, error) {
	h, payload, err := readFrameRaw(r, pos)
	if err != nil {
		return h, nil, pos, err
	}

	// An uncompressed frame's payload IS the record stream: split it in
	// place instead of copying it through the noop codec.
	decoded := payload
	if h.codec() != codec.FlagNone {
		c, err := codecForFlag(h.codec(), log.codec)
		if err != nil {
			return h, nil, pos, err
		}
		decoded, err = c.Decode(nil, payload, int(h.uncompressed))
		if err != nil {
			return h, nil, pos, fmt.Errorf("%w: decode: %v", errCorrupt, err)
		}
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
	n, err := readAt(r, hdrBuf[:], pos)
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

// verifyChunkBytes is the read granularity of verifyFrameAtBuffered: big
// enough to make a page-cache read cheap, small enough that the commit
// path never allocates a whole frame just to hash it.
const verifyChunkBytes = 64 << 10

// verifyChunks lends verifyFrameAtBuffered its read buffer for one
// verify. Each Log's flusher used to keep its own: 64 KiB of heap on
// every partition that ever committed, however small its frames. The
// list holds at most one chunk per P, whoever returned it. (A sync.Pool
// keeps a lone chunk in the private slot of the P that put it back, out
// of reach of a flusher that next runs on another P, so a migrating
// flusher kept allocating fresh ones.)
var verifyChunks = make(chunkList, runtime.GOMAXPROCS(0))

// chunkList is a bounded free list of verifyChunkBytes buffers.
type chunkList chan *[]byte

func (c chunkList) get() *[]byte {
	select {
	case b := <-c:
		return b
	default:
		b := make([]byte, verifyChunkBytes)
		return &b
	}
}

func (c chunkList) put(b *[]byte) {
	select {
	case c <- b:
	default:
	}
}

// verifyFrameAtBuffered re-reads the frame at pos and validates its CRC
// over the raw (possibly compressed) on-disk bytes, without decoding. It
// returns the frame header (record count, base offset) and the position
// just after the frame. It streams the payload through *buf (grown to
// verifyChunkBytes if smaller) instead of allocating the frame, so the
// durability read-back (VerifyDurable), the recovery walk and the
// resync past a corrupt frame (nextValidFramePos) never allocate per
// frame. Same errors as readFrameRaw.
func verifyFrameAtBuffered(r io.ReaderAt, pos int64, buf *[]byte) (frameHeader, int64, error) {
	var hdrBuf [headerSize]byte
	n, err := readAt(r, hdrBuf[:], pos)
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

	// Hash the header from the chunk, not from hdrBuf: crc32 leaks its
	// input, which would move hdrBuf to the heap on every commit.
	crc := crc32.Update(0, crc32cTable, chunk[:copy(chunk, hdrBuf[2:23])])
	remaining := int64(h.compressed)
	at := pos + headerSize
	for remaining > 0 {
		want := min(remaining, int64(len(chunk)))
		got, rerr := readAt(r, chunk[:want], at)
		if rerr != nil && rerr != io.EOF {
			return h, pos, rerr
		}
		if int64(got) < want {
			return h, pos, io.ErrUnexpectedEOF
		}
		crc = crc32.Update(crc, crc32cTable, chunk[:got])
		remaining -= want
		at += want
	}
	if want, got := h.crc, crc; want != got {
		return h, pos, fmt.Errorf("%w: crc want=0x%x got=0x%x at pos=%d", errCorrupt, want, got, pos)
	}
	return h, pos + int64(headerSize) + int64(h.compressed), nil
}
