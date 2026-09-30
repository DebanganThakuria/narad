package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

// A frame is the on-disk encoding of one record, all fields big endian:
//
//	offset  size  field
//	0       4     magic "NWAL" (0x4e57414c)
//	4       4     payload length (never zero)
//	8       8     sequence number
//	16      4     CRC32 (IEEE) of the payload
//	20      n     payload
const (
	frameMagic      uint32 = 0x4e57414c // NWAL
	frameHeaderSize        = 20
)

// errCorruptFrame marks frame validation failures (bad magic, bad length,
// checksum mismatch) as opposed to I/O errors. Open-time recovery treats a
// corrupt frame in the last (active) segment as a torn tail to truncate.
var errCorruptFrame = errors.New("corrupt frame")

// errZeroHeader is readFrame's report of a frame header that is all zero
// bytes: the unwritten space of a prepared segment (see SegmentPrealloc),
// where the active segment's data ends. It wraps errCorruptFrame, so
// Open-time recovery treats it exactly like the bad magic it is to older
// binaries: the end of the last segment when no valid frame follows
// (truncated like any torn tail), corruption anywhere else. Rolls trim a
// prepared segment before sealing it, so a sealed segment never ends in
// zeros. Replay stops at it as it does at end of file.
var errZeroHeader = fmt.Errorf("wal: zero frame header: %w", errCorruptFrame)

// appendFrame encodes one record onto dst, growing it geometrically like
// append so repeated staging into the shared write buffer stays cheap.
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

// appendFrameWith is appendFrame for a payload produced in place: it
// reserves the frame, lets fill write the payload directly into the
// reserved region, then stamps the header (including the CRC over what
// fill wrote). fill must append exactly size bytes; anything else is an
// error and the buffer is left unchanged.
func appendFrameWith(dst []byte, seq uint64, size int, fill func(dst []byte) []byte) ([]byte, error) {
	start := len(dst)
	end := start + frameHeaderSize + size
	if cap(dst) < end {
		nextCap := end
		if doubled := cap(dst) * 2; doubled > nextCap {
			nextCap = doubled
		}
		next := make([]byte, len(dst), nextCap)
		copy(next, dst)
		dst = next
	}
	payloadStart := start + frameHeaderSize
	filled := fill(dst[:payloadStart])
	// The capacity reserved above is enough for the whole frame, so a
	// well-behaved fill appends in place: same backing array, exactly end
	// bytes. dst may be empty here (first record of a batch), so compare
	// through one-element reslices, which are valid whenever cap >= 1.
	if len(filled) != end || cap(filled) < 1 || &filled[:1][0] != &dst[:1][0] {
		return dst[:start], fmt.Errorf("wal: fill wrote %d bytes, want %d", len(filled)-payloadStart, size)
	}
	dst = filled
	frame := dst[start:end]
	binary.BigEndian.PutUint32(frame[0:4], frameMagic)
	binary.BigEndian.PutUint32(frame[4:8], uint32(size))
	binary.BigEndian.PutUint64(frame[8:16], seq)
	binary.BigEndian.PutUint32(frame[16:20], crc32.ChecksumIEEE(frame[frameHeaderSize:]))
	return dst, nil
}

// replayReadBufferSize is the read-ahead used when scanning a segment.
// Reading through a buffer turns the three syscalls per record of the
// unbuffered form (seek, header read, payload read) into one large read
// per 64 KiB; the byte position is tracked arithmetically, which is
// exactly what CursorAfter already assumes.
const replayReadBufferSize = 64 << 10

// frameReader reads frames sequentially from one segment through a
// buffered reader. Readers are pooled: the dispatcher replays the WAL
// every few milliseconds, and a fresh 64 KiB buffer per pass was most of
// its garbage. The header is read into the reader's own array rather
// than a local one, which escaped to the heap once per frame through the
// io.Reader call.
type frameReader struct {
	r      *bufio.Reader
	header [frameHeaderSize]byte
}

var frameReaderPool = sync.Pool{
	New: func() any { return &frameReader{r: bufio.NewReaderSize(nil, replayReadBufferSize)} },
}

// getFrameReader returns a pooled frameReader reading from src. Release
// it with putFrameReader once the scan is done.
func getFrameReader(src io.Reader) *frameReader {
	fr := frameReaderPool.Get().(*frameReader)
	fr.r.Reset(src)
	return fr
}

// putFrameReader returns fr to the pool, dropping its source so the pool
// does not keep a closed file reachable.
func putFrameReader(fr *frameReader) {
	fr.r.Reset(nil)
	frameReaderPool.Put(fr)
}

// frameHeader is a validated frame header.
type frameHeader struct {
	seq  uint64
	size int // payload length, never zero
	crc  uint32
}

// readFrame decodes the next frame. It returns ok=false without an error
// on a clean or truncated EOF (a torn tail), errZeroHeader for an
// all-zero header, and wraps validation failures in errCorruptFrame so
// callers can distinguish them from I/O errors. The returned payload is
// a fresh allocation that the reader never touches again, so a caller
// may keep it or alias into it.
func (fr *frameReader) readFrame(segmentBase uint64, offset int64, maxRecord int) (Record, bool, error) {
	h, ok, err := fr.readHeader(offset, maxRecord)
	if err != nil || !ok {
		return Record{}, false, err
	}
	payload, ok, err := fr.readPayload(h, offset)
	if err != nil || !ok {
		return Record{}, false, err
	}
	return Record{
		ID:      RecordID{SegmentBase: segmentBase, Offset: offset, Seq: h.seq},
		Payload: payload,
	}, true, nil
}

// readHeader reads and validates the header of the next frame, which
// starts at offset. Errors and ok as for readFrame; the payload is left
// for readPayload or skipPayload.
func (fr *frameReader) readHeader(offset int64, maxRecord int) (frameHeader, bool, error) {
	header := &fr.header
	if _, err := io.ReadFull(fr.r, header[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return frameHeader{}, false, nil
		}
		return frameHeader{}, false, fmt.Errorf("wal: read frame header: %w", err)
	}
	if *header == ([frameHeaderSize]byte{}) {
		return frameHeader{}, false, errZeroHeader
	}
	if got := binary.BigEndian.Uint32(header[0:4]); got != frameMagic {
		return frameHeader{}, false, fmt.Errorf("wal: bad frame magic at offset %d: %w", offset, errCorruptFrame)
	}
	n := binary.BigEndian.Uint32(header[4:8])
	if n == 0 {
		return frameHeader{}, false, fmt.Errorf("wal: empty frame at offset %d: %w", offset, errCorruptFrame)
	}
	if int(n) > maxRecord {
		return frameHeader{}, false, fmt.Errorf("wal: frame size %d exceeds max %d: %w", n, maxRecord, errCorruptFrame)
	}
	return frameHeader{
		seq:  binary.BigEndian.Uint64(header[8:16]),
		size: int(n),
		crc:  binary.BigEndian.Uint32(header[16:20]),
	}, true, nil
}

// readPayload reads the payload of the frame whose header readHeader
// just returned into a fresh allocation and verifies its checksum.
// ok=false on a truncated EOF.
func (fr *frameReader) readPayload(h frameHeader, offset int64) ([]byte, bool, error) {
	payload := make([]byte, h.size)
	if _, err := io.ReadFull(fr.r, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("wal: read frame payload: %w", err)
	}
	if got := crc32.ChecksumIEEE(payload); got != h.crc {
		return nil, false, fmt.Errorf("wal: checksum mismatch at offset %d: %w", offset, errCorruptFrame)
	}
	return payload, true, nil
}

// skipPayload consumes the payload of the frame whose header readHeader
// just returned without keeping it: the checksum is computed over the
// bytes as they pass through the read buffer, so a skipped record is
// verified exactly like a delivered one but never allocated. ok=false
// on a truncated EOF.
func (fr *frameReader) skipPayload(h frameHeader, offset int64) (bool, error) {
	var sum uint32
	for left := h.size; left > 0; {
		chunk, err := fr.r.Peek(min(left, fr.r.Size()))
		sum = crc32.Update(sum, crc32.IEEETable, chunk)
		_, _ = fr.r.Discard(len(chunk))
		left -= len(chunk)
		if err != nil && left > 0 {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, fmt.Errorf("wal: read frame payload: %w", err)
		}
	}
	if sum != h.crc {
		return false, fmt.Errorf("wal: checksum mismatch at offset %d: %w", offset, errCorruptFrame)
	}
	return true, nil
}
