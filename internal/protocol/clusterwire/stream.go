// Package clusterwire defines the cluster stream-framing protocol used
// for node-to-node RPC over QUIC. Each request/response is a length-
// prefixed StreamFrame carrying an opaque payload (the node-RPC wire
// format lives in internal/protocol/node).
package clusterwire

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// MaxStreamFramePayloadBytes is the default cap on a frame payload
// (16 MiB), applied on write and, unless overridden, on read.
const MaxStreamFramePayloadBytes = 16 << 20

const (
	streamFrameHeaderBytes = 20
	streamMagic            = uint32(0x4e525331) // NRS1
	streamVersion          = byte(1)
)

// StreamFrameType identifies the kind of frame on a cluster stream.
type StreamFrameType uint8

// Frame types. Values are stable on the wire; gaps correspond to frame
// types retired with the follower-replication subsystem.
const (
	StreamFrameError       StreamFrameType = 3
	StreamFramePing        StreamFrameType = 4
	StreamFramePong        StreamFrameType = 5
	StreamFrameNodeRequest StreamFrameType = 8
	StreamFrameNodeReply   StreamFrameType = 9
)

// StreamFrame is one framed message on a cluster stream. On the wire it
// is a fixed 20-byte header followed by the payload:
//
//	[0:4]   magic "NRS1"
//	[4]     version (1)
//	[5]     frame type
//	[6:8]   reserved (zero on write, ignored on read)
//	[8:16]  request ID, big-endian
//	[16:20] payload length, big-endian
type StreamFrame struct {
	Type      StreamFrameType
	RequestID uint64
	Payload   []byte
}

// StreamError is the payload of a StreamFrameError reply.
type StreamError struct {
	Message string
}

// WriteStreamFrame writes frame to w in the wire layout described on
// StreamFrame. It rejects payloads larger than
// MaxStreamFramePayloadBytes.
func WriteStreamFrame(w io.Writer, frame StreamFrame) error {
	if len(frame.Payload) > MaxStreamFramePayloadBytes {
		return fmt.Errorf("stream frame payload too large: %d bytes", len(frame.Payload))
	}

	var header [streamFrameHeaderBytes]byte
	binary.BigEndian.PutUint32(header[0:4], streamMagic)
	header[4] = streamVersion
	header[5] = byte(frame.Type)
	binary.BigEndian.PutUint64(header[8:16], frame.RequestID)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(frame.Payload)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if len(frame.Payload) == 0 {
		return nil
	}
	_, err := w.Write(frame.Payload)
	return err
}

// ReadStreamFrame reads one frame from r, rejecting payloads larger
// than maxPayloadBytes. A maxPayloadBytes <= 0 means
// MaxStreamFramePayloadBytes.
func ReadStreamFrame(r io.Reader, maxPayloadBytes int) (StreamFrame, error) {
	if maxPayloadBytes <= 0 {
		maxPayloadBytes = MaxStreamFramePayloadBytes
	}

	var header [streamFrameHeaderBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return StreamFrame{}, err
	}
	if got := binary.BigEndian.Uint32(header[0:4]); got != streamMagic {
		return StreamFrame{}, fmt.Errorf("invalid stream magic: 0x%x", got)
	}
	if got := header[4]; got != streamVersion {
		return StreamFrame{}, fmt.Errorf("unsupported stream version: %d", got)
	}

	payloadLen := int(binary.BigEndian.Uint32(header[16:20]))
	if payloadLen > maxPayloadBytes {
		return StreamFrame{}, fmt.Errorf("stream frame payload too large: %d bytes", payloadLen)
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return StreamFrame{}, err
	}
	return StreamFrame{
		Type:      StreamFrameType(header[5]),
		RequestID: binary.BigEndian.Uint64(header[8:16]),
		Payload:   payload,
	}, nil
}

// EncodeStreamError encodes a StreamError payload: a big-endian uint16
// length followed by the message bytes. Messages longer than 64 KiB are
// truncated. The error result is always nil; the signature is kept for
// symmetry with other encoders.
func EncodeStreamError(message string) ([]byte, error) {
	if len(message) > math.MaxUint16 {
		message = message[:math.MaxUint16]
	}
	out := make([]byte, 2+len(message))
	binary.BigEndian.PutUint16(out[0:2], uint16(len(message)))
	copy(out[2:], message)
	return out, nil
}

// DecodeStreamError decodes a StreamError payload produced by
// EncodeStreamError.
func DecodeStreamError(payload []byte) (StreamError, error) {
	if len(payload) < 2 {
		return StreamError{}, io.ErrUnexpectedEOF
	}
	msgLen := int(binary.BigEndian.Uint16(payload[0:2]))
	if len(payload) < 2+msgLen {
		return StreamError{}, io.ErrUnexpectedEOF
	}
	return StreamError{Message: string(payload[2 : 2+msgLen])}, nil
}
