package node

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

type writer struct {
	buf []byte
}

func opWriter(op Operation, capacity int) *writer {
	w := newWriter(1 + capacity)
	w.buf = append(w.buf, byte(op))
	return w
}

func newWriter(capacity int) *writer {
	if capacity < 0 {
		capacity = 0
	}
	return &writer{buf: make([]byte, 0, capacity)}
}

func (w *writer) u16(v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) i32(v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) i64(v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	w.buf = append(w.buf, b[:]...)
}

func (w *writer) bool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
		return
	}
	w.buf = append(w.buf, 0)
}

func (w *writer) string(v string) error {
	return w.bytes([]byte(v))
}

func (w *writer) bytes(v []byte) error {
	if len(v) > math.MaxUint32 {
		return fmt.Errorf("field too large: %d bytes", len(v))
	}
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(v)))
	w.buf = append(w.buf, b[:]...)
	w.buf = append(w.buf, v...)
	return nil
}

func (w *writer) bytesOut() []byte {
	return w.buf
}

type reader struct {
	payload []byte
	pos     int
}

func opReader(payload []byte, expected Operation) (*reader, error) {
	r := newReader(payload)
	op, err := r.op()
	if err != nil {
		return nil, err
	}
	if op != expected {
		return nil, fmt.Errorf("unexpected operation %d, want %d", op, expected)
	}
	return r, nil
}

func newReader(payload []byte) *reader {
	return &reader{payload: payload}
}

func (r *reader) op() (Operation, error) {
	if r.remaining() < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	op := Operation(r.payload[r.pos])
	r.pos++
	return op, nil
}

func (r *reader) u16() (uint16, error) {
	if r.remaining() < 2 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint16(r.payload[r.pos : r.pos+2])
	r.pos += 2
	return v, nil
}

func (r *reader) i32() (int32, error) {
	if r.remaining() < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := int32(binary.BigEndian.Uint32(r.payload[r.pos : r.pos+4]))
	r.pos += 4
	return v, nil
}

func (r *reader) i64() (int64, error) {
	if r.remaining() < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := int64(binary.BigEndian.Uint64(r.payload[r.pos : r.pos+8]))
	r.pos += 8
	return v, nil
}

func (r *reader) bool() (bool, error) {
	if r.remaining() < 1 {
		return false, io.ErrUnexpectedEOF
	}
	v := r.payload[r.pos]
	r.pos++
	switch v {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("invalid bool value %d", v)
	}
}

func (r *reader) string() (string, error) {
	v, err := r.bytes()
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (r *reader) bytes() ([]byte, error) {
	if r.remaining() < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint32(r.payload[r.pos : r.pos+4]))
	r.pos += 4
	if n < 0 || r.remaining() < n {
		return nil, io.ErrUnexpectedEOF
	}
	out := r.payload[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *reader) done() error {
	if r.pos != len(r.payload) {
		return errors.New("trailing node rpc payload data")
	}
	return nil
}

func (r *reader) remaining() int {
	return len(r.payload) - r.pos
}

func fieldLen(v string) int {
	return fieldLenBytes([]byte(v))
}

func fieldLenBytes(v []byte) int {
	return 4 + len(v)
}
