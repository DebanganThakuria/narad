package ingress

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// produceRecordFormat versions the on-disk record layout. Bump only
// with a decoder that still accepts every prior format.
const produceRecordFormat byte = 1

// EncodeProduceRecord serializes a record into the ingress WAL payload
// format: a format byte followed by length-prefixed fields, all
// big-endian. The record's WAL field is not encoded — the WAL assigns
// it at append time.
func EncodeProduceRecord(record ProduceRecord) ([]byte, error) {
	if record.Topic == "" {
		return nil, errors.New("ingress: topic required")
	}
	if record.TargetPartition < 0 {
		return nil, errors.New("ingress: target partition must be >= 0")
	}
	if len(record.Payload) == 0 {
		return nil, errors.New("ingress: payload required")
	}

	size := 1 + stringSize(record.Topic) + stringSize(record.Key) + 4 + 8 + bytesSize(record.Payload)
	out := make([]byte, 0, size)
	out = append(out, produceRecordFormat)
	out = appendString(out, record.Topic)
	out = appendString(out, record.Key)
	out = binary.BigEndian.AppendUint32(out, uint32(record.TargetPartition))
	out = binary.BigEndian.AppendUint64(out, uint64(record.CreatedAtUnixMs))
	out = appendBytes(out, record.Payload)
	return out, nil
}

// DecodeProduceRecord parses a payload written by EncodeProduceRecord.
// It rejects unknown formats, truncated fields, and trailing bytes, so
// a corrupt WAL frame can never decode into a plausible-looking record.
func DecodeProduceRecord(data []byte) (ProduceRecord, error) {
	r := byteReader{data: data}
	format, err := r.u8()
	if err != nil {
		return ProduceRecord{}, err
	}
	if format != produceRecordFormat {
		return ProduceRecord{}, fmt.Errorf("ingress: unsupported produce record format %d", format)
	}
	topicName, err := r.string()
	if err != nil {
		return ProduceRecord{}, err
	}
	key, err := r.string()
	if err != nil {
		return ProduceRecord{}, err
	}
	partition, err := r.u32()
	if err != nil {
		return ProduceRecord{}, err
	}
	createdAt, err := r.u64()
	if err != nil {
		return ProduceRecord{}, err
	}
	payload, err := r.bytes()
	if err != nil {
		return ProduceRecord{}, err
	}
	if err := r.done(); err != nil {
		return ProduceRecord{}, err
	}
	return ProduceRecord{
		Topic:           topicName,
		Key:             key,
		TargetPartition: int(partition),
		Payload:         payload,
		CreatedAtUnixMs: int64(createdAt),
	}, nil
}

func stringSize(s string) int {
	return 4 + len(s)
}

func bytesSize(b []byte) int {
	return 4 + len(b)
}

func appendString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(s)))
	return append(dst, s...)
}

func appendBytes(dst []byte, b []byte) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b)))
	return append(dst, b...)
}

// byteReader is a bounds-checked cursor over an encoded record. bytes
// copies out of the backing slice so decoded records never alias WAL
// read buffers.
type byteReader struct {
	data []byte
	off  int
}

func (r *byteReader) u8() (byte, error) {
	if len(r.data)-r.off < 1 {
		return 0, io.ErrUnexpectedEOF
	}
	v := r.data[r.off]
	r.off++
	return v, nil
}

func (r *byteReader) u32() (uint32, error) {
	if len(r.data)-r.off < 4 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(r.data[r.off : r.off+4])
	r.off += 4
	return v, nil
}

func (r *byteReader) u64() (uint64, error) {
	if len(r.data)-r.off < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(r.data[r.off : r.off+8])
	r.off += 8
	return v, nil
}

func (r *byteReader) string() (string, error) {
	data, err := r.bytes()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (r *byteReader) bytes() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if len(r.data)-r.off < int(n) {
		return nil, io.ErrUnexpectedEOF
	}
	out := append([]byte(nil), r.data[r.off:r.off+int(n)]...)
	r.off += int(n)
	return out, nil
}

func (r *byteReader) done() error {
	if r.off != len(r.data) {
		return fmt.Errorf("ingress: trailing bytes: %d", len(r.data)-r.off)
	}
	return nil
}
