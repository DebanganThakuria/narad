package storage

// Keyed records. Every record in a partition log is stored wrapped in
// a tiny self-describing envelope carrying the produce key and the
// commit timestamp. Fan-out re-keys parent records with the key; delay
// children anchor due times to the commit timestamp; consumers receive
// both as Message.Key and Message.Timestamp:
//
//	offset 0  [version: 1 byte]  0x01
//	offset 1  [keyLen: uvarint]
//	offset N  [key: keyLen bytes]
//	offset M  [committedAtUnixMs: 8 bytes big-endian]
//	offset P  [payload]

import (
	"encoding/binary"
	"fmt"
)

const keyedRecordVersion byte = 0x01

// EncodeKeyedRecord wraps a produce key, the commit timestamp, and the
// payload into the stored record envelope.
func EncodeKeyedRecord(key string, committedAtUnixMs int64, payload []byte) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen32+len(key)+8+len(payload))
	buf = append(buf, keyedRecordVersion)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(committedAtUnixMs))
	buf = append(buf, payload...)
	return buf
}

// DecodeKeyedRecord splits a stored record envelope into key, commit
// timestamp, and payload. The returned payload aliases the input.
// Errors wrap ErrCorruptRecord so callers classify a bad envelope like
// any other unreadable record.
func DecodeKeyedRecord(b []byte) (key string, committedAtUnixMs int64, payload []byte, err error) {
	if len(b) == 0 {
		return "", 0, nil, fmt.Errorf("%w: empty keyed record", ErrCorruptRecord)
	}
	if b[0] != keyedRecordVersion {
		return "", 0, nil, fmt.Errorf("%w: keyed record version %d", ErrCorruptRecord, b[0])
	}
	keyLen, n := binary.Uvarint(b[1:])
	if n <= 0 || keyLen > uint64(len(b)-1-n) {
		return "", 0, nil, fmt.Errorf("%w: keyed record key length", ErrCorruptRecord)
	}
	rest := b[1+n:]
	key = string(rest[:keyLen])
	rest = rest[keyLen:]
	if len(rest) < 8 {
		return "", 0, nil, fmt.Errorf("%w: keyed record timestamp truncated", ErrCorruptRecord)
	}
	committedAtUnixMs = int64(binary.BigEndian.Uint64(rest[:8]))
	return key, committedAtUnixMs, rest[8:], nil
}

// ReadKeyed reads the record at offset and splits it into produce key,
// commit timestamp, and payload.
func (l *Log) ReadKeyed(offset int64) (string, int64, []byte, error) {
	stored, err := l.Read(offset)
	if err != nil {
		return "", 0, nil, err
	}
	return DecodeKeyedRecord(stored)
}
