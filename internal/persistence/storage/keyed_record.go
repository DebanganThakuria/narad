package storage

// Keyed records. Every record in a partition log is stored wrapped in
// a tiny self-describing envelope so the produce key survives the
// commit — fan-out needs it to re-key parent records into child
// partitions, and consumers receive it as Message.Key:
//
//	offset 0  [version: 1 byte]  0x01
//	offset 1  [keyLen: uvarint]
//	offset N  [key: keyLen bytes]
//	offset M  [payload]

import (
	"encoding/binary"
	"fmt"
)

const keyedRecordVersion byte = 0x01

// EncodeKeyedRecord wraps a produce key and payload into the stored
// record envelope.
func EncodeKeyedRecord(key string, payload []byte) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen32+len(key)+len(payload))
	buf = append(buf, keyedRecordVersion)
	buf = binary.AppendUvarint(buf, uint64(len(key)))
	buf = append(buf, key...)
	buf = append(buf, payload...)
	return buf
}

// DecodeKeyedRecord splits a stored record envelope into key and
// payload. The returned payload aliases the input. Errors wrap
// ErrCorruptRecord so callers classify a bad envelope like any other
// unreadable record.
func DecodeKeyedRecord(b []byte) (key string, payload []byte, err error) {
	if len(b) == 0 {
		return "", nil, fmt.Errorf("%w: empty keyed record", ErrCorruptRecord)
	}
	if b[0] != keyedRecordVersion {
		return "", nil, fmt.Errorf("%w: keyed record version %d", ErrCorruptRecord, b[0])
	}
	keyLen, n := binary.Uvarint(b[1:])
	if n <= 0 || keyLen > uint64(len(b)-1-n) {
		return "", nil, fmt.Errorf("%w: keyed record key length", ErrCorruptRecord)
	}
	rest := b[1+n:]
	return string(rest[:keyLen]), rest[keyLen:], nil
}

// ReadKeyed reads the record at offset and splits it into produce key
// and payload.
func (l *Log) ReadKeyed(offset int64) (string, []byte, error) {
	stored, err := l.Read(offset)
	if err != nil {
		return "", nil, err
	}
	return DecodeKeyedRecord(stored)
}
