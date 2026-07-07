package storage

// Keyed records. Historically a partition log stored only the produce
// payload, which made the produce key unrecoverable after commit —
// fan-out needs it to re-key parent records into child partitions (and
// consumers get Message.Key populated as a side benefit). Records are
// now stored wrapped in a tiny self-describing envelope:
//
//	offset 0  [version: 1 byte]  0x01
//	offset 1  [keyLen: uvarint]
//	offset N  [key: keyLen bytes]
//	offset M  [payload]
//
// Logs written before this format hold bare payloads, so each log
// persists the offset its keyed records start at (the "keyed.from"
// marker, written once at first open) and ReadKeyed treats everything
// below it as an unkeyed bare payload. Downgrading a node after keyed
// records were written is not supported: an old binary would serve the
// envelope bytes as the payload.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	keyedRecordVersion byte = 0x01

	keyedFromFileName = "keyed.from"
)

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
// and payload. Records below the log's keyed.from marker predate the
// envelope and are returned as key-less bare payloads.
func (l *Log) ReadKeyed(offset int64) (string, []byte, error) {
	stored, err := l.Read(offset)
	if err != nil {
		return "", nil, err
	}
	if offset < l.keyedFrom {
		return "", stored, nil
	}
	return DecodeKeyedRecord(stored)
}

// KeyedFromOffset returns the first offset whose record carries the
// keyed envelope. Records below it are bare payloads.
func (l *Log) KeyedFromOffset() int64 { return l.keyedFrom }

// loadOrInitKeyedFrom reads the log's keyed.from marker, writing it at
// the recovered tail on first open under an envelope-aware binary so
// pre-existing records keep reading as bare payloads. nextOffset is the
// recovered append position: everything below it was written before
// this open.
func loadOrInitKeyedFrom(dir string, nextOffset int64) (int64, error) {
	path := filepath.Join(dir, keyedFromFileName)
	buf, err := os.ReadFile(path)
	switch {
	case err == nil:
		if len(buf) != 8 {
			return 0, fmt.Errorf("storage: keyed.from marker corrupt: got %d bytes, want 8", len(buf))
		}
		return int64(binary.BigEndian.Uint64(buf)), nil
	case errors.Is(err, os.ErrNotExist):
		if err := writeOffsetFile(dir, keyedFromFileName, nextOffset); err != nil {
			return 0, fmt.Errorf("storage: persist keyed.from marker: %w", err)
		}
		return nextOffset, nil
	default:
		return 0, err
	}
}
