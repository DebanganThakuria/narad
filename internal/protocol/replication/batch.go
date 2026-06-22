package replication

import (
	"encoding/binary"
	"fmt"
	"io"
)

const frameLengthBytes = 4

func EncodeBatchPayload(records [][]byte) ([]byte, error) {
	var total int
	for _, record := range records {
		if len(record) > int(^uint32(0)) {
			return nil, fmt.Errorf("record too large: %d bytes", len(record))
		}
		total += frameLengthBytes + len(record)
	}

	out := make([]byte, 0, total)
	var lenBuf [frameLengthBytes]byte
	for _, record := range records {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(record)))
		out = append(out, lenBuf[:]...)
		out = append(out, record...)
	}
	return out, nil
}

func DecodeBatchPayload(r io.Reader, maxRecords int) ([][]byte, error) {
	var records [][]byte
	for {
		var lenBuf [frameLengthBytes]byte
		_, err := io.ReadFull(r, lenBuf[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			return nil, io.ErrUnexpectedEOF
		}
		if err != nil {
			return nil, err
		}
		if maxRecords > 0 && len(records) >= maxRecords {
			return nil, fmt.Errorf("too many records")
		}

		n := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, err
		}
		records = append(records, payload)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("empty batch")
	}
	return records, nil
}
