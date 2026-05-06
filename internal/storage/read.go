package storage

import (
	"encoding/binary"
	"io"
)

// Read returns the payload bytes stored at the given offset.
func (l *Log) Read(offset int64) ([]byte, error) {
	pos, ok := l.index[offset]
	if !ok {
		return nil, ErrOffsetNotFound
	}

	if _, err := l.file.Seek(pos, io.SeekStart); err != nil {
		return nil, err
	}

	var length int32
	if err := binary.Read(l.file, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(l.file, data); err != nil {
		return nil, err
	}

	return data, nil
}
