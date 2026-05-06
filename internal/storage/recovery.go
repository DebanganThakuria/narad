package storage

import (
	"encoding/binary"
	"io"
)

// recover replays the log file from the start, rebuilding the in-memory
// offset->position index. If the tail of the file is corrupt or torn
// (partial write following a crash), it is truncated. Returns the next
// offset to assign on Append.
//
// This implements PRD §9 "Log Recovery": scan -> validate -> truncate
// invalid tail -> rebuild index -> resume from last offset.
func (l *Log) recover() (int64, error) {
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var offset int64 = 0

	for {
		// Current position = start of record
		pos, err := l.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}

		var length int32
		err = binary.Read(l.file, binary.BigEndian, &length)
		if err != nil {
			if err == io.EOF {
				break // clean end
			}
			// Corrupt tail → truncate
			if err := l.truncate(pos); err != nil {
				return 0, err
			}
			break
		}

		// Invalid length → corruption
		if length <= 0 {
			if err := l.truncate(pos); err != nil {
				return 0, err
			}
			break
		}

		// Check if full payload exists
		nextPos := pos + 4 + int64(length)

		fileEnd, err := l.file.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}

		if nextPos > fileEnd {
			// Partial write → truncate
			if err := l.truncate(pos); err != nil {
				return 0, err
			}
			break
		}

		l.index[offset] = pos

		if _, err := l.file.Seek(pos+4+int64(length), io.SeekStart); err != nil {
			return 0, err
		}

		offset++
	}

	// Move to end for appends
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}

	return offset, nil
}

// truncate cuts the file at pos and seeks there so subsequent Appends
// land at the right offset.
func (l *Log) truncate(pos int64) error {
	if err := l.file.Truncate(pos); err != nil {
		return err
	}
	_, err := l.file.Seek(pos, io.SeekStart)
	return err
}
