package storage

import (
	"encoding/binary"
	"io"
)

// Append writes data to the end of the log and returns the assigned
// offset. The write is fsynced before returning, then any waiting
// long-poller is woken via NotifyC.
func (l *Log) Append(data []byte) (int64, error) {
	offset := l.nextOffset

	pos, err := l.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, err
	}

	length := int32(len(data))

	if err = binary.Write(l.file, binary.BigEndian, length); err != nil {
		return -1, err
	}
	if _, err = l.file.Write(data); err != nil {
		return -1, err
	}
	if err = l.file.Sync(); err != nil {
		return -1, err
	}

	l.index[offset] = pos
	l.nextOffset++

	select {
	case l.notify <- struct{}{}:
	default:
	}

	return offset, nil
}

// NotifyC returns a channel that emits a value whenever a new record is
// appended. The channel is shared and buffered (size 1) — wake-ups
// coalesce; treat each receive as "something new, go check".
func (l *Log) NotifyC() <-chan struct{} {
	return l.notify
}
