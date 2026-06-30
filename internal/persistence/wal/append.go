package wal

import (
	"errors"
	"fmt"
	"os"
)

func (l *Log) appendLocked(payload []byte) (RecordID, *syncBatch, error) {
	if l.closed {
		return RecordID{}, nil, errors.New("wal: log closed")
	}
	if l.syncErr != nil {
		return RecordID{}, nil, l.syncErr
	}
	if l.file == nil {
		return RecordID{}, nil, errors.New("wal: active file closed")
	}
	frameSize := frameHeaderSize + len(payload)
	if l.segmentSize > 0 && l.segmentSize+int64(frameSize) > l.opts.SegmentBytes {
		batch, err := l.syncLocked()
		completeBatch(batch, err)
		if err != nil {
			return RecordID{}, nil, err
		}
		if err := l.rollLocked(); err != nil {
			return RecordID{}, nil, err
		}
	}

	if l.pending == nil {
		l.pending = &syncBatch{done: make(chan struct{})}
	}
	batch := l.pending
	seq := l.nextSeq
	id := RecordID{SegmentBase: l.segmentBase, Offset: l.segmentSize, Seq: seq}
	l.writeBuffer = appendFrame(l.writeBuffer, seq, payload)
	l.segmentSize += int64(frameSize)
	l.nextSeq++
	return id, batch, nil
}

func (l *Log) rollLocked() error {
	l.fileOps.Lock()
	defer l.fileOps.Unlock()

	if err := l.file.Close(); err != nil {
		return fmt.Errorf("wal: close rolled segment: %w", err)
	}
	l.segmentBase = l.nextSeq
	l.segmentSize = 0
	path := segmentPath(l.dir, l.segmentBase)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment: %w", err)
	}
	l.file = file
	return nil
}

func (l *Log) signalSync() {
	select {
	case l.wakeup <- struct{}{}:
	default:
	}
}
