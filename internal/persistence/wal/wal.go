// Package wal provides a small segmented write-ahead log with grouped fsync.
package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type RecordID struct {
	SegmentBase uint64
	Offset      int64
	Seq         uint64
}

type Cursor struct {
	SegmentBase uint64
	Offset      int64
	Seq         uint64
}

type Record struct {
	ID      RecordID
	Payload []byte
}

type Log struct {
	dir  string
	opts Options

	mu          sync.Mutex
	fileOps     sync.Mutex
	file        *os.File
	segmentBase uint64
	segmentSize int64
	nextSeq     uint64
	unsynced    int64
	writeBuffer []byte
	pending     *syncBatch
	closed      bool
	syncErr     error

	wakeup chan struct{}
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
}

type syncBatch struct {
	done chan struct{}
	err  error
}

func Open(dir string, opts Options) (*Log, error) {
	opts = normalizeOptions(opts)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	segments, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(segments) == 0 {
		segments = []segmentInfo{{base: 0, path: segmentPath(dir, 0)}}
		if err := createEmptySegment(segments[0].path); err != nil {
			return nil, err
		}
	}

	nextSeq, lastValidEnd, err := scanForOpen(segments, opts.MaxRecord)
	if err != nil {
		return nil, err
	}
	last := segments[len(segments)-1]
	if nextSeq < last.base {
		nextSeq = last.base
	}
	file, err := os.OpenFile(last.path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open active segment: %w", err)
	}
	if err := file.Truncate(lastValidEnd); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("wal: truncate active segment: %w", err)
	}
	if _, err := file.Seek(lastValidEnd, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("wal: seek active segment: %w", err)
	}

	l := &Log{
		dir:         dir,
		opts:        opts,
		file:        file,
		segmentBase: last.base,
		segmentSize: lastValidEnd,
		nextSeq:     nextSeq,
		wakeup:      make(chan struct{}, 1),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
	}
	go l.syncLoop()
	return l, nil
}

func (l *Log) Append(ctx context.Context, payload []byte) (RecordID, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RecordID{}, err
	}
	if len(payload) == 0 {
		return RecordID{}, errors.New("wal: empty payload")
	}
	if len(payload) > l.opts.MaxRecord {
		return RecordID{}, fmt.Errorf("wal: payload size %d exceeds max %d", len(payload), l.opts.MaxRecord)
	}

	l.mu.Lock()
	id, batch, err := l.appendLocked(payload)
	l.mu.Unlock()
	if err != nil {
		return RecordID{}, err
	}

	l.signalSync()

	// The record is now in the write buffer with a committed seq; the
	// sync loop will durably persist it and it will be replayed after a
	// restart regardless of ctx. Abandoning the wait on ctx.Done() here
	// would report failure for a record that is in fact durable, which a
	// retrying caller would then duplicate. So past the append we wait
	// for the true sync outcome rather than honouring cancellation.
	// (ctx is still checked before the append, at the top of Append.)
	<-batch.done
	return id, batch.err
}

func (l *Log) Close() error {
	l.once.Do(func() { close(l.stop) })
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return l.syncErr
	}
	err := l.file.Close()
	l.file = nil
	if l.syncErr != nil {
		return l.syncErr
	}
	return err
}
