package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

func (l *Log) syncLoop() {
	defer close(l.done)
	timer := time.NewTimer(l.opts.SyncInterval)
	defer timer.Stop()

	for {
		select {
		case <-l.wakeup:
		case <-timer.C:
		case <-l.stop:
			l.mu.Lock()
			l.closed = true
			l.mu.Unlock()
			l.flushSync()
			return
		}
		l.flushSync()
		resetTimer(timer, l.opts.SyncInterval)
	}
}

func (l *Log) flushSync() {
	l.mu.Lock()
	if len(l.writeBuffer) == 0 {
		l.mu.Unlock()
		return
	}
	file := l.file
	buffer := l.writeBuffer
	batch := l.pending
	l.writeBuffer = nil
	l.pending = nil
	l.unsynced = 0

	var err error
	if file == nil {
		l.mu.Unlock()
		err = errors.New("wal: active file closed")
	} else {
		start := time.Now()
		// Claim fileOps before releasing mu so a later segment roll cannot
		// close this file before this detached buffer is written and synced.
		l.fileOps.Lock()
		l.mu.Unlock()
		err = l.writeAndSyncFileOps(file, buffer)
		l.fileOps.Unlock()
		l.observe("sync", observeOutcome(err), time.Since(start))
	}
	if err != nil {
		err = fmt.Errorf("wal: write and sync: %w", err)
		l.mu.Lock()
		l.syncErr = err
		pending := l.pending
		l.writeBuffer = nil
		l.pending = nil
		l.unsynced = 0
		l.mu.Unlock()
		completeBatch(pending, err)
	}
	completeBatch(batch, err)
}

func (l *Log) syncLocked() (*syncBatch, error) {
	if len(l.writeBuffer) == 0 {
		return nil, nil
	}
	buffer := l.writeBuffer
	l.writeBuffer = nil
	l.fileOps.Lock()
	err := l.writeAndSyncFileOps(l.file, buffer)
	l.fileOps.Unlock()
	batch := l.pending
	l.pending = nil
	if err != nil {
		l.syncErr = fmt.Errorf("wal: write and sync: %w", err)
		return batch, l.syncErr
	}
	l.unsynced = 0
	return batch, nil
}

// writeAndSyncFileOps writes buffer to file and fsyncs it. It must be called
// with fileOps held. A write or sync failure is latched in writeFailed inside
// the same fileOps critical section, so a writer that was waiting on fileOps
// (e.g. the roll path's syncLocked) cannot write and ack a later batch on top
// of a possibly torn region.
func (l *Log) writeAndSyncFileOps(file *os.File, buffer []byte) error {
	if l.writeFailed != nil {
		return l.writeFailed
	}
	err := writeFull(file, buffer)
	if err == nil {
		err = file.Sync()
	}
	if err != nil {
		l.writeFailed = err
	}
	return err
}

func completeBatch(batch *syncBatch, err error) {
	if batch == nil {
		return
	}
	batch.err = err
	close(batch.done)
}

func writeFull(file interface {
	Write([]byte) (int, error)
}, data []byte,
) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return fmt.Errorf("wal: write frame batch: %w", err)
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func resetTimer(timer *time.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(d)
}
