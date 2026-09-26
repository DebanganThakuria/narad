package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Segment preparation; see SegmentPrealloc for why.
//
// Once the active segment passes half of SegmentBytes, appendLocked asks
// the preparer goroutine for a successor. The preparer creates
// prepFileName, zero-fills it to SegmentBytes and marks it ready. The
// next roll on the append path renames it to the new segment's name
// instead of creating an empty file, so every append into it overwrites
// blocks that are already allocated and written, and the group commit's
// data sync no longer changes the file's size. A roll that finds no
// ready spare creates an empty segment exactly as before.
//
// Readers see a prepared segment as its frames followed by zeros. Replay
// stops at an all-zero frame header as at end of file, and the open log's
// replay never reads past the synced size anyway (Log.ReplayFromCursor).
// Open reads the zeros as a torn tail, exactly as binaries that predate
// preparation do, and truncates the active segment to its data as it
// always has, so a torn frame can never sit behind the next append. The
// roll that seals a prepared segment first trims it back to its data,
// so a sealed segment never ends in zeros: the unused space is freed and
// every segment stays readable by those older binaries.

// prepFileName is the name a segment is prepared under. It does not end
// in segmentSuffix, so listSegments (recovery, replay, compaction, and
// binaries that predate preparation) never sees a half-written file.
const prepFileName = "next-segment.prep"

// prepChunkBytes is the unit of zero-filling. Each chunk is data-synced
// before the next is written, so preparation never builds a dirty
// backlog that a journal commit elsewhere on the file system (the active
// segment's, a partition log's) would have to flush first.
const prepChunkBytes = 256 << 10

// prepZeros is the source of every zero-filling write.
var prepZeros [prepChunkBytes]byte

// errPrepStopped aborts a preparation because the log is closing.
var errPrepStopped = errors.New("wal: log closing")

// requestPrepLocked asks the preparer for the next segment once the
// active one is half full. Cheap enough for every append: one compare
// until the request is made, then nothing until the next roll. Caller
// must hold mu.
func (l *Log) requestPrepLocked() {
	if !l.prealloc || l.prepRequested || l.segmentSize < l.opts.SegmentBytes/2 {
		return
	}
	l.prepRequested = true
	l.wakePreparer()
}

// wakePreparer nudges the preparer without blocking; a wakeup already
// pending covers this one.
func (l *Log) wakePreparer() {
	select {
	case l.prepWake <- struct{}{}:
	default:
	}
}

// prepLoop is the preparer goroutine, running only while preparation is
// enabled. It prepares the next segment when an append asks for one.
func (l *Log) prepLoop() {
	defer close(l.prepDone)
	for {
		select {
		case <-l.stop:
			return
		case <-l.prepWake:
		}
		l.mu.Lock()
		prepare := l.prepRequested && !l.spareReady && !l.closed && l.syncErr == nil
		l.mu.Unlock()
		if prepare && l.prepareSpare() == nil {
			l.mu.Lock()
			l.spareReady = true
			l.mu.Unlock()
		}
	}
}

// prepareSpare creates prepFileName and zero-fills it to SegmentBytes.
// On any failure (a full disk, an I/O error, Close) the file is removed:
// a roll then creates an empty segment, as without preparation, and the
// next roll's successor is asked for again.
func (l *Log) prepareSpare() (err error) {
	path := filepath.Join(l.dir, prepFileName)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("wal: remove stale prepared segment: %w", err)
	}
	file, err := syncfile.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("wal: create prepared segment: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	for written := int64(0); written < l.opts.SegmentBytes; {
		select {
		case <-l.stop:
			return errPrepStopped
		default:
		}
		n := min(int64(prepChunkBytes), l.opts.SegmentBytes-written)
		if err := writeFull(file, prepZeros[:n]); err != nil {
			return err
		}
		if err := syncfile.SyncData(file); err != nil {
			return fmt.Errorf("wal: sync prepared segment: %w", err)
		}
		written += n
	}
	return nil
}

// createSegmentLocked creates the segment file at path for a roll. With
// usePrepared set and a spare ready, the spare is renamed into place and
// the returned prepared flag is true; otherwise it is a fresh empty file
// created with O_EXCL, as before preparation existed. Caller must hold mu
// and fileOps.
func (l *Log) createSegmentLocked(path string, usePrepared bool) (*os.File, bool, error) {
	if usePrepared && l.spareReady {
		// Consumed or abandoned either way: a failed rename leaves the
		// spare to be recreated by the next preparation.
		l.spareReady = false
		if l.renameSpareLocked(path) {
			file, err := syncfile.OpenFile(path, os.O_RDWR, 0o600)
			if err != nil {
				// Nothing left behind: the retry's O_EXCL create would
				// otherwise fail on this very file.
				_ = os.Remove(path)
				return nil, false, fmt.Errorf("wal: open prepared segment: %w", err)
			}
			return file, true, nil
		}
	}
	file, err := syncfile.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o600)
	if err != nil {
		return nil, false, fmt.Errorf("wal: create segment: %w", err)
	}
	return file, false, nil
}

// renameSpareLocked moves the prepared spare to path and reports whether
// it did. rename(2) silently replaces an existing target, so it only
// renames onto a free name; an occupied one is left for the O_EXCL
// create to refuse, as a roll always has. Caller must hold mu and
// fileOps.
func (l *Log) renameSpareLocked(path string) bool {
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return false
	}
	return syncfile.Rename(filepath.Join(l.dir, prepFileName), path) == nil
}
