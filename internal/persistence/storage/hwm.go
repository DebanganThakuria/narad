package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

const hwmFileName = "hwm"

func hwmFilePath(dir string) string {
	return filepath.Join(dir, hwmFileName)
}

// The hwm file is the visibility boundary of a CLOSED log. Close writes
// the exact high-watermark there, so readers that answer without opening
// the log (ReadPersistedHighWatermark) see every visible record, and the
// next open keeps a hidden tail hidden (loadHighWatermark).
//
// An open log owes the file nothing per commit. Before its first advance
// it empties the file (releaseHighWatermarkFile), and an empty or missing
// file tells recovery to take the boundary from the CRC-verified record
// tail. That tail never hides a visible record, because a commit fsyncs
// its frames before it advances the boundary. So a crash leaves an empty
// file and every committed record visible, and the commit path skips the
// second serial fsync (the boundary file after the segment) it used to
// pay on every commit under the produce lock.
//
// Emptying the file rather than letting it lag is what keeps the format
// safe for every earlier version: they persisted the file on every
// commit, recover a file of 8 bytes as min(file, tail) and an empty one
// as the tail. A file that lagged the acked commits would make an older
// binary, started after a crash (a rollback), hide acked records, and
// its failed-commit discard would then truncate them.
//
// The tail an open recovers is read through the page cache, so after a
// process crash it can hold frames the dead process wrote but never saw
// fsynced (it died inside a commit's fsync, or before it). Exposing
// those as they are would serve records a later power loss can take
// back, after consumers acked them and the offset files recorded that:
// the offsets would then be reused by other records. So before an open
// takes the boundary from the tail it fsyncs the active segment
// (syncRecoveredTail), and everything it exposes is durable. Sealed
// segments need no sync: a roll fsyncs the active segment before it
// creates the next one.
//
// The cost of a crash is then duplicates, never loss: records written
// by a commit that never returned (a crash mid-commit, a failed commit
// whose truncate failed, a poisoned log) are exposed by the tail too,
// and the ingress WAL, which still owns them, re-commits them at fresh
// offsets. The next commit on the partition exposed such a tail anyway,
// since it fsyncs the segment and advances the boundary over it.

// loadHighWatermark restores the boundary on open. A file of 8 bytes is
// the boundary a clean Close wrote (or a rebalance copy carried, see
// WritePersistedHighWatermark), clamped to the recovered tail; an empty
// or missing file means the tail, which is fsynced first (see the file
// comment above).
//
// The hwm file is not written here. It is only read while the log is
// closed, the first advance empties it, and Close writes it again.
func (l *Log) loadHighWatermark(nextOffset int64) error {
	nextOffset = max(nextOffset, 0)
	data, err := os.ReadFile(l.hwmPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("storage: read hwm: %w", err)
	}
	// A file that exists (empty or not) has a durable name, or at least
	// one a previous process already relied on; Close fsyncs the
	// directory only for a file this Log creates.
	l.hwmDirSynced = err == nil
	switch len(data) {
	case 0:
		if nextOffset > 0 {
			if err := l.syncRecoveredTail(); err != nil {
				return err
			}
		}
		l.highWatermark.Store(nextOffset)
		if nextOffset == 0 {
			// An empty partition needs no file: missing already means 0.
			l.persistedHWM.Store(0)
		} else {
			// Records but no boundary on disk (a crash): Close writes the
			// exact one even if nothing is committed before it.
			l.persistedHWM.Store(-1)
		}
	case 8:
		persisted := max(int64(binary.BigEndian.Uint64(data)), 0)
		l.highWatermark.Store(min(persisted, nextOffset))
		l.persistedHWM.Store(persisted)
	default:
		return fmt.Errorf("storage: invalid hwm file size %d", len(data))
	}
	return nil
}

// syncRecoveredTail fsyncs the recovered active segment, so a boundary
// taken from its tail covers only durable frames. A failure fails the
// open: the bytes past the last good sync are of unknown durability (see
// flusher.syncIfNeeded), and the next open retries. It costs one fsync
// per open that takes the boundary from the tail (after a crash, or for
// a log that was never closed), never one on the commit path.
func (l *Log) syncRecoveredTail() error {
	active := l.segments[len(l.segments)-1]
	if err := active.sync(); err != nil {
		return fmt.Errorf("storage: sync recovered tail: %w", err)
	}
	return nil
}

// releaseHighWatermarkFile empties the hwm file before the first
// advance of this Log's life, so no crash from here on can recover a
// boundary below a record the advance exposes (see the file comment).
// Once per Log: a commit pays nothing for the file after the first.
//
// It runs even when the file is missing or already empty, which costs
// one open and one sync on the first commit only, so an unusable hwm
// path fails that commit (the ingress WAL retries it) instead of
// surfacing only at Close, where the closed log's boundary would go
// missing.
//
// A failure fails the advance, and the caller's commit with it: the
// records stay hidden, the flusher discards them, and the next commit
// retries the release.
func (l *Log) releaseHighWatermarkFile() error {
	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()
	if l.hwmReleased.Load() {
		return nil
	}
	start := time.Now()
	f, err := syncfile.OpenFile(l.hwmPath, os.O_WRONLY|os.O_CREATE, dataFileMode)
	if err != nil {
		l.observeHighWatermarkPersist(time.Since(start), "error")
		return fmt.Errorf("storage: release hwm: %w", err)
	}
	// From here the file may no longer hold the boundary it had, so
	// Close must write the exact one whether or not this succeeds.
	l.persistedHWM.Store(-1)
	err = syncfile.Truncate(f, 0)
	if err == nil {
		err = syncfile.Sync(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		l.observeHighWatermarkPersist(time.Since(start), "error")
		return fmt.Errorf("storage: release hwm: %w", err)
	}
	l.observeHighWatermarkPersist(time.Since(start), "ok")
	l.hwmReleased.Store(true)
	return nil
}

// persistClosedHighWatermark writes the exact high-watermark for readers
// of the closed log and for the next open, unless the file already holds
// it. Close calls it once the flusher has stopped, so the boundary no
// longer moves; it also runs for a poisoned log, whose boundary only
// ever covered fsynced records.
func (l *Log) persistClosedHighWatermark() error {
	hwm := l.highWatermark.Load()
	if hwm < 0 || hwm == l.persistedHWM.Load() {
		return nil
	}
	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()
	start := time.Now()
	if err := l.persistHighWatermark(hwm); err != nil {
		l.observeHighWatermarkPersist(time.Since(start), "error")
		return err
	}
	l.observeHighWatermarkPersist(time.Since(start), "ok")
	l.persistedHWM.Store(hwm)
	return nil
}

// persistHighWatermark durably writes the 8-byte HWM in place: an
// 8-byte value fits in a single sector, and a single-sector write is
// atomic across a crash (the reader sees the old or new 8 bytes, never
// a torn mix), so there is no temp file and rename. A crash before the
// sync leaves the file empty (released) or holding its previous
// boundary, either of which recovers every record the log exposed.
//
// Caller must hold hwmMu.
func (l *Log) persistHighWatermark(next int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(next))

	f, err := syncfile.OpenFile(l.hwmPath, os.O_WRONLY|os.O_CREATE, dataFileMode)
	if err != nil {
		return fmt.Errorf("storage: open hwm: %w", err)
	}
	if _, err = syncfile.WriteAt(f, buf[:], 0); err != nil {
		err = fmt.Errorf("storage: write hwm: %w", err)
	} else if err = syncfile.SyncData(f); err != nil {
		err = fmt.Errorf("storage: sync hwm: %w", err)
	}
	if cerr := f.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("storage: close hwm: %w", cerr)
	}
	if err != nil {
		return err
	}
	// The open above may have CREATED the file, and file creation is only
	// durable once the parent directory is fsynced. Without it a crash can
	// lose the file, and the next open would take the boundary from the
	// tail and expose the hidden tail (duplicates). Once per Log lifetime
	// at most, and only for a file this Log created.
	if !l.hwmDirSynced {
		if err := syncDir(l.dir); err != nil {
			return fmt.Errorf("storage: sync partition dir for hwm: %w", err)
		}
		l.hwmDirSynced = true
	}
	return nil
}

// WritePersistedHighWatermark writes the durable high-watermark file
// for a partition directory. Used by a rebalance copy to reproduce the
// source's exact visibility boundary (which may lag the record tail;
// the hidden tail must stay hidden so a reopened copy does not
// double-expose records the WAL will re-commit). 8 bytes, big-endian.
func WritePersistedHighWatermark(dir string, hwm int64) error {
	if hwm < 0 {
		hwm = 0
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(hwm))
	return os.WriteFile(hwmFilePath(dir), buf[:], dataFileMode)
}

// ReadPersistedHighWatermark reads a partition directory's durable
// high-watermark file without opening the log. ok=false when the file
// (or the directory) does not exist or is empty. Close writes the file
// exactly, so for a cleanly closed log this value is exact; idle-evicted
// logs rely on that to answer "is there committed backlog?" without
// reopening. While a log that exposed records is open, and after a crash
// until the log is opened and closed again, the file is empty (see
// releaseHighWatermarkFile) and this reports ok=false: an open log
// answers from memory, and startup opens every owned partition before
// the node reports ready.
func ReadPersistedHighWatermark(dir string) (int64, bool, error) {
	data, err := os.ReadFile(hwmFilePath(dir))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("storage: read persisted hwm: %w", err)
	}
	if len(data) == 0 {
		return 0, false, nil
	}
	if len(data) != 8 {
		return 0, false, fmt.Errorf("storage: invalid hwm file size %d", len(data))
	}
	return max(int64(binary.BigEndian.Uint64(data)), 0), true, nil
}

// PersistedHighWatermark is the high-watermark a restart would recover
// from the files as they are now: the file's boundary clamped to the
// durable record tail, or that tail when the file holds none (see
// loadHighWatermark). The file is read from disk, so a broken one
// reports its error. Used to verify durability, not on any hot path.
func (l *Log) PersistedHighWatermark() (int64, error) {
	data, err := os.ReadFile(l.hwmPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("storage: read persisted hwm: %w", err)
	}
	tail := l.durableTail.Load()
	switch len(data) {
	case 0:
		return tail, nil
	case 8:
		return min(max(int64(binary.BigEndian.Uint64(data)), 0), tail), nil
	default:
		return 0, fmt.Errorf("storage: invalid hwm file size %d", len(data))
	}
}

// syncDir fsyncs a directory so entries created in it are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := syncfile.Sync(d)
	closeErr := d.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (l *Log) observeHighWatermarkPersist(duration time.Duration, outcome string) {
	if m := l.opts.Metrics; m != nil {
		m.ObserveHighWatermarkPersist(duration, outcome)
	}
}
