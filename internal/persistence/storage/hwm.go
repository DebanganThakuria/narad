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

// loadHighWatermark sets the recovered high-watermark to the recovered
// record tail (nextOffset): every CRC-valid frame the scan found is
// visible. It never hides a record that was visible before the restart,
// because a commit fsyncs its frames before it advances the boundary, so
// no committed frame is past the tail. That is what lets a commit skip a
// second fsync for the boundary file (see CommitDurable).
//
// Records written and fsynced by a commit that never returned (a crash
// mid-commit, a failed commit whose truncate failed, a poisoned log) are
// exposed by this too: the ingress WAL still owns them and re-commits
// them at fresh offsets, so they can be delivered twice, never lost.
// The file used to keep such a tail hidden across a restart, but only
// until the next commit on the partition (the WAL's re-commit itself)
// advanced the boundary over it, so it only changed when the
// duplicates appeared.
//
// The file is still kept for readers of a closed log (see
// ReadPersistedHighWatermark). Close writes it exactly; after a crash it
// lags the recovered boundary by up to HWMSyncInterval, so when it
// differs it is rewritten here, before anything can read the log, which
// bounds the lag by the reopen. A failed rewrite does not fail the open:
// the in-memory boundary is right, and the flusher retries the write.
func (l *Log) loadHighWatermark(nextOffset int64) error {
	persisted, ok, err := ReadPersistedHighWatermark(l.dir)
	if err != nil {
		return err
	}
	nextOffset = max(nextOffset, 0)
	l.highWatermark.Store(nextOffset)
	if (ok && persisted == nextOffset) || (!ok && nextOffset == 0) {
		// Up to date, or an empty partition that needs no file yet.
		l.persistedHWM.Store(nextOffset)
		return nil
	}

	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()
	start := time.Now()
	if err := l.persistHighWatermark(nextOffset); err != nil {
		l.observeHighWatermarkPersist(time.Since(start), "error")
		l.logger.Warn("storage: could not persist the recovered high-watermark; readers of the closed log see the old one until a later persist",
			"dir", l.dir, "high_watermark", nextOffset, "err", err)
		// Unknown file content: any boundary is ahead of it, so the
		// flusher's timer and Close both retry the write.
		l.persistedHWM.Store(-1)
		return nil
	}
	l.observeHighWatermarkPersist(time.Since(start), "ok")
	l.persistedHWM.Store(nextOffset)
	l.lastHWMSync = time.Now()
	return nil
}

// WritePersistedHighWatermark writes the durable high-watermark file
// for a partition directory: the boundary readers of the closed
// directory see. Used by a rebalance copy to carry the source's
// boundary. Opening the directory as a Log recovers the boundary from
// the record tail instead (see loadHighWatermark) and rewrites the file
// to match, so records the copy holds past hwm become visible then.
// 8 bytes, big-endian.
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
// (or the directory) does not exist or is empty — an empty partition.
// Close force-syncs the HWM file, so for a cleanly closed log this
// value is exact, not lagging; idle-evicted logs rely on that to
// answer "is there committed backlog?" without reopening. After a crash
// it can lag the log's visible records by up to HWMSyncInterval until
// the log is opened again, which rewrites it (see loadHighWatermark).
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
// at least: the durable record tail (recovery rebuilds the boundary from
// the CRC-verified tail, see loadHighWatermark), or the persisted file
// when that is higher. The file is read from disk, so a broken one
// reports its error. Used to verify durability, not on any hot path.
func (l *Log) PersistedHighWatermark() (int64, error) {
	data, err := os.ReadFile(l.hwmPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("storage: read persisted hwm: %w", err)
	}
	var persisted int64
	switch len(data) {
	case 0:
	case 8:
		persisted = max(int64(binary.BigEndian.Uint64(data)), 0)
	default:
		return 0, fmt.Errorf("storage: invalid hwm file size %d", len(data))
	}
	return max(persisted, l.durableTail.Load()), nil
}

// persistHighWatermark durably writes the 8-byte HWM in place.
//
// It runs every HWMSyncInterval while the boundary moves, on the forced
// passes (Sync, rotation, the shutdown drain), at Close, and at open when
// recovery moved the boundary; not per commit (see loadHighWatermark for
// why recovery does not need it). An 8-byte value fits in a single
// sector, and a single-sector write is atomic across a crash (the reader
// sees the old or new 8 bytes, never a torn mix), so there is no temp
// file and rename, which exists only to make variable-length writes
// atomic and cost a new inode and a directory mutation per persist. The
// fixed-size file is overwritten in place and fsynced.
//
// The file descriptor is opened on the first persist and kept open for
// the life of the Log (closed by Close): the open/close pair per commit
// was two syscalls of pure overhead on the hottest small-file sync. The
// file is never replaced by rename while a Log is open (the staging-dir
// writer runs before NewLog; delete paths close the Log first), so a
// held descriptor cannot write into an unlinked inode.
//
// Caller must hold hwmMu.
func (l *Log) persistHighWatermark(next int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(next))

	if l.hwmFile == nil {
		f, err := syncfile.OpenFile(l.hwmPath, os.O_WRONLY|os.O_CREATE, dataFileMode)
		if err != nil {
			return fmt.Errorf("storage: open hwm: %w", err)
		}
		l.hwmFile = f
	}
	if _, err := syncfile.WriteAt(l.hwmFile, buf[:], 0); err != nil {
		l.closeHWMFileLocked()
		return fmt.Errorf("storage: write hwm: %w", err)
	}
	// Data-only sync: an in-place single-sector overwrite has no
	// metadata worth journaling (first-creation durability is the dir
	// fsync below).
	if err := syncfile.SyncData(l.hwmFile); err != nil {
		l.closeHWMFileLocked()
		return fmt.Errorf("storage: sync hwm: %w", err)
	}
	// The open above may have CREATED the file, and file creation is only
	// durable once the parent directory is fsynced. Without it a crash
	// can lose the file entirely, and readers of the closed log would
	// take the partition for empty until it is opened again. One dir
	// fsync per Log lifetime covers the first-creation case cheaply.
	if !l.hwmDirSynced {
		if err := syncDir(l.dir); err != nil {
			return fmt.Errorf("storage: sync partition dir for hwm: %w", err)
		}
		l.hwmDirSynced = true
	}
	return nil
}

// closeHWMFileLocked releases the held hwm descriptor (if any). A later
// persist reopens it. Caller must hold hwmMu.
func (l *Log) closeHWMFileLocked() error {
	if l.hwmFile == nil {
		return nil
	}
	err := l.hwmFile.Close()
	l.hwmFile = nil
	return err
}

// closeHWMFile releases the held hwm descriptor under hwmMu.
func (l *Log) closeHWMFile() error {
	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()
	return l.closeHWMFileLocked()
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

func (l *Log) syncHighWatermark(force bool) error {
	target := l.highWatermark.Load()
	if target < 0 || target <= l.persistedHWM.Load() {
		return nil
	}

	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()

	target = l.highWatermark.Load()
	if target < 0 || target <= l.persistedHWM.Load() {
		return nil
	}
	// Deferring is only safe because flusher.needsTimer keeps the timer
	// armed while highWatermark > persistedHWM (for hwmPersistDeadline,
	// when this stops deferring). A new deferral here needs a matching
	// condition there, or the work is silently never scheduled once the
	// log goes idle.
	if !force && time.Since(l.lastHWMSync) < l.opts.HWMSyncInterval {
		return nil
	}

	start := time.Now()
	outcome := "ok"
	if err := l.persistHighWatermark(target); err != nil {
		outcome = "error"
		l.observeHighWatermarkPersist(time.Since(start), outcome)
		// Retried on the same cadence as a deferred persist, not on
		// every flusher pass while the file stays broken.
		l.lastHWMSync = time.Now()
		return err
	}
	l.persistedHWM.Store(target)
	l.lastHWMSync = time.Now()
	l.observeHighWatermarkPersist(time.Since(start), outcome)
	return nil
}

func (l *Log) observeHighWatermarkPersist(duration time.Duration, outcome string) {
	if m := l.opts.Metrics; m != nil {
		m.ObserveHighWatermarkPersist(duration, outcome)
	}
}
