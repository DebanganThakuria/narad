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

// loadHighWatermark restores the persisted HWM on recovery. The HWM is the
// durable VISIBILITY boundary and is deliberately allowed to lag the durable
// tail (nextOffset): records written+fsynced but whose commit did not advance
// the HWM are a "hidden tail" that must stay hidden across restart, because the
// WAL replays and re-commits them at fresh offsets — exposing the hidden copy
// would double-deliver. So recovery trusts the persisted file (clamped to the
// recovered tail), NOT nextOffset.
func (l *Log) loadHighWatermark(nextOffset int64) error {
	data, err := os.ReadFile(l.hwmPath)
	if errors.Is(err, os.ErrNotExist) {
		l.bootstrapHighWatermark(nextOffset)
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: read hwm: %w", err)
	}
	if len(data) == 0 {
		l.bootstrapHighWatermark(nextOffset)
		return nil
	}
	if len(data) != 8 {
		return fmt.Errorf("storage: invalid hwm file size %d", len(data))
	}

	persisted := min(max(int64(binary.BigEndian.Uint64(data)), 0), nextOffset)
	l.highWatermark.Store(persisted)
	l.persistedHWM.Store(persisted)
	return nil
}

func (l *Log) bootstrapHighWatermark(nextOffset int64) {
	if nextOffset < 0 {
		nextOffset = 0
	}
	l.highWatermark.Store(nextOffset)
	l.persistedHWM.Store(nextOffset)
}

// WritePersistedHighWatermark writes the durable high-watermark file
// for a partition directory. Used by a rebalance copy to reproduce the
// source's exact visibility boundary (which may lag the record tail —
// the hidden tail must stay hidden so a reopened copy does not
// double-expose records the WAL will re-commit). 8 bytes, big-endian.
func WritePersistedHighWatermark(dir string, hwm int64) error {
	if hwm < 0 {
		hwm = 0
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(hwm))
	return os.WriteFile(hwmFilePath(dir), buf[:], 0o644)
}

// ReadPersistedHighWatermark reads a partition directory's durable
// high-watermark file without opening the log. ok=false when the file
// (or the directory) does not exist or is empty — an empty partition.
// Close force-syncs the HWM file, so for a cleanly closed log this
// value is exact, not lagging; idle-evicted logs rely on that to
// answer "is there committed backlog?" without reopening.
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

// PersistedHighWatermark reads the HWM back from disk — the value a
// restart would recover — falling back to the in-memory HWM when no
// file has been written yet. Used to verify durability, not on any hot
// path.
func (l *Log) PersistedHighWatermark() (int64, error) {
	data, err := os.ReadFile(l.hwmPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return l.HighWatermark(), nil
		}
		return 0, fmt.Errorf("storage: read persisted hwm: %w", err)
	}
	if len(data) == 0 {
		return l.HighWatermark(), nil
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("storage: invalid hwm file size %d", len(data))
	}

	persisted := max(int64(binary.BigEndian.Uint64(data)), 0)
	return persisted, nil
}

// persistHighWatermark durably writes the 8-byte HWM in place.
//
// The HWM is persisted on every commit (durability of the visible boundary is
// required — a record once exposed must stay exposed across a crash). The cost
// must therefore be minimal. The previous temp-file + fsync + rename made a new
// inode and a directory mutation on EVERY commit across every partition; under
// load that metadata churn — not the value write — was the dominant disk cost
// (≈50ms p95). An 8-byte value fits in a single sector, and a single-sector
// write is atomic across a crash (the reader sees the old or new 8 bytes, never
// a torn mix), so the temp+rename dance — which exists only to make
// variable-length writes atomic — is unnecessary. We overwrite the fixed-size
// file in place and fsync, eliminating the inode/dir churn.
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
		f, err := os.OpenFile(l.hwmPath, os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			return fmt.Errorf("storage: open hwm: %w", err)
		}
		l.hwmFile = f
	}
	if _, err := l.hwmFile.WriteAt(buf[:], 0); err != nil {
		l.closeHWMFileLocked()
		return fmt.Errorf("storage: write hwm: %w", err)
	}
	// Data-only sync: an in-place single-sector overwrite has no
	// metadata worth journaling (first-creation durability is the dir
	// fsync below), and this runs once per commit: the hottest of the
	// small-file syncs.
	if err := syncfile.SyncData(l.hwmFile); err != nil {
		l.closeHWMFileLocked()
		return fmt.Errorf("storage: sync hwm: %w", err)
	}
	// The open above may have CREATED the file, and file creation is only
	// durable once the parent directory is fsynced. Without it a crash
	// can lose the file entirely; recovery would then bootstrap the HWM
	// from the tail and expose the hidden tail (double-delivery). One dir
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
	syncErr := d.Sync()
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
	if !force && time.Since(l.lastHWMSync) < l.opts.HWMSyncInterval {
		return nil
	}

	start := time.Now()
	outcome := "ok"
	if err := l.persistHighWatermark(target); err != nil {
		outcome = "error"
		l.observeHighWatermarkPersist(time.Since(start), outcome)
		return err
	}
	l.persistedHWM.Store(target)
	l.lastHWMSync = time.Now()
	l.observeHighWatermarkPersist(time.Since(start), outcome)
	return nil
}

// persistHighWatermarkAtLeast durably writes target (or the current
// in-memory high-watermark if that is higher) unconditionally, unless
// the persisted value already covers it. The commit path calls it BEFORE
// advancing the in-memory high-watermark, so a record is never visible
// with an unpersisted boundary. The following AdvanceHighWatermark then
// finds persistedHWM already at target and the pass-ending
// syncHighWatermark is a no-op.
func (l *Log) persistHighWatermarkAtLeast(target int64) error {
	if cur := l.highWatermark.Load(); cur > target {
		target = cur
	}
	if target < 0 || target <= l.persistedHWM.Load() {
		return nil
	}

	l.hwmMu.Lock()
	defer l.hwmMu.Unlock()

	if target <= l.persistedHWM.Load() {
		return nil
	}
	start := time.Now()
	outcome := "ok"
	if err := l.persistHighWatermark(target); err != nil {
		outcome = "error"
		l.observeHighWatermarkPersist(time.Since(start), outcome)
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
