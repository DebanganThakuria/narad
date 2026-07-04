package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
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
func (l *Log) persistHighWatermark(next int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(next))

	f, err := os.OpenFile(l.hwmPath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("storage: open hwm: %w", err)
	}
	if _, err := f.WriteAt(buf[:], 0); err != nil {
		_ = f.Close()
		return fmt.Errorf("storage: write hwm: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("storage: sync hwm: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
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

func (l *Log) observeHighWatermarkPersist(duration time.Duration, outcome string) {
	if m := l.opts.Metrics; m != nil {
		m.ObserveHighWatermarkPersist(duration, outcome)
	}
}
