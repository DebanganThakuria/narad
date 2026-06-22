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

func hwmTempFilePath(dir string) string {
	return filepath.Join(dir, hwmFileName+".tmp")
}

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

func (l *Log) persistHighWatermark(next int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(next))

	tmpFile, err := os.OpenFile(l.hwmTmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("storage: open hwm temp: %w", err)
	}
	if _, err := tmpFile.Write(buf); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("storage: write hwm temp: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("storage: sync hwm temp: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("storage: close hwm temp: %w", err)
	}
	if err := os.Rename(l.hwmTmpPath, l.hwmPath); err != nil {
		return fmt.Errorf("storage: replace hwm: %w", err)
	}
	return nil
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
