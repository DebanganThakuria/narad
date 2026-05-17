package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

	persisted := int64(binary.BigEndian.Uint64(data))
	if persisted < 0 {
		persisted = 0
	}
	if persisted > nextOffset {
		persisted = nextOffset
	}
	l.highWatermark.Store(persisted)
	return nil
}

func (l *Log) bootstrapHighWatermark(nextOffset int64) {
	if nextOffset < 0 {
		nextOffset = 0
	}
	l.highWatermark.Store(nextOffset)
}

func (l *Log) persistHighWatermark(next int64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(next))
	if err := os.WriteFile(l.hwmTmpPath, buf, 0o644); err != nil {
		return fmt.Errorf("storage: write hwm temp: %w", err)
	}
	tmpFile, err := os.OpenFile(l.hwmTmpPath, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("storage: open hwm temp: %w", err)
	}
	defer tmpFile.Close()
	if err := tmpFile.Sync(); err != nil {
		return fmt.Errorf("storage: sync hwm temp: %w", err)
	}
	if err := os.Rename(l.hwmTmpPath, l.hwmPath); err != nil {
		return fmt.Errorf("storage: replace hwm: %w", err)
	}
	return nil
}
