package ingress

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const produceCheckpointFile = "checkpoint"

func loadCheckpoint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("ingress: read checkpoint: %w", err)
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("ingress: invalid checkpoint size %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

func storeCheckpoint(dir, name string, nextSeq uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], nextSeq)

	tmpPath := filepath.Join(dir, name+".tmp")
	checkpointPath := filepath.Join(dir, name)
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("ingress: open checkpoint temp: %w", err)
	}
	if _, err := file.Write(buf[:]); err != nil {
		_ = file.Close()
		return fmt.Errorf("ingress: write checkpoint temp: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("ingress: sync checkpoint temp: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("ingress: close checkpoint temp: %w", err)
	}
	if err := os.Rename(tmpPath, checkpointPath); err != nil {
		return fmt.Errorf("ingress: replace checkpoint: %w", err)
	}
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}
