package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const consumerOffsetFileName = "consumer.offset"

func ReadConsumerOffset(partitionDir string) (int64, bool, error) {
	buf, err := os.ReadFile(filepath.Join(partitionDir, consumerOffsetFileName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(buf) != 8 {
		return 0, false, fmt.Errorf("consumer offset file corrupt: got %d bytes, want 8", len(buf))
	}
	return int64(binary.BigEndian.Uint64(buf)), true, nil
}

func WriteConsumerOffset(partitionDir string, offset int64) error {
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(partitionDir, consumerOffsetFileName+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(offset))
	if _, err := tmp.Write(buf[:]); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(partitionDir, consumerOffsetFileName))
}
