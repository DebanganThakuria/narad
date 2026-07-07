package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const consumerOffsetFileName = "consumer.offset"

// ErrPartitionDirMissing reports that a consumer offset write was
// refused because the partition directory no longer exists (e.g. the
// topic was deleted concurrently).
var ErrPartitionDirMissing = errors.New("storage: partition directory missing")

// ReadConsumerOffset returns the committed consumer offset persisted in
// partitionDir. ok=false (with a nil error) when no offset has been
// committed yet.
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

// WriteConsumerOffset durably persists the committed consumer offset,
// creating the partition directory if needed. The write is atomic
// (temp file + fsync + rename), so a crash leaves either the old or the
// new offset, never a torn value.
func WriteConsumerOffset(partitionDir string, offset int64) error {
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		return err
	}
	return writeOffsetFile(partitionDir, consumerOffsetFileName, offset)
}

// WriteConsumerOffsetIfPartitionDirExists is WriteConsumerOffset except
// it fails with ErrPartitionDirMissing instead of recreating a deleted
// partition directory — the guard against resurrecting a topic that was
// removed while a commit was in flight.
func WriteConsumerOffsetIfPartitionDirExists(partitionDir string, offset int64) error {
	info, err := os.Stat(partitionDir)
	if errors.Is(err, os.ErrNotExist) {
		return ErrPartitionDirMissing
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("consumer offset partition path is not a directory: %s", partitionDir)
	}
	return writeOffsetFile(partitionDir, consumerOffsetFileName, offset)
}

// writeOffsetFile atomically persists an 8-byte big-endian offset file
// under dir (temp file + fsync + rename). Shared by the consumer
// offset, the keyed.from marker, and the fan-out cursor files.
func writeOffsetFile(dir, name string, offset int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(offset))
	return writeFileAtomic(dir, name, buf[:])
}

// writeFileAtomic atomically persists content at dir/name (temp file +
// fsync + rename). A missing dir maps to ErrPartitionDirMissing so
// callers never resurrect a concurrently deleted topic.
func writeFileAtomic(dir, name string, content []byte) error {
	tmp, err := os.CreateTemp(dir, name+".*")
	if errors.Is(err, os.ErrNotExist) {
		return ErrPartitionDirMissing
	}
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	if _, err := tmp.Write(content); err != nil {
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
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrPartitionDirMissing
		}
		return err
	}
	return nil
}
