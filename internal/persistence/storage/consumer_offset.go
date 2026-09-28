package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

const consumerOffsetFileName = "consumer.offset"

// The consumer offset committer (broker/runtime) keeps a partition's
// consumer state files open between writes and batches their syncs, so
// it writes them itself. These are the names, sizes, mode and codecs it
// needs; the formats stay defined in this package.
const (
	// ConsumerOffsetFileName is the 8-byte frontier file.
	ConsumerOffsetFileName = consumerOffsetFileName
	// ConsumerAheadFileName is the two-slot acked-ahead file.
	ConsumerAheadFileName = consumerAheadFileName
	// ConsumerAheadSlotSize is one consumer.ahead slot; the file holds
	// two, slot 1 right after slot 0.
	ConsumerAheadSlotSize = consumerAheadSlotSize
	// ConsumerStateFileMode is the mode both files are created with.
	ConsumerStateFileMode = dataFileMode
)

// EncodeConsumerOffset is consumer.offset's content for offset: the
// frontier as 8 big-endian bytes.
func EncodeConsumerOffset(offset int64) [8]byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(offset))
	return buf
}

// DecodeConsumerAheadSlot validates one consumer.ahead slot exactly as
// ReadConsumerAhead validates each slot it reads. ok=false for anything
// that is not a complete, checksummed record of a known version. The
// record's Slot is left zero: the caller knows which slot it read.
func DecodeConsumerAheadSlot(slot []byte) (ConsumerAhead, bool) {
	return decodeConsumerAheadSlot(slot)
}

// ErrPartitionDirMissing reports that a consumer offset write was
// refused because the partition directory no longer exists (e.g. the
// topic was deleted concurrently).
var ErrPartitionDirMissing = errors.New("storage: partition directory missing")

// ReadConsumerOffset returns the committed consumer offset persisted in
// partitionDir. ok=false (with a nil error) when no offset has been
// committed yet. An empty file also reads as "none": the in-place
// writer creates the file and fills it in two steps, so a crash between
// them leaves zero bytes, which means no offset was ever persisted.
func ReadConsumerOffset(partitionDir string) (int64, bool, error) {
	buf, err := os.ReadFile(filepath.Join(partitionDir, consumerOffsetFileName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(buf) == 0 {
		return 0, false, nil
	}
	if len(buf) != 8 {
		return 0, false, fmt.Errorf("consumer offset file corrupt: got %d bytes, want 8", len(buf))
	}
	return int64(binary.BigEndian.Uint64(buf)), true, nil
}

// WriteConsumerOffset durably persists the committed consumer offset,
// creating the partition directory if needed.
func WriteConsumerOffset(partitionDir string, offset int64) error {
	if err := os.MkdirAll(partitionDir, dataDirMode); err != nil {
		return err
	}
	return writeOffsetFileInPlace(partitionDir, consumerOffsetFileName, offset)
}

// WriteConsumerOffsetIfPartitionDirExists is WriteConsumerOffset except
// it fails with ErrPartitionDirMissing instead of recreating a deleted
// partition directory: the guard against resurrecting a topic that was
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
	return writeOffsetFileInPlace(partitionDir, consumerOffsetFileName, offset)
}

// writeOffsetFileInPlace durably persists an 8-byte big-endian offset
// file under dir by overwriting it in place.
//
// The previous temp-file + fsync + rename cost seven syscalls, a new
// inode, and a directory mutation per active partition every commit
// interval; under acks on a hundred partitions that was a thousand
// metadata-heavy fsyncs a second competing with produce. An 8-byte
// value fits in one sector and a single-sector overwrite is atomic
// across a crash (the reader sees the old or the new 8 bytes), which is
// exactly the argument the high-watermark file already relies on. The
// first creation additionally fsyncs the directory so the new name is
// durable; a crash between create and write leaves an empty file, which
// ReadConsumerOffset reads as "no offset".
func writeOffsetFileInPlace(dir, name string, offset int64) error {
	buf := EncodeConsumerOffset(offset)
	path := filepath.Join(dir, name)

	created := false
	f, err := syncfile.OpenFile(path, os.O_WRONLY, dataFileMode)
	if errors.Is(err, os.ErrNotExist) {
		f, err = syncfile.OpenFile(path, os.O_WRONLY|os.O_CREATE, dataFileMode)
		created = true
	}
	if errors.Is(err, os.ErrNotExist) {
		// The directory itself is gone.
		return ErrPartitionDirMissing
	}
	if err != nil {
		return err
	}
	if _, err := syncfile.WriteAt(f, buf[:], 0); err != nil {
		_ = f.Close()
		return err
	}
	if err := syncfile.SyncData(f); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if created {
		if err := syncDir(dir); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrPartitionDirMissing
			}
			return err
		}
	}
	return nil
}

// writeFileAtomic atomically persists content at dir/name (temp file +
// fsync + rename). A missing dir maps to ErrPartitionDirMissing so
// callers never resurrect a concurrently deleted topic. Used for
// variable-length files (fan-out cursors), where in-place overwrite is
// not atomic.
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

	if _, err := syncfile.Write(tmp, content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := syncfile.SyncData(tmp); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := syncfile.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrPartitionDirMissing
		}
		return err
	}
	return nil
}
