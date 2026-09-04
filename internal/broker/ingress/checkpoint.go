package ingress

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

const produceCheckpointFile = "checkpoint"

// loadCheckpoint reads a fixed 8-byte big-endian checkpoint. A missing
// or empty file reads as 0 (nothing dispatched yet: the in-place writer
// creates the file before its first write, so a crash between the two
// leaves an empty file, which is the same state); any other size is
// corrupt.
func loadCheckpoint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("ingress: read checkpoint: %w", err)
	}
	if len(data) == 0 {
		return 0, nil
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("ingress: invalid checkpoint size %d", len(data))
	}
	return binary.BigEndian.Uint64(data), nil
}

// checkpointWriter persists the dispatch checkpoint by overwriting a
// fixed 8-byte file in place through a descriptor it keeps open.
//
// The previous write-to-temp, fdatasync, rename, directory-fsync
// sequence ran on every progressing dispatch pass (every ~10 ms under
// load) on the same disk the ingress WAL group commit waits on: two
// flushes and a directory mutation to move 8 bytes. An 8-byte value fits
// in one sector and a single-sector overwrite is atomic across a crash
// (the reader sees the old or the new value), the same argument the
// partition high-watermark file relies on. So: one WriteAt, one
// fdatasync. The directory is fsynced once, when the file is first
// created, so the name is durable.
type checkpointWriter struct {
	path      string
	file      *os.File
	dirSynced bool
}

func newCheckpointWriter(dir, name string) *checkpointWriter {
	return &checkpointWriter{path: filepath.Join(dir, name)}
}

// store durably writes nextSeq. Not safe for concurrent use; the
// dispatcher is the single writer and Close runs after it stops.
func (w *checkpointWriter) store(nextSeq uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], nextSeq)

	if w.file == nil {
		created := false
		f, err := os.OpenFile(w.path, os.O_WRONLY, 0o644)
		if errors.Is(err, os.ErrNotExist) {
			f, err = os.OpenFile(w.path, os.O_WRONLY|os.O_CREATE, 0o644)
			created = true
		}
		if err != nil {
			return fmt.Errorf("ingress: open checkpoint: %w", err)
		}
		w.file = f
		w.dirSynced = !created
	}
	if _, err := w.file.WriteAt(buf[:], 0); err != nil {
		w.reset()
		return fmt.Errorf("ingress: write checkpoint: %w", err)
	}
	if err := syncfile.SyncData(w.file); err != nil {
		w.reset()
		return fmt.Errorf("ingress: sync checkpoint: %w", err)
	}
	if !w.dirSynced {
		dirFile, err := os.Open(filepath.Dir(w.path))
		if err != nil {
			return fmt.Errorf("ingress: open checkpoint dir: %w", err)
		}
		syncErr := dirFile.Sync()
		_ = dirFile.Close()
		if syncErr != nil {
			return fmt.Errorf("ingress: sync checkpoint dir: %w", syncErr)
		}
		w.dirSynced = true
	}
	return nil
}

// reset drops the held descriptor after a failure so the next store
// reopens the file.
func (w *checkpointWriter) reset() {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
}

func (w *checkpointWriter) close() error {
	if w == nil || w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}
