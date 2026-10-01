package ingress

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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

// checkpointSyncDelay is how long a stored checkpoint may sit written
// but not yet fdatasynced. The value only bounds crash-replay
// duplicates (every seq below it is committed to its partition), so
// deferring the flush costs at most this much re-dispatch after an OS
// crash or power loss, never a record. The WAL is compacted only behind
// the synced value (see durable), so compaction trails a store by at
// most one flush.
const checkpointSyncDelay = 250 * time.Millisecond

// checkpointWriter persists the dispatch checkpoint by overwriting a
// fixed 8-byte file in place through a descriptor it keeps open.
//
// The previous write-to-temp, fdatasync, rename, directory-fsync
// sequence ran on every progressing dispatch pass (every ~10 ms under
// load) on the same disk the ingress WAL group commit waits on: two
// flushes and a directory mutation to move 8 bytes. An 8-byte value fits
// in one sector and a single-sector overwrite is atomic across a crash
// (the reader sees the old or the new value), the same argument the
// partition high-watermark file relies on. So: one WriteAt per store.
// The directory is fsynced once, when the file is first created, so the
// name is durable.
//
// The fdatasync is off the dispatcher's path: a store writes the value
// (a process crash keeps it in the page cache) and a background flush
// makes it durable within checkpointSyncDelay, one flush for however
// many stores landed meanwhile. On macOS every flush is a device-wide
// F_FULLFSYNC that the WAL group commit also waits behind. close syncs
// whatever is still pending.
type checkpointWriter struct {
	path  string
	delay time.Duration

	mu        sync.Mutex
	idle      sync.Cond // signalled when a background sync finishes
	file      *os.File
	dirSynced bool
	// dirty is set by a store and cleared when a sync of the file
	// starts; a store landing during that sync sets it again.
	dirty bool
	// flushArmed is true while a background flush is scheduled.
	flushArmed bool
	// syncing is true while a background flush runs without mu, so
	// reset and close wait for it before closing the descriptor.
	syncing bool
	// syncErr is a failed background flush, reported by the next store.
	syncErr error
	// written is the last value written to the file; synced is the
	// highest value a successful fdatasync covered. Compaction must not
	// pass synced: after a power loss the file can come back holding
	// synced (or anything later), while unlinked WAL segments stay
	// unlinked.
	written uint64
	synced  uint64
}

func newCheckpointWriter(dir, name string) *checkpointWriter {
	w := &checkpointWriter{path: filepath.Join(dir, name), delay: checkpointSyncDelay}
	w.idle.L = &w.mu
	return w
}

// store writes nextSeq and schedules its fdatasync. Stores are
// serialized by the caller (the dispatcher is the single writer) and
// Close runs after it stops; the background flush is the only other
// goroutine touching the file.
func (w *checkpointWriter) store(nextSeq uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], nextSeq)

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.syncErr != nil {
		// A background flush failed: report it once and drop the
		// descriptor. This call writes nothing; the caller retries
		// (the dispatcher's storedSeq did not advance), and that next
		// store reopens the file and writes its value, which a new
		// flush then syncs.
		err := w.syncErr
		w.syncErr = nil
		w.resetLocked()
		return fmt.Errorf("ingress: sync checkpoint: %w", err)
	}
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
	if _, err := syncfile.WriteAt(w.file, buf[:], 0); err != nil {
		w.resetLocked()
		return fmt.Errorf("ingress: write checkpoint: %w", err)
	}
	w.written = nextSeq
	if !w.dirSynced {
		// A new name must be durable before anything relies on it:
		// flush the data and the directory now, once.
		if err := syncfile.SyncData(w.file); err != nil {
			w.resetLocked()
			return fmt.Errorf("ingress: sync checkpoint: %w", err)
		}
		dirFile, err := os.Open(filepath.Dir(w.path))
		if err != nil {
			return fmt.Errorf("ingress: open checkpoint dir: %w", err)
		}
		syncErr := syncfile.Sync(dirFile)
		_ = dirFile.Close()
		if syncErr != nil {
			return fmt.Errorf("ingress: sync checkpoint dir: %w", syncErr)
		}
		w.dirSynced = true
		w.markSyncedLocked(nextSeq)
		return nil
	}
	w.dirty = true
	if !w.flushArmed {
		w.flushArmed = true
		time.AfterFunc(w.delay, w.flush)
	}
	return nil
}

// storeDurable writes nextSeq and fdatasyncs it before returning, so
// the WAL may be compacted behind it at once. OpenManager uses it to
// make the recovered value durable (a process crash can leave it
// written but not flushed) before anything compacts behind it.
func (w *checkpointWriter) storeDurable(nextSeq uint64) error {
	if err := w.store(nextSeq); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waitIdleLocked()
	if w.file == nil {
		return errors.New("ingress: checkpoint file closed")
	}
	if w.dirty {
		if err := syncfile.SyncData(w.file); err != nil {
			w.resetLocked()
			return fmt.Errorf("ingress: sync checkpoint: %w", err)
		}
		w.dirty = false
	}
	w.markSyncedLocked(w.written)
	return nil
}

// durable returns the highest checkpoint known to be on disk: the WAL
// may be compacted up to it and no further. 0 for a nil writer.
func (w *checkpointWriter) durable() uint64 {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.synced
}

// markSyncedLocked records that an fdatasync covered v. Checkpoints
// only move forward, so synced never goes back.
func (w *checkpointWriter) markSyncedLocked(v uint64) {
	w.synced = max(w.synced, v)
}

// flush is the background fdatasync a store schedules.
func (w *checkpointWriter) flush() {
	w.mu.Lock()
	w.flushArmed = false
	if w.file == nil || !w.dirty || w.syncing {
		w.mu.Unlock()
		return
	}
	f := w.file
	value := w.written
	w.dirty = false
	w.syncing = true
	w.mu.Unlock()

	err := syncfile.SyncData(f)

	w.mu.Lock()
	w.syncing = false
	switch {
	case err != nil:
		w.dirty = true
		w.syncErr = err
	default:
		w.markSyncedLocked(value)
		// A store that landed during the sync found this flush running
		// and did not arm another; arm it here so the newer value (and
		// compaction behind it) does not wait for the next store.
		if w.dirty && !w.flushArmed && w.file != nil {
			w.flushArmed = true
			time.AfterFunc(w.delay, w.flush)
		}
	}
	w.idle.Broadcast()
	w.mu.Unlock()
}

// waitIdleLocked waits for a running background flush to finish, so
// the descriptor it uses is not closed under it.
func (w *checkpointWriter) waitIdleLocked() {
	for w.syncing {
		w.idle.Wait()
	}
}

// resetLocked drops the held descriptor after a failure so the next
// store reopens the file. The value is rewritten by that store.
func (w *checkpointWriter) resetLocked() {
	w.waitIdleLocked()
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	w.dirty = false
}

// close syncs a written-but-unsynced value and closes the descriptor.
func (w *checkpointWriter) close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.waitIdleLocked()
	if w.file == nil {
		return nil
	}
	var err error
	if w.dirty {
		if serr := syncfile.SyncData(w.file); serr != nil {
			err = fmt.Errorf("ingress: sync checkpoint: %w", serr)
		} else {
			w.markSyncedLocked(w.written)
		}
		w.dirty = false
	}
	if cerr := w.file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	w.file = nil
	return err
}
