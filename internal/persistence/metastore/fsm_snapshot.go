package metastore

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// Snapshot copies the database to a temporary file beside it for Raft
// to persist. Raft calls it on the FSM goroutine, serialised with Apply
// and Restore, so the copy holds exactly the entries applied so far.
// The copy is made under a read transaction that ends before Snapshot
// returns, and Persist then streams the file into Raft's snapshot sink
// on Raft's own goroutine while Apply carries on; Release removes it.
//
// Nothing large is held in memory. This used to copy the whole database
// into a buffer that grew by doubling (a 26 MiB database allocated
// 64 MiB per snapshot), so a metastore grown large enough could crash
// every node at its next snapshot. No read transaction outlives Snapshot either: an open one
// makes any commit that grows bbolt's memory map wait for it, and Raft
// persists while the FSM applies. The price is free disk equal to the
// database's size for the copy; without it the snapshot fails, which is
// logged and counted, and Raft tries again at its next interval.
//
// It refuses while the FSM has stopped applying, and while the database
// is ahead of what Raft has handed the FSM since this start (a restart
// that kept a database newer than the snapshot Raft resumed from): Raft
// labels the image with its own applied index, and an image whose
// content ran past its label would have entries replayed on top of it
// by a node that restores it (a 3.0.x node does not skip them). Raft
// retries at its next snapshot interval, and the replay that closes the
// gap only skips entries, so it takes milliseconds.
func (f *fsmState) Snapshot() (raft.FSMSnapshot, error) {
	if err := f.stopErr(); err != nil {
		return nil, err
	}
	if applied, seen := f.applied.Load(), f.lastSeen.Load(); applied > seen {
		return nil, fmt.Errorf("metastore: snapshot deferred: the database holds raft index %d but the replay since the start has reached only %d", applied, seen)
	}
	started := time.Now()
	f.mu.RLock()
	defer f.mu.RUnlock()
	path, size, err := f.copyDatabase()
	if err != nil {
		f.snapshotFailures.Add(1)
		f.log.Error("metastore: could not copy the database for a raft snapshot; raft tries again at its next snapshot interval, and its log grows until a snapshot succeeds",
			"path", f.dbPath, "error", err, "hint", "a snapshot needs free disk beside fsm.db equal to its size")
		return nil, fmt.Errorf("metastore: snapshot: copy %s: %w", f.dbPath, err)
	}
	return &fsmSnapshot{f: f, path: path, size: size, started: started}, nil
}

// snapshotCopySuffix starts the name of the temporary copy Snapshot
// makes beside the database (fsm.db.snapshot-<random>).
const snapshotCopySuffix = ".snapshot-"

// copyDatabase writes a consistent copy of the database to a new file
// beside it and returns its path and size. The read transaction ends
// before it returns. On failure nothing is left behind.
func (f *fsmState) copyDatabase() (path string, size int64, err error) {
	file, err := os.CreateTemp(filepath.Dir(f.dbPath), filepath.Base(f.dbPath)+snapshotCopySuffix+"*")
	if err != nil {
		return "", 0, err
	}
	err = f.view(func(tx *bolt.Tx) error {
		size, err = tx.WriteTo(file)
		return err
	})
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(file.Name())
		return "", 0, err
	}
	return file.Name(), size, nil
}

// removeLeftovers removes the temporary files a crash can leave beside
// the database: snapshot copies Release never removed, and a restore
// image never installed. It runs once the database is open, so no
// other process is using them (bbolt holds an exclusive lock on it).
func removeLeftovers(log *slog.Logger, dbPath string) {
	copies, _ := filepath.Glob(dbPath + snapshotCopySuffix + "*")
	for _, p := range append(copies, dbPath+restoreSuffix) {
		if err := os.Remove(p); err == nil {
			log.Info("metastore: removed a temporary file a crash left beside the database", "path", p)
		} else if !errors.Is(err, os.ErrNotExist) {
			log.Warn("metastore: could not remove a temporary file a crash left beside the database", "path", p, "error", err)
		}
	}
}

// restoreSuffix names the file Restore streams an image into before it
// replaces the database (fsm.db.restore).
const restoreSuffix = ".restore"

// Restore replaces the local database with a leader snapshot: the image
// is streamed to a sidecar file and fsynced (never read into memory),
// checked, atomically renamed over the live database, and reopened. On
// any failure f.db is left holding an open database: either the new one
// or the untouched old one, and the sidecar is removed.
//
// The image is read before it replaces anything. One that is not a
// database is refused with an error and the live database stays. One
// that has applied an entry type newer than this build knows stops the
// FSM (and the node): installing it would apply its log tail with older
// semantics, and failing the restore alone would let Raft run on in
// its place, or fall back at start to an older snapshot whose log tail
// is compacted away.
func (f *fsmState) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.stopErr(); err != nil {
		return err
	}
	tmp := f.dbPath + restoreSuffix
	if err := writeFileSync(tmp, rc); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("metastore: restore: write the snapshot to %s: %w", tmp, err)
	}
	image, err := readImageMeta(tmp)
	if err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("metastore: restore: the snapshot is not a readable database: %w", err)
	}
	if image.maxEntryType > uint64(MaxEntryType) {
		_ = os.Remove(tmp)
		f.applyErrors[applyErrNewerDatabase].Add(1)
		cause := newerDatabaseError("the raft snapshot", image.maxEntryType, f.build)
		return f.stop(0, image.maxEntryType, cause.Error(), cause)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Close()
	if err := os.Rename(tmp, f.dbPath); err != nil {
		_ = os.Remove(tmp)
		// The old file is still in place; reopen it so f.db is never
		// left holding a closed database.
		db, _, reopenErr := openBolt(f.dbPath, f.build)
		if reopenErr != nil {
			return fmt.Errorf("metastore: restore rename: %v; reopen old db: %w", err, reopenErr)
		}
		f.db = db
		return err
	}
	// Make the rename durable, same pattern as segment/checkpoint writes.
	syncDir(f.dbPath)

	db, meta, err := openBolt(f.dbPath, f.build)
	if err != nil {
		// The old file is gone; nothing left to reopen. Surface a hard
		// error rather than silently keeping a closed handle.
		return fmt.Errorf("metastore: reopen restored db: %w", err)
	}
	f.db = db
	// An image this release wrote of its own database carries a trusted
	// index (WriteTo stamps the read transaction's id into the image), so
	// the replay after it skips what it holds. Any other image counts as
	// holding nothing Raft will hand the FSM again.
	f.applied.Store(meta.trustedApplied())
	f.lastSeen.Store(meta.trustedApplied())
	f.version.Add(1)
	f.versions.bumpAll()
	return nil
}

// syncDir fsyncs the directory holding path, making a rename in it
// durable. Best effort, like the segment and checkpoint writes.
func syncDir(path string) {
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
}

// writeFileSync streams r to path and fsyncs it before closing, so the
// restored snapshot is durable before it replaces the live database.
func writeFileSync(path string, r io.Reader) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(file, r); err != nil {
		_ = file.Close()
		return err
	}
	if err := syncfile.SyncData(file); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// fsmSnapshot is a copy of the database in a temporary file. Persist
// streams it into Raft's sink and Release removes it.
type fsmSnapshot struct {
	f    *fsmState
	path string
	size int64
	// started is when Snapshot began the copy; the snapshot's duration
	// runs from there to the sink's close.
	started time.Time
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.persist(sink); err != nil {
		_ = sink.Cancel()
		s.f.snapshotFailures.Add(1)
		s.f.log.Error("metastore: could not persist a raft snapshot; raft tries again at its next snapshot interval, and its log grows until a snapshot succeeds",
			"copy", s.path, "bytes", s.size, "error", err)
		return err
	}
	s.f.snapshotBytes.Store(s.size)
	s.f.snapshotNanos.Store(int64(time.Since(s.started)))
	return nil
}

func (s *fsmSnapshot) persist(sink raft.SnapshotSink) error {
	file, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer file.Close()
	n, err := io.Copy(sink, file)
	if err != nil {
		return err
	}
	if n != s.size {
		return fmt.Errorf("metastore: snapshot copy %s holds %d bytes, want %d", s.path, n, s.size)
	}
	return sink.Close()
}

// Release removes the copy. Raft calls it once, after Persist or
// instead of it; a crash before it leaves the copy for the next open to
// remove.
func (s *fsmSnapshot) Release() {
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.f.log.Warn("metastore: could not remove a raft snapshot copy; the next start removes it", "path", s.path, "error", err)
	}
}
