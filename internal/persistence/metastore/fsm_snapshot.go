package metastore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// Snapshot captures the whole bbolt database as one in-memory blob.
// Raft serialises Snapshot with Apply, so the copy is consistent.
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
	f.mu.RLock()
	defer f.mu.RUnlock()
	var buf bytes.Buffer
	err := f.view(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buf)
		return err
	})
	return &fsmSnapshot{data: buf.Bytes()}, err
}

// Restore replaces the local database with a leader snapshot: the blob
// is written and fsynced to a sidecar file, checked, atomically renamed
// over the live database, and reopened. On any failure f.db is left
// holding an open database: either the new one or the untouched old one.
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
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	tmp := f.dbPath + ".restore"
	if err := writeFileSync(tmp, data); err != nil {
		return err
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

// writeFileSync writes data to path and fsyncs it before closing so the
// restored snapshot is durable before it replaces the live database.
func writeFileSync(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := syncfile.SyncData(file); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// fsmSnapshot is a fully materialised database image; Persist just
// streams it into the sink.
type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
