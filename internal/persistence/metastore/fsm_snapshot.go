package metastore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

func (f *fsmState) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var buf bytes.Buffer
	err := f.view("snapshot", func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buf)
		return err
	})
	return &fsmSnapshot{data: buf.Bytes()}, err
}

func (f *fsmState) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	tmp := f.dbPath + ".restore"
	if err := writeFileSync(tmp, data); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Close()
	if err := os.Rename(tmp, f.dbPath); err != nil {
		// The old file is still in place; reopen it so f.db is never
		// left holding a closed database.
		db, reopenErr := openBolt(f.dbPath)
		if reopenErr != nil {
			return fmt.Errorf("metastore: restore rename: %v; reopen old db: %w", err, reopenErr)
		}
		f.db = db
		return err
	}
	// Make the rename durable, same pattern as segment/checkpoint writes.
	if dir, err := os.Open(filepath.Dir(f.dbPath)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	db, err := openBolt(f.dbPath)
	if err != nil {
		// The old file is gone; nothing left to reopen. Surface a hard
		// error rather than silently keeping a closed handle.
		return fmt.Errorf("metastore: reopen restored db: %w", err)
	}
	f.db = db
	f.version.Add(1)
	f.versions.bumpAll()
	return nil
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
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
