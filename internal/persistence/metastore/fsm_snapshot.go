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
	if err := writeFileSync(tmp, data, 0o600); err != nil {
		return fmt.Errorf("metastore: write restore db: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Close()
	if err := os.Rename(tmp, f.dbPath); err != nil {
		return fmt.Errorf("metastore: rename restore db: %w", err)
	}
	// fsync the directory so the rename itself is durable: without it a
	// crash here can leave the old (or no) directory entry on reboot while
	// the Raft log has been compacted past this snapshot.
	if err := fsyncDir(filepath.Dir(f.dbPath)); err != nil {
		return fmt.Errorf("metastore: fsync db dir: %w", err)
	}
	db, err := openBolt(f.dbPath)
	if err != nil {
		return err
	}
	f.db = db
	f.version.Add(1)
	f.versions.bumpAll()
	return nil
}

// writeFileSync writes data to path and fsyncs the file before returning.
// os.WriteFile does not fsync, so its bytes can be lost on a crash.
func writeFileSync(path string, data []byte, perm os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
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

// fsyncDir fsyncs a directory so a create/rename within it is durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
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
