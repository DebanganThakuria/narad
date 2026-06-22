package metastore

import (
	"bytes"
	"io"
	"os"

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
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.db.Close()
	if err := os.Rename(tmp, f.dbPath); err != nil {
		return err
	}
	db, err := openBolt(f.dbPath)
	if err != nil {
		return err
	}
	f.db = db
	f.version.Add(1)
	return nil
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
