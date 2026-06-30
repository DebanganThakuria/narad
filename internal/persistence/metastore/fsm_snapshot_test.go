package metastore

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

type fakeSnapshotSink struct {
	bytes.Buffer
}

func (s *fakeSnapshotSink) ID() string    { return "test-snapshot" }
func (s *fakeSnapshotSink) Cancel() error { return nil }
func (s *fakeSnapshotSink) Close() error  { return nil }

// Snapshot -> Persist -> Restore must reproduce the source DB. This guards the
// durable-restore path (fsync of the temp file + parent dir) against
// regressions while confirming the round-trip still yields a readable DB.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	src, err := newFSM(filepath.Join(t.TempDir(), "fsm.db"), nil)
	if err != nil {
		t.Fatalf("newFSM(src): %v", err)
	}
	key, want := []byte("orders"), []byte(`{"name":"orders"}`)
	if err := src.update("seed", func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTopics).Put(key, want)
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &fakeSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	dst, err := newFSM(filepath.Join(t.TempDir(), "fsm.db"), nil)
	if err != nil {
		t.Fatalf("newFSM(dst): %v", err)
	}
	if err := dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	var got []byte
	if err := dst.view("read", func(tx *bolt.Tx) error {
		got = append([]byte(nil), tx.Bucket(bucketTopics).Get(key)...)
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("restored value = %q, want %q", got, want)
	}
}
