package metastore

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// bbolt takes an exclusive flock on open and, with no Timeout option,
// waits for it forever. A second narad process pointed at the same data
// directory (a stale pod, a double start on bare metal, an operator's
// inspection tool holding fsm.db) used to hang silently in
// metastore.New with no log line and no error. The open now waits at
// most boltOpenTimeout and fails with an error that names the file.
func TestMetastoreOpenFailsFastWhenDBLocked(t *testing.T) {
	prev := boltOpenTimeout
	boltOpenTimeout = 300 * time.Millisecond
	t.Cleanup(func() { boltOpenTimeout = prev })

	for _, file := range []string{"fsm.db", "raft.db"} {
		t.Run(file, func(t *testing.T) {
			dir := t.TempDir()
			holder, err := bolt.Open(filepath.Join(dir, file), 0o600, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Close()

			done := make(chan error, 1)
			go func() {
				s, err := New(Config{NodeID: "n1", DataDir: dir, BindAddr: "127.0.0.1:0"})
				if err == nil {
					_ = s.Close()
				}
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("metastore.New succeeded while another process held the database lock")
				}
				if !errors.Is(err, bolt.ErrTimeout) {
					t.Fatalf("error = %v, want a bolt.ErrTimeout", err)
				}
				if want := filepath.Join(dir, file); !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q does not name the locked file %s", err, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("metastore.New hung on the locked bbolt file instead of timing out")
			}
		})
	}
}

// HasExistingState opens raft.db too, and must not hang either.
func TestHasExistingStateFailsFastWhenDBLocked(t *testing.T) {
	prev := boltOpenTimeout
	boltOpenTimeout = 300 * time.Millisecond
	t.Cleanup(func() { boltOpenTimeout = prev })

	dir := t.TempDir()
	holder, err := bolt.Open(filepath.Join(dir, "raft.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()

	done := make(chan error, 1)
	go func() {
		_, err := HasExistingState(dir)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, bolt.ErrTimeout) {
			t.Fatalf("error = %v, want bolt.ErrTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("HasExistingState hung on the locked raft.db")
	}
}
