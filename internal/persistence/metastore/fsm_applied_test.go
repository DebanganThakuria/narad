package metastore

// The applied index is stored with each entry's effects, so a restart
// re-applies nothing the database already holds: not the log replay of
// a node without a snapshot, and not the tail after a snapshot. A
// database whose index cannot be trusted is rebuilt or restored instead,
// and one written by a newer release is refused.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

var attachSchema = []byte(`{"type":"object","properties":{"id":{"type":"string"}}}`)

// writeRefusedAttachHistory leaves the log with an attach the FSM
// refused (the child has a schema, the parent none) followed by the
// parent getting the same schema. Re-evaluated against the later state,
// the refused attach would succeed.
func writeRefusedAttachHistory(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{"par", "kid"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 1}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	if err := s.PutSchema(ctx, "kid", 1, attachSchema); err != nil {
		t.Fatalf("kid schema: %v", err)
	}
	if err := s.AttachChild(ctx, "par", "kid", 0); err == nil {
		t.Fatal("attach of a child with a schema to a parent without one was accepted; the scenario needs a refusal")
	}
	if err := s.PutSchema(ctx, "par", 1, attachSchema); err != nil {
		t.Fatalf("par schema: %v", err)
	}
}

func requireStandaloneKid(t *testing.T, s *Store) {
	t.Helper()
	kid, err := s.GetTopic(context.Background(), "kid")
	if err != nil {
		t.Fatalf("get kid: %v", err)
	}
	if kid.IsChild() {
		t.Fatalf("after the restart kid is a child of %q; the attach was refused when it was first applied", kid.Parent)
	}
}

// editDatabase opens a closed store's fsm.db directly, as another
// program (an older release, a tool) would, and runs fn in a write
// transaction.
func editDatabase(t *testing.T, dataDir string, fn func(*bolt.Tx) error) {
	t.Helper()
	db, err := bolt.Open(filepath.Join(dataDir, "fsm.db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open fsm.db: %v", err)
	}
	defer db.Close()
	if err := db.Update(fn); err != nil {
		t.Fatalf("edit fsm.db: %v", err)
	}
}

func deleteFSMMeta(tx *bolt.Tx) error {
	if err := tx.DeleteBucket([]byte("fsm_meta")); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return err
	}
	return nil
}

func putMetaKey(tx *bolt.Tx, key string, v uint64) error {
	b, err := tx.CreateBucketIfNotExists([]byte("fsm_meta"))
	if err != nil {
		return err
	}
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], v)
	return b.Put([]byte(key), raw[:])
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v", path, err)
	}
	return err == nil
}

func TestRestartReplayDoesNotReEvaluateARefusedAttach(t *testing.T) {
	cfg := singleNodeConfig(t)
	h := openStore(t, cfg)
	writeRefusedAttachHistory(t, h.s)
	h.close(t)

	h.reopen(t, cfg)
	requireStandaloneKid(t, h.s)
}

func TestAppliedIndexIsCommittedWithTheEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fsm.db")
	readOnly := func() (applied, txid, fileTxid uint64) {
		t.Helper()
		db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second, ReadOnly: true})
		if err != nil {
			t.Fatalf("read-only open: %v", err)
		}
		defer db.Close()
		applied, txid = storedMeta(t, db, "applied"), storedMeta(t, db, "txid")
		if err := db.View(func(tx *bolt.Tx) error { fileTxid = uint64(tx.ID()); return nil }); err != nil {
			t.Fatal(err)
		}
		return applied, txid, fileTxid
	}

	f, err := newFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyEntry(t, f, 5, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("create: %v", err)
	}
	_ = f.db.Close()
	if applied, txid, fileTxid := readOnly(); applied != 5 || txid != fileTxid {
		t.Fatalf("after a success: applied %d (want 5), txid %d (want the file's %d)", applied, txid, fileTxid)
	}

	f, err = newFSM(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := applyEntry(t, f, 6, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate create = %v, want ErrAlreadyExists", err)
	}
	_ = f.db.Close()
	if applied, txid, fileTxid := readOnly(); applied != 6 || txid != fileTxid {
		t.Fatalf("after a refusal: applied %d (want 6), txid %d (want the file's %d)", applied, txid, fileTxid)
	}
}

// After a snapshot a restart neither restores the snapshot over a
// database that already covers it nor applies the tail again.
func TestRestartAfterASnapshotReappliesNothing(t *testing.T) {
	ctx := context.Background()
	cfg := singleNodeConfig(t)
	h := openStore(t, cfg)
	for i := range 3 {
		if err := h.s.CreateTopic(ctx, topic.Topic{Name: fmt.Sprintf("before-%d", i), Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.s.r.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for i := range 3 {
		if err := h.s.CreateTopic(ctx, topic.Topic{Name: fmt.Sprintf("after-%d", i), Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	h.close(t)

	h.reopen(t, cfg)
	if got := h.s.MetadataVersion(); got != 0 {
		t.Fatalf("metadata version = %d after the restart, want 0: the snapshot was restored or the tail re-applied", got)
	}
	for _, prefix := range []string{"before", "after"} {
		for i := range 3 {
			if _, err := h.s.GetTopic(ctx, fmt.Sprintf("%s-%d", prefix, i)); err != nil {
				t.Fatalf("%s-%d after the restart: %v", prefix, i, err)
			}
		}
	}
}

// The first restart on this release of a node without a snapshot: its
// fsm.db was written by 3.0.x and has no index, so the replay would
// re-evaluate every entry on top of it. It is set aside and rebuilt.
func TestUpgradeRestartWithoutASnapshotRebuildsFromTheLog(t *testing.T) {
	cfg := singleNodeConfig(t)
	h := openStore(t, cfg)
	writeRefusedAttachHistory(t, h.s)
	h.close(t)
	editDatabase(t, cfg.DataDir, deleteFSMMeta)

	h.reopen(t, cfg)
	if !fileExists(t, filepath.Join(cfg.DataDir, "fsm.db.stale")) {
		t.Fatal("fsm.db without an applied index was not set aside as fsm.db.stale")
	}
	requireStandaloneKid(t, h.s)
}

// A write by anything else (a rolled-back 3.0.x node, a tool) moves the
// file's transaction id past the one stored with the index, so the
// index no longer describes the file and is not used to skip entries.
func TestForeignWriteUntrustsThePersistedIndex(t *testing.T) {
	ctx := context.Background()
	cfg := singleNodeConfig(t)
	h := openStore(t, cfg)
	writeRefusedAttachHistory(t, h.s)
	h.close(t)
	editDatabase(t, cfg.DataDir, func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTopics).Put([]byte("foreign"), []byte(`{"name":"foreign","partitions":1}`))
	})

	h.reopen(t, cfg)
	if !fileExists(t, filepath.Join(cfg.DataDir, "fsm.db.stale")) {
		t.Fatal("fsm.db written by another program was not set aside")
	}
	if _, err := h.s.GetTopic(ctx, "foreign"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the foreign write survived the rebuild (get = %v)", err)
	}
	requireStandaloneKid(t, h.s)
}

// removeRaftState deletes a closed store's Raft log and snapshots, as a
// lost or wiped raft.db would, and returns a copy of them in backup.
func removeRaftState(t *testing.T, dataDir string) (backup string) {
	t.Helper()
	backup = t.TempDir()
	for _, name := range []string{"raft.db", "snapshots"} {
		if err := os.Rename(filepath.Join(dataDir, name), filepath.Join(backup, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	return backup
}

// A node that would bootstrap with no Raft state beside an fsm.db that
// holds metadata refuses to start: a new cluster on an empty database
// would hold none of its topics, and the startup sweep would then remove
// their partition directories. The refusal leaves fsm.db untouched and
// says what to do; restoring the Raft state brings the topics back.
func TestBootstrapWithoutRaftStateRefusesADatabaseThatHoldsMetadata(t *testing.T) {
	ctx := context.Background()
	cfg := singleNodeConfig(t)
	h := openStore(t, cfg)
	if err := h.s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	h.close(t)
	backup := removeRaftState(t, cfg.DataDir)
	fsmPath := filepath.Join(cfg.DataDir, "fsm.db")
	before, err := os.ReadFile(fsmPath)
	if err != nil {
		t.Fatal(err)
	}

	// Twice: the first refusal must leave nothing that lets the next
	// start go ahead on an empty database.
	for attempt := 1; attempt <= 2; attempt++ {
		s, err := New(cfg)
		if err == nil {
			_ = s.Close()
			t.Fatalf("start %d: a node with no raft state started beside an fsm.db holding topics", attempt)
		}
		for _, want := range []string{fsmPath, "no raft state", "restore raft.db", "move fsm.db"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("start %d: New = %v; want it to say %q", attempt, err, want)
			}
		}
		if fileExists(t, fsmPath+".stale") {
			t.Fatalf("start %d: fsm.db was set aside", attempt)
		}
		if after, _ := os.ReadFile(fsmPath); !bytes.Equal(before, after) {
			t.Fatalf("start %d: the refused start wrote to fsm.db", attempt)
		}
	}

	// The refused starts left an empty raft.db and snapshots directory;
	// the backup replaces them.
	for _, name := range []string{"raft.db", "snapshots"} {
		if err := os.RemoveAll(filepath.Join(cfg.DataDir, name)); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(backup, name), filepath.Join(cfg.DataDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	h.reopen(t, cfg)
	if _, err := h.s.GetTopic(ctx, "orders"); err != nil {
		t.Fatalf("orders after the raft state was restored: %v", err)
	}
}

// A node that joins an existing cluster with no Raft state sets an old
// fsm.db aside: it describes another log, and the leader's log or
// snapshot rebuilds the database.
func TestJoinOnlyNodeWithoutRaftStateSetsAStaleDatabaseAside(t *testing.T) {
	ctx := context.Background()
	cfg := singleNodeConfig(t)
	h := openStore(t, cfg)
	if err := h.s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	h.close(t)
	removeRaftState(t, cfg.DataDir)

	cfg.JoinOnly = true
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New(join-only): %v", err)
	}
	h.s = s
	if !fileExists(t, filepath.Join(cfg.DataDir, "fsm.db.stale")) {
		t.Fatal("fsm.db from before the Raft state was lost was not set aside")
	}
	if _, err := s.GetTopic(ctx, "orders"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the joiner's replica holds a topic only the old log had (get = %v)", err)
	}
}

// backupMetastore copies a closed store's raft.db, snapshots and fsm.db
// to a new directory, as a backup of the volume would.
func backupMetastore(t *testing.T, dataDir string) (backup string) {
	t.Helper()
	backup = t.TempDir()
	copyMetastoreFiles(t, dataDir, backup, "raft.db", "snapshots", "fsm.db")
	return backup
}

// copyMetastoreFiles replaces each named file or directory in to with
// the one in from.
func copyMetastoreFiles(t *testing.T, from, to string, names ...string) {
	t.Helper()
	for _, name := range names {
		src, dst := filepath.Join(from, name), filepath.Join(to, name)
		if err := os.RemoveAll(dst); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(src)
		if err != nil {
			t.Fatal(err)
		}
		if info.IsDir() {
			err = os.CopyFS(dst, os.DirFS(src))
		} else {
			var raw []byte
			if raw, err = os.ReadFile(src); err == nil {
				err = os.WriteFile(dst, raw, 0o600)
			}
		}
		if err != nil {
			t.Fatalf("copy %s: %v", name, err)
		}
	}
}

// restoreOlderRaftState leaves a closed single-node store whose raft.db
// and snapshots come from a backup taken after topic "early" was created
// (and, with snapshot, after a Raft snapshot), beside the fsm.db that
// also holds "late", created after the backup. It returns the backup.
func restoreOlderRaftState(t *testing.T, cfg Config, snapshot bool) (backup string) {
	t.Helper()
	ctx := context.Background()
	h := openStore(t, cfg)
	if err := h.s.CreateTopic(ctx, topic.Topic{Name: "early", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if snapshot {
		if err := h.s.r.Snapshot().Error(); err != nil {
			t.Fatalf("snapshot: %v", err)
		}
	}
	h.close(t)
	backup = backupMetastore(t, cfg.DataDir)

	h.reopen(t, cfg)
	if err := h.s.CreateTopic(ctx, topic.Topic{Name: "late", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	h.close(t)
	copyMetastoreFiles(t, backup, cfg.DataDir, "raft.db", "snapshots")
	return backup
}

// A node whose raft.db and snapshots were restored from a backup beside
// a newer fsm.db refuses to start. The database holds entries past the
// end of the restored log and snapshot; rebuilding it from them would
// drop every topic created since the backup, and the startup sweep would
// then remove their partition directories. The refusal names the indexes
// and leaves fsm.db untouched, and restoring fsm.db from the same backup
// starts the node as the backup left it.
func TestRaftStateRestoredFromAnOlderBackupIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapshot bool
	}{
		{"no snapshot", false},
		{"the backup holds a snapshot", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := singleNodeConfig(t)
			backup := restoreOlderRaftState(t, cfg, tc.snapshot)
			fsmPath := filepath.Join(cfg.DataDir, "fsm.db")
			meta, err := readImageMeta(fsmPath)
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(fsmPath)
			if err != nil {
				t.Fatal(err)
			}

			for attempt := 1; attempt <= 2; attempt++ {
				s, err := New(cfg)
				if err == nil {
					_, lateErr := s.GetTopic(ctx, "late")
					_ = s.Close()
					t.Fatalf("start %d: a node whose raft state is older than fsm.db started (late: %v)", attempt, lateErr)
				}
				for _, want := range []string{fsmPath, "raft state is older than fsm.db", fmt.Sprintf("applied raft index %d", meta.applied), "same backup", "topics directory"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("start %d: New = %v; want it to say %q", attempt, err, want)
					}
				}
				if fileExists(t, fsmPath+".stale") {
					t.Fatalf("start %d: fsm.db was set aside", attempt)
				}
				if after, _ := os.ReadFile(fsmPath); !bytes.Equal(before, after) {
					t.Fatalf("start %d: the refused start wrote to fsm.db", attempt)
				}
			}

			copyMetastoreFiles(t, backup, cfg.DataDir, "fsm.db")
			h := openStore(t, cfg)
			if _, err := h.s.GetTopic(ctx, "early"); err != nil {
				t.Fatalf("early after fsm.db was restored from the same backup: %v", err)
			}
		})
	}
}

// A node that joins a running cluster sets aside a database newer than
// its Raft state, and the cluster's log rebuilds it.
func TestJoinOnlyNodeSetsADatabaseNewerThanItsRaftStateAside(t *testing.T) {
	ctx := context.Background()
	cfg := singleNodeConfig(t)
	restoreOlderRaftState(t, cfg, false)

	cfg.JoinOnly = true
	h := openStore(t, cfg)
	if !fileExists(t, filepath.Join(cfg.DataDir, "fsm.db.stale")) {
		t.Fatal("fsm.db newer than the raft state was not set aside")
	}
	if _, err := h.s.GetTopic(ctx, "early"); err != nil {
		t.Fatalf("early after the rebuild: %v", err)
	}
}

func TestDatabaseFromANewerReleaseIsRefusedAtOpen(t *testing.T) {
	cfg := singleNodeConfig(t)
	cfg.Build = "narad test"
	db, err := bolt.Open(filepath.Join(cfg.DataDir, "fsm.db"), 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { return putMetaKey(tx, "max_entry_type", uint64(MaxEntryType)+1) }); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	before, err := os.ReadFile(filepath.Join(cfg.DataDir, "fsm.db"))
	if err != nil {
		t.Fatal(err)
	}

	s, err := New(cfg)
	if err == nil {
		_ = s.Close()
		t.Fatal("opened a database holding an entry type newer than this build")
	}
	if after, _ := os.ReadFile(filepath.Join(cfg.DataDir, "fsm.db")); !bytes.Equal(before, after) {
		t.Fatal("the refused open wrote to the newer release's database")
	}
	for _, want := range []string{fmt.Sprintf("entry type %d", MaxEntryType+1), "newer Narad release", "narad test"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("New = %v; want it to name %q", err, want)
		}
	}
}

// newerImage is a database image that records an entry type newer than
// this build, holding one topic.
func newerImage(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "image.db")
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketTopics)
		if err != nil {
			return err
		}
		if err := b.Put([]byte("from-newer"), []byte(`{"name":"from-newer","partitions":1}`)); err != nil {
			return err
		}
		return putMetaKey(tx, "max_entry_type", uint64(MaxEntryType)+1)
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	image, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func TestSnapshotFromANewerReleaseStopsTheRestore(t *testing.T) {
	f := newTestFSM(t)
	if err := applyEntry(t, f, 1, opCreateTopic, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	live := f.db

	err := f.Restore(io.NopCloser(bytes.NewReader(newerImage(t))))
	if !errors.Is(err, ErrStoppedApplying) {
		t.Fatalf("Restore of a newer release's snapshot = %v, want ErrStoppedApplying", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("entry type %d", MaxEntryType+1)) {
		t.Fatalf("Restore error %q does not name the entry type", err)
	}
	if f.db != live || !hasTopic(t, f, "orders") || hasTopic(t, f, "from-newer") {
		t.Fatal("the refused snapshot replaced the live database")
	}
	if err := applyEntry(t, f, 2, opCreateTopic, topic.Topic{Name: "events", Partitions: 1}); !errors.Is(err, ErrStoppedApplying) {
		t.Fatalf("an entry after the refused restore = %v, want ErrStoppedApplying", err)
	}
}

// A database ahead of what Raft has handed the FSM since the start (a
// restart that skipped the snapshot restore) must not be snapshotted:
// Raft labels the image with its own applied index, and a node restoring
// it would replay entries the image already holds.
func TestSnapshotWaitsForTheReplayToReachTheDatabase(t *testing.T) {
	ctx := context.Background()
	cfg := singleNodeConfig(t)
	h := openStore(t, cfg)
	if err := h.s.CreateTopic(ctx, topic.Topic{Name: "before", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := h.s.r.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	for i := range 3 {
		if err := h.s.CreateTopic(ctx, topic.Topic{Name: fmt.Sprintf("after-%d", i), Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	h.close(t)

	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.s = s
	// A single voter needs an election timeout before it commits, so the
	// replay has not started yet.
	if snap, err := s.fsm.Snapshot(); err == nil {
		snap.Release()
		t.Fatal("snapshot taken while the database is ahead of the replay")
	}
	waitUntil(t, 15*time.Second, "caught up", func() bool { return s.IsLeader() && s.AppliedCaughtUp() })
	snap, err := s.fsm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot once the replay reached the database: %v", err)
	}
	snap.Release()
}
