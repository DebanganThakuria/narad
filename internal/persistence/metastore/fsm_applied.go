package metastore

// The persisted applied index.
//
// fsm.db outlives the process, and Raft replays its log into the FSM on
// every start: from index 1 when there is no snapshot, or the tail after
// the snapshot it restores. Replaying onto a database that already holds
// those entries applied them twice, and entries that are not idempotent
// were re-evaluated against later state (an attach refused at the time
// succeeded on one node's replay, and that node alone treated the topic
// as a child). It also cost one synced transaction per entry.
//
// So every entry's index is written to the fsm_meta bucket in the same
// transaction as its effects (a refused entry's in a small transaction
// of its own), and Apply skips entries at or below it. Beside it go the
// bbolt transaction id that wrote it and the newest entry type the
// database has applied.
//
// The index describes the file only while nothing else has written it.
// Every bbolt commit moves the file's transaction id, so the index is
// trusted only while the stored txid is still the file's: a write by
// another program (a 3.0.x binary after a rollback, which ignores
// fsm_meta, or a tool) untrusts it, and the start-up rules in store.go
// then restore a snapshot or rebuild the file from the log instead of
// skipping anything.

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketFSMMeta = []byte("fsm_meta")

	metaKeyApplied      = []byte("applied")
	metaKeyTxid         = []byte("txid")
	metaKeyMaxEntryType = []byte("max_entry_type")
)

// dataBuckets are the buckets that hold metadata, as opposed to
// fsm_meta's bookkeeping.
var dataBuckets = [][]byte{bucketTopics, bucketSchemas, bucketAssignments, bucketMembers, bucketUsers, bucketRemovedMembers}

// fsmMeta is what a database says about itself.
type fsmMeta struct {
	// applied is the index of the last entry the database holds, as
	// stored; meaningful only when trusted.
	applied uint64
	// maxEntryType is the newest entry type the database has applied.
	maxEntryType uint64
	// trusted: applied > 0 and the stored txid is the file's current
	// one, so nothing but this FSM has written the file since.
	trusted bool
	// hasData: any metadata bucket holds a key, or an index is stored.
	hasData bool
}

// trustedApplied is the index Apply may skip up to: the stored one when
// it describes the file, else 0.
func (m fsmMeta) trustedApplied() uint64 {
	if m.trusted {
		return m.applied
	}
	return 0
}

func getMetaUint(b *bolt.Bucket, key []byte) uint64 {
	if b == nil {
		return 0
	}
	v := b.Get(key)
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func putMetaUint(b *bolt.Bucket, key []byte, v uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], v)
	return b.Put(key, raw[:])
}

// readFSMMeta reads the database's own record of itself in tx, a read
// transaction (whose ID is the file's current transaction id).
func readFSMMeta(tx *bolt.Tx) fsmMeta {
	b := tx.Bucket(bucketFSMMeta)
	m := fsmMeta{
		applied:      getMetaUint(b, metaKeyApplied),
		maxEntryType: getMetaUint(b, metaKeyMaxEntryType),
	}
	m.trusted = m.applied > 0 && getMetaUint(b, metaKeyTxid) == uint64(tx.ID())
	m.hasData = m.applied > 0
	for _, name := range dataBuckets {
		if bk := tx.Bucket(name); bk != nil {
			if k, _ := bk.Cursor().First(); k != nil {
				m.hasData = true
				break
			}
		}
	}
	return m
}

// putFSMMeta records, in the write transaction that carries entry
// index's effects, that the database now holds it. tx.ID() is the
// transaction id this commit gives the file.
func putFSMMeta(tx *bolt.Tx, index uint64, entryType uint32) error {
	b, err := tx.CreateBucketIfNotExists(bucketFSMMeta)
	if err != nil {
		return err
	}
	if err := putMetaUint(b, metaKeyApplied, index); err != nil {
		return err
	}
	if err := putMetaUint(b, metaKeyTxid, uint64(tx.ID())); err != nil {
		return err
	}
	if stored := getMetaUint(b, metaKeyMaxEntryType); uint64(entryType) > stored {
		return putMetaUint(b, metaKeyMaxEntryType, uint64(entryType))
	}
	return nil
}

// openBolt opens (or creates) the database at path and reads its own
// record of itself before writing anything. A database that has applied
// an entry type newer than this build knows is refused untouched: this
// binary would read it with older semantics. Missing buckets are created
// in one transaction, which also moves the stored txid along with the
// file's when the index was trusted, so creating a bucket never
// untrusts it. build names this binary in the refusal.
func openBolt(path, build string) (*bolt.DB, fsmMeta, error) {
	db, err := bolt.Open(path, 0o600, boltOptions())
	if err != nil {
		return nil, fsmMeta{}, fmt.Errorf("open %s: %w", path, err)
	}
	var meta fsmMeta
	missing := false
	err = db.View(func(tx *bolt.Tx) error {
		meta = readFSMMeta(tx)
		for _, name := range append([][]byte{bucketFSMMeta}, dataBuckets...) {
			if tx.Bucket(name) == nil {
				missing = true
			}
		}
		return nil
	})
	if err == nil && meta.maxEntryType > uint64(MaxEntryType) {
		_ = db.Close()
		return nil, fsmMeta{}, newerDatabaseError(path, meta.maxEntryType, build)
	}
	if err == nil && missing {
		err = db.Update(func(tx *bolt.Tx) error {
			for _, name := range append([][]byte{bucketFSMMeta}, dataBuckets...) {
				if _, err := tx.CreateBucketIfNotExists(name); err != nil {
					return err
				}
			}
			if meta.trusted {
				return putMetaUint(tx.Bucket(bucketFSMMeta), metaKeyTxid, uint64(tx.ID()))
			}
			return nil
		})
	}
	if err != nil {
		_ = db.Close()
		return nil, fsmMeta{}, fmt.Errorf("open %s: %w", path, err)
	}
	return db, meta, nil
}

// readImageMeta reads the record of a database file that is not in use,
// such as a snapshot image Restore has written beside the live database
// and not yet installed. It also proves the image is a database.
func readImageMeta(path string) (fsmMeta, error) {
	opts := boltOptions()
	opts.ReadOnly = true
	db, err := bolt.Open(path, 0o600, opts)
	if err != nil {
		return fsmMeta{}, err
	}
	defer db.Close()
	var meta fsmMeta
	err = db.View(func(tx *bolt.Tx) error {
		meta = readFSMMeta(tx)
		return nil
	})
	return meta, err
}

// noRaftStateError refuses to bootstrap a new cluster beside a database
// that holds metadata: the Raft state that went with it (raft.db, the
// snapshots) is gone, and a cluster started on an empty database would
// hold none of this node's topics. Setting the file aside on its own
// would turn a lost raft.db into the loss of every partition copy, so
// the operator decides.
func noRaftStateError(path string) error {
	dir := filepath.Dir(path)
	return fmt.Errorf("metastore: %s holds metadata, but there is no raft state beside it (raft.db is missing or empty and there is no raft snapshot), so this node would bootstrap a new cluster with an empty log and none of its topics; refusing to start. "+
		"To keep its topics, restore raft.db and the snapshots directory in %s from a backup. "+
		"To start this node empty instead, move fsm.db out of %s, and move the topics directory beside %s aside too if its partition data must be kept: a node started empty removes every topic directory that no topic names",
		path, dir, dir, dir)
}

// newerDatabaseError reports a database or snapshot that has applied an
// entry type this release does not know.
func newerDatabaseError(what string, entryType uint64, build string) error {
	return fmt.Errorf("%s holds raft entry type %d, written by a newer Narad release than this build (%s, which knows entry types up to %d); run that release or newer",
		what, entryType, buildName(build), MaxEntryType)
}

func buildName(build string) string {
	if build == "" {
		return "unknown build"
	}
	return build
}

// recordConsumed persists index for an entry consumed without a
// committed transaction of its own: a refused command, or an entry that
// does not decode. A failure is only logged: re-evaluating a
// deterministic refusal after a restart gives the same answer against
// the same state, and a disk that cannot commit stops the FSM on the
// next entry's write.
func (f *fsmState) recordConsumed(index uint64, entryType uint32) {
	if err := f.db.Update(func(tx *bolt.Tx) error { return putFSMMeta(tx, index, entryType) }); err != nil {
		f.log.Warn("metastore: could not record a consumed raft entry as applied; a restart evaluates it again", "index", index, "error", err)
	}
}

// setAsideDatabase moves the database file to fsm.db.stale (one slot,
// overwritten) and opens an empty one in its place. The FSM must not be
// in use: this runs before Raft starts.
func (f *fsmState) setAsideDatabase(reason string) error {
	stale := f.dbPath + ".stale"
	if err := f.db.Close(); err != nil {
		return fmt.Errorf("close %s: %w", f.dbPath, err)
	}
	if err := os.Rename(f.dbPath, stale); err != nil {
		// Keep the database open and in use rather than none at all.
		if db, meta, reopenErr := openBolt(f.dbPath, f.build); reopenErr == nil {
			f.db, f.meta = db, meta
		}
		return fmt.Errorf("set aside %s: %w", f.dbPath, err)
	}
	syncDir(f.dbPath)
	db, meta, err := openBolt(f.dbPath, f.build)
	if err != nil {
		return err
	}
	f.db, f.meta = db, meta
	f.applied.Store(0)
	f.lastSeen.Store(0)
	f.log.Warn("metastore: set aside fsm.db as fsm.db.stale and rebuilding it from the raft log; delete fsm.db.stale once the node is ready",
		"reason", reason, "stale", stale)
	return nil
}

// prepareFSMForStart settles, before Raft starts, how the FSM's
// database meets the Raft state beside it, and reports whether Raft
// should restore its latest snapshot into the FSM at start:
//
//   - No Raft state beside a database holding anything: the database
//     describes a log that is gone. A node that joins an existing
//     cluster sets it aside, and the leader's log or snapshot rebuilds
//     it. A node that would bootstrap refuses to start instead: a new
//     cluster on an empty database would hold none of its topics, and
//     the startup sweep would then remove their partition directories.
//     The operator restores the Raft state, or moves fsm.db away to
//     start empty (noRaftStateError says how).
//   - A trusted index at or past the latest snapshot: the database
//     already holds everything the snapshot does. Raft is told not to
//     restore it (it still takes the snapshot's index and configuration)
//     and Apply skips the entries the database holds, so a restart
//     re-applies nothing.
//   - A trusted index behind the latest snapshot, or an untrusted one
//     with a snapshot to restore: Raft restores it, as it always did.
//   - An untrusted index and no snapshot, with the log still starting
//     at index 1 (the first start after upgrading from 3.0.x on a young
//     or quiet cluster, or one after a rollback): replaying onto the
//     populated file would apply every entry twice, so the file is set
//     aside and rebuilt from the log.
//   - An untrusted index, no snapshot and a compacted log: nothing to
//     rebuild from. The database is kept and replayed onto, and that is
//     logged at error.
//
// A trusted index past everything Raft holds (its log and its latest
// snapshot) belongs to another log and is treated as untrusted.
func prepareFSMForStart(log *slog.Logger, f *fsmState, hasState, joinOnly bool, logs raft.LogStore, snaps raft.SnapshotStore) (restore bool, err error) {
	var snapIndex uint64
	list, err := snaps.List()
	if err != nil {
		return false, fmt.Errorf("metastore: list snapshots: %w", err)
	}
	if len(list) > 0 {
		snapIndex = list[0].Index
	}
	first, err := logs.FirstIndex()
	if err != nil {
		return false, fmt.Errorf("metastore: raft log first index: %w", err)
	}
	last, err := logs.LastIndex()
	if err != nil {
		return false, fmt.Errorf("metastore: raft log last index: %w", err)
	}
	applied := f.meta.trustedApplied()

	if !hasState {
		switch {
		case !f.meta.hasData:
			return true, nil
		case joinOnly:
			return true, f.setAsideDatabase("there is no raft state beside it, and the cluster this node joins replaces it with its own log or snapshot")
		default:
			return false, noRaftStateError(f.dbPath)
		}
	}
	if applied > max(last, snapIndex) {
		log.Warn("metastore: fsm.db records an applied index past the raft log and its latest snapshot; it belongs to another log and is not used",
			"applied_index", applied, "last_log_index", last, "snapshot_index", snapIndex)
		applied = 0
		f.applied.Store(0)
	}
	switch {
	case applied > 0 && applied >= snapIndex:
		f.lastSeen.Store(snapIndex)
		log.Info("metastore: fsm.db already holds the latest raft snapshot; skipping its restore and the entries fsm.db holds",
			"applied_index", applied, "snapshot_index", snapIndex)
		return false, nil
	case applied > 0:
		log.Info("metastore: restoring the latest raft snapshot over an older fsm.db", "applied_index", applied, "snapshot_index", snapIndex)
		return true, nil
	case snapIndex > 0:
		log.Info("metastore: fsm.db has no applied index this release can trust; restoring the latest raft snapshot over it", "snapshot_index", snapIndex)
		return true, nil
	case first <= 1 || !f.meta.hasData:
		if f.meta.hasData {
			return true, f.setAsideDatabase("fsm.db has no applied index this release can trust (an older release or another program wrote it last), and replaying the raft log onto it would apply entries twice")
		}
		return true, nil
	default:
		log.Error("metastore: fsm.db has no applied index this release can trust, there is no raft snapshot to restore and the raft log no longer starts at index 1; replaying the log onto fsm.db as it is",
			"first_log_index", first, "last_log_index", last)
		return true, nil
	}
}
