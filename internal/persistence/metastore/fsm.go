package metastore

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

// boltOpenTimeout bounds how long a bbolt open waits for the file lock.
// bbolt takes an exclusive flock and, with no timeout, waits for it
// forever: a stale process on the same data directory, a double start
// on bare metal, or an operator's inspection tool holding fsm.db made
// `narad serve` hang inside metastore.New with no log line and no
// error, so /healthz never came up and Kubernetes restarted the pod in
// a loop with nothing to show for it. With the timeout the open fails
// with bolt.ErrTimeout and the error names the file. A var so tests can
// shorten it.
var boltOpenTimeout = 5 * time.Second

// boltOptions is the option set every bbolt open in this package uses.
func boltOptions() *bolt.Options {
	return &bolt.Options{Timeout: boltOpenTimeout}
}

var (
	bucketTopics      = []byte("topics")
	bucketSchemas     = []byte("schemas")
	bucketAssignments = []byte("assignments")
	bucketMembers     = []byte("members")
	bucketUsers       = []byte("users")
	// bucketRemovedMembers holds tombstones for members removed by
	// decommission, keyed by member ID; see applyRemoveMember.
	bucketRemovedMembers = []byte("removed_members")
)

func schemaKey(topicName string, version int) []byte {
	return fmt.Appendf(nil, "%s:%d", topicName, version)
}

func assignmentKey(topicName string, partition int) []byte {
	return fmt.Appendf(nil, "%s:%d", topicName, partition)
}

// fsmState is the Raft FSM backed by bbolt.
//
// mu protects the db pointer only during Restore (which swaps it).
// Apply is serialised with Restore by Raft's runFSM goroutine so it
// does not need to hold mu. Read methods hold RLock to prevent using a
// closed db while Restore is swapping the pointer.
type fsmState struct {
	mu       sync.RWMutex
	db       *bolt.DB
	dbPath   string
	version  atomic.Uint64
	versions metadataDomainVersions
}

func newFSM(path string) (*fsmState, error) {
	db, err := openBolt(path)
	if err != nil {
		return nil, err
	}
	return &fsmState{db: db, dbPath: path, versions: newMetadataDomainVersions()}, nil
}

func openBolt(path string) (*bolt.DB, error) {
	db, err := bolt.Open(path, 0o600, boltOptions())
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	return db, db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketTopics, bucketSchemas, bucketAssignments, bucketMembers, bucketUsers, bucketRemovedMembers} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
}

func (f *fsmState) view(fn func(*bolt.Tx) error) error {
	return f.db.View(fn)
}

func (f *fsmState) update(fn func(*bolt.Tx) error) error {
	return f.db.Update(fn)
}

// Apply is called by Raft when a log entry is committed. A business
// error (e.g. ErrAlreadyExists) is returned as the FSM response so the
// caller sees it, and the metadata version only advances on success.
func (f *fsmState) Apply(l *raft.Log) any {
	var c cmd
	if err := json.Unmarshal(l.Data, &c); err != nil {
		return err
	}

	var err error
	switch c.Op {
	case opCreateTopic:
		err = f.applyCreateTopic(c.Data)
	case opUpdateTopic:
		err = f.applyUpdateTopic(c.Data)
	case opDeleteTopic:
		err = f.applyDeleteTopic(c.Data)
	case opPutSchema:
		err = f.applyPutSchema(c.Data)
	case opAssignPartition:
		err = f.applyAssignPartition(c.Data)
	case opMemberJoin:
		err = f.applyMemberJoin(c.Data)
	case opMemberHeartbeat:
		err = f.applyMemberHeartbeat(c.Data)
	case opMemberDead:
		err = f.applyMemberDead(c.Data)
	case opCreateUser:
		err = f.applyCreateUser(c.Data)
	case opUpdateUser:
		err = f.applyUpdateUser(c.Data)
	case opDeleteUser:
		err = f.applyDeleteUser(c.Data)
	case opSeedRootUser:
		err = f.applySeedRootUser(c.Data)
	case opAttachChild:
		err = f.applyAttachChild(c.Data)
	case opDetachChild:
		err = f.applyDetachChild(c.Data)
	case opSetAssignmentTarget:
		err = f.applySetAssignmentTarget(c.Data)
	case opCompleteMove:
		err = f.applyCompleteMove(c.Data)
	case opAbortMove:
		err = f.applyAbortMove(c.Data)
	case opSetMemberDraining:
		err = f.applySetMemberDraining(c.Data)
	case opRemoveMember:
		err = f.applyRemoveMember(c.Data)
	case opReadmitMember:
		err = f.applyReadmitMember(c.Data)
	case opSetUserPassword:
		err = f.applySetUserPassword(c.Data)
	case opSetUserGrants:
		err = f.applySetUserGrants(c.Data)
	default:
		return fmt.Errorf("metastore: unknown op %d", c.Op)
	}
	if err == nil {
		f.version.Add(1)
	}
	return err
}

func (f *fsmState) metadataVersion() uint64 {
	return f.version.Load()
}
